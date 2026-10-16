import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass, field

from pyrogram import Client, StopTransmission, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    BadRequest, ChannelInvalid, ChannelPrivate, FloodWait, InviteHashExpired,
    InviteHashInvalid, PeerIdInvalid, RPCError, UserAlreadyParticipant,
    UsernameInvalid, UsernameNotOccupied,
)
from pyrogram.types import (
    BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup,
)

try:  # newer Pyrogram forks (Kurigram etc.) deprecate reply_to_message_id / disable_web_page_preview
    from pyrogram.types import LinkPreviewOptions, ReplyParameters
except ImportError:
    LinkPreviewOptions = ReplyParameters = None

from db import DB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("srb")

# ---------- config (env overrides config.json) ----------
try:
    with open("config.json") as f:
        DATA = json.load(f)
except FileNotFoundError:
    DATA = {}


def getenv(var, default=None):
    return os.environ.get(var) or DATA.get(var) or default


API_ID = int(getenv("ID"))
API_HASH = getenv("HASH")
BOT_TOKEN = getenv("TOKEN")
STRING = getenv("STRING")
OWNER = int(getenv("OWNER", 0))              # admin user id
WORKERS = int(getenv("WORKERS", 2))          # concurrent jobs
MAX_PER_USER = int(getenv("MAX_PER_USER", 3))  # queued+running jobs per user
MAX_RANGE = int(getenv("MAX_RANGE", 100))    # max messages per "from-to" link
DELAY = float(getenv("DELAY", 3))            # pause between account fetches
DB_PATH = getenv("DB_PATH", "bot.db")
BIN_CHANNEL = int(getenv("BIN_CHANNEL", 0))  # channel id (-100...) that receives a copy of everything sent

db = DB(DB_PATH)
if not OWNER:
    raise SystemExit("OWNER is required (your Telegram user id)")
db.add_user(OWNER)

bot = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, sleep_threshold=120)
acc = (
    Client("myacc", api_id=API_ID, api_hash=API_HASH, session_string=STRING,
           in_memory=True, sleep_threshold=120)
    if STRING else None
)

INVITE_RE = re.compile(r"https?://t\.me/(?:\+|joinchat/)[\w-]+")
LINK_RE = re.compile(
    r"https?://t\.me/(?:(?P<kind>[cb])/)?(?P<ref>\w+)(?P<nums>(?:/\d+){1,2})"
    r"(?:\?\S*)?(?:\s*-\s*(?P<end>\d+))?"
)
MEDIA_KINDS = ("document", "video", "animation", "sticker", "voice", "audio", "photo", "video_note")
THUMB_KINDS = ("document", "video", "animation", "audio", "video_note")

USAGE = """**FOR PUBLIC CHATS**

__just send post/s link__

**FOR PRIVATE CHATS**

__first send invite link of the chat (unnecessary if the account of string session already member of the chat)
then send post/s link__

**FOR BOT CHATS**

__send link with '/b/', bot's username and message id__

```
https://t.me/b/botusername/4321
```

**MULTI POSTS**

__use "from - to" (spaces don't matter)__

```
https://t.me/xxxx/1001-1010

https://t.me/c/xxxx/101 - 120
```

**COMMANDS**: /settings — options, /cancel — stop your running/queued jobs"""


class UserError(Exception):
    pass


@dataclass
class Job:
    user_id: int
    chat_id: int
    reply_id: int
    specs: list
    cancelled: bool = False
    bin: bool = False
    bin_err: bool = False
    status: object = field(default=None, repr=False)


queue: asyncio.Queue = asyncio.Queue()
jobs_by_user: dict = {}
pending: dict = {}  # token -> (uid, chat_id, reply_id, specs, created) waiting for the BIN yes/no answer
busy = 0  # workers currently running a job


def allowed(uid):
    return uid == OWNER or db.is_user(uid)


def rp(mid):
    """Reply kwargs that work on classic Pyrogram 2.0.x and on forks that deprecate reply_to_message_id."""
    if ReplyParameters:
        return {"reply_parameters": ReplyParameters(message_id=mid)}
    return {"reply_to_message_id": mid}


def no_preview():
    if LinkPreviewOptions:
        return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
    return {"disable_web_page_preview": True}


def clean_markup(msg):
    """Rebuild the inline keyboard: URL buttons stay links, every other button becomes a text-only "noop" button."""
    mk = getattr(msg, "reply_markup", None)
    if not isinstance(mk, InlineKeyboardMarkup):
        return None
    rows = [[InlineKeyboardButton(b.text, url=b.url) if b.url else InlineKeyboardButton(b.text, callback_data="noop")
             for b in row] for row in mk.inline_keyboard]
    rows = [r for r in rows if r]
    return InlineKeyboardMarkup(rows) if rows else None


awaiting_bin: set = set()  # owner ids currently expected to send a BIN channel
last_owner_alert = None


def bin_channel():
    """Effective BIN channel: the one the admin set in the bot, else BIN_CHANNEL from config."""
    v = db.get_setting(0, "bin_channel")
    return int(v) if v else BIN_CHANNEL


def bin_reason(e):
    """Human-readable reason for a Telegram error when talking to the BIN channel."""
    code = str(getattr(e, "ID", "") or type(e).__name__).upper()
    if any(x in code for x in ("ADMIN_REQUIRED", "WRITE_FORBIDDEN", "SEND_", "RIGHTS", "FORBIDDEN", "NOT_ALLOWED")):
        return "the bot is not an admin with the “Post Messages” permission in the BIN channel"
    if any(x in code for x in ("CHANNEL_PRIVATE", "NOT_PARTICIPANT", "CHANNEL_INVALID", "PEER_ID_INVALID",
                               "CHAT_ID_INVALID", "CHAT_INVALID")):
        return "the bot is not a member of that chat (or the ID is wrong)"
    if "USERNAME" in code:
        return "that username does not exist"
    return f"unexpected error: {e}"


async def check_bin(cid):
    """Return None if the bot can post in the chat, else the reason it can't."""
    try:
        me = await bot.get_chat_member(cid, "me")
    except RPCError as e:
        return bin_reason(e)
    status = str(getattr(me, "status", "")).upper()
    if "OWNER" in status:
        return None
    if "ADMIN" not in status:
        return "the bot is not an admin in the BIN channel (add it as admin with “Post Messages”)"
    priv = getattr(me, "privileges", None)
    if priv is not None and getattr(priv, "can_post_messages", True) is False:
        return "the bot is an admin but lacks the “Post Messages” permission"
    return None


async def alert_owner(text):
    """Message the owner, at most once per 10 minutes."""
    global last_owner_alert
    if last_owner_alert is not None and time.monotonic() - last_owner_alert < 600:
        return
    last_owner_alert = time.monotonic()
    try:
        await bot.send_message(OWNER, text)
    except Exception:
        pass


async def to_bin(job, sent, mk=None):
    """Copy a delivered message to the bin channel. Never raises."""
    cid = bin_channel()
    if not cid or not job.bin or sent is None:
        return
    try:
        await sent.copy(cid, reply_markup=mk)  # sent msgs lack the markup, pass it explicitly
    except Exception as e:
        reason = bin_reason(e)
        log.warning("bin channel copy failed: %s", e)
        if not job.bin_err:  # tell the user once per job, and the owner (throttled)
            job.bin_err = True
            try:
                await bot.send_message(job.chat_id, f"⚠️ **BIN copy failed**: __{reason}__", **rp(job.reply_id))
            except Exception:
                pass
            await alert_owner(f"⚠️ **BIN channel problem**: __{reason}__\n\nFix it, or set another channel in /settings.")


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def make_progress(client, job, label):
    """Async progress callback, edits the status message at most every 4s."""
    t0 = last = time.monotonic()

    async def cb(current, total):
        nonlocal last
        if job.cancelled:
            client.stop_transmission()
        now = time.monotonic()
        if now - last < 4 and current < total:
            return
        last = now
        speed = current / max(now - t0, 0.1)
        eta = int((total - current) / speed) if speed else 0
        try:
            await job.status.edit_text(
                f"__{label}__ **{current * 100 / total:.1f}%** • {human(speed)}/s • ETA {eta}s")
        except RPCError:
            pass

    return cb


# ---------- commands ----------
@bot.on_message(filters.private & filters.command("start"))
async def start(_, m):
    await m.reply(
        f"__👋 Hi **{m.from_user.mention}**, I am Save Restricted Bot, I can send you restricted "
        f"content by it's post link__\n\n{USAGE}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🌐 Source Code", url="https://github.com/aghil246/Save-Restricted-Bot")]]),
        quote=True,
    )


# ---------- settings ----------
BIN_LABELS = {"off": "Off", "on": "Always on", "ask": "Ask each time"}
SET_RE = r"^set:(bin:(?:off|on|ask)|close|binset|binrm|bincancel)$"


def settings_view(uid):
    cid = bin_channel()
    is_owner = uid == OWNER
    rows = []
    if cid:
        cur = db.get_setting(uid, "bin", "off")
        title = db.get_setting(0, "bin_title") if db.get_setting(0, "bin_channel") else None
        title = "".join(ch for ch in (title or "") if ch not in "*_`[]")
        label = f"{title} (`{cid}`)" if title else f"`{cid}`"
        text = (f"**⚙️ Settings**\n\n**BIN channel**: {label}\n**Copy to BIN channel**: {BIN_LABELS[cur]}\n"
                "__Whether results are also copied to the BIN channel. \"Ask each time\" shows a Yes/No prompt per job.__")
        rows += [[InlineKeyboardButton(("✅ " if k == cur else "") + v, callback_data=f"set:bin:{k}")]
                 for k, v in BIN_LABELS.items()]
    else:
        text = "**⚙️ Settings**\n\n**BIN channel**: not set" + ("" if is_owner else "\n__Ask the admin to set one.__")
    if is_owner:
        rows.append([InlineKeyboardButton("📡 Change BIN channel" if cid else "📡 Set BIN channel", callback_data="set:binset")])
        if db.get_setting(0, "bin_channel") is not None:
            rows.append([InlineKeyboardButton("🗑 Remove BIN channel", callback_data="set:binrm")])
    rows.append([InlineKeyboardButton("✖️ Close", callback_data="set:close")])
    return text, InlineKeyboardMarkup(rows)


@bot.on_message(filters.private & filters.command("settings"))
async def settings(_, m):
    if not allowed(m.from_user.id):
        return await m.reply(f"**Not authorized.** Your ID: `{m.from_user.id}`", quote=True)
    awaiting_bin.discard(m.from_user.id)
    text, mk = settings_view(m.from_user.id)
    await m.reply(text, reply_markup=mk, quote=True)


@bot.on_callback_query(filters.regex(SET_RE))
async def on_settings(_, q):
    uid = q.from_user.id
    if not allowed(uid):
        return await q.answer("Not authorized", show_alert=True)
    data = q.matches[0].group(1)
    if data == "close":
        awaiting_bin.discard(uid)
        await q.answer()
        return await q.message.delete()
    if data in ("binset", "binrm") and uid != OWNER:
        return await q.answer("Admin only", show_alert=True)

    if data == "binset":
        awaiting_bin.add(uid)
        await q.message.edit_text(
            "**📡 Set BIN channel**\n\n1. Add me to the channel as **admin** with the **Post Messages** permission.\n"
            "2. Send the channel ID (`-100…`) or @username here, or forward any message from the channel.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="set:bincancel")]]))
        return await q.answer()
    if data == "bincancel":
        awaiting_bin.discard(uid)
    elif data == "binrm":
        db.del_setting(0, "bin_channel")
        db.del_setting(0, "bin_title")
    else:
        db.set_setting(uid, "bin", data.split(":")[1])

    text, mk = settings_view(uid)
    try:
        await q.message.edit_text(text, reply_markup=mk)
    except RPCError:
        pass  # message not modified
    await q.answer("Saved" if data not in ("bincancel",) else None)


async def handle_bin_input(m):
    chat = getattr(m, "forward_from_chat", None) or getattr(getattr(m, "forward_origin", None), "chat", None)
    target = chat.id if chat else None
    if target is None:
        t = (m.text or "").strip()
        if re.fullmatch(r"-?\d+", t):
            target = int(t) if t.startswith("-100") else int("-100" + t.lstrip("-"))
        else:
            u = re.sub(r"^(?:https?://)?t\.me/", "", t).lstrip("@")
            if re.fullmatch(r"\w{4,}", u):
                target = u
    if target is None:
        return await m.reply("Send the channel ID (`-100…`), its @username, or forward a message from it.", quote=True)

    try:
        c = await bot.get_chat(target)
    except RPCError as e:
        return await m.reply(f"❌ **Can't access that chat**: __{bin_reason(e)}__\n\nAdd me as admin, then send it again.", quote=True)
    if not any(x in str(c.type).upper() for x in ("CHANNEL", "GROUP")):
        return await m.reply("❌ That is not a channel or group. Send a channel.", quote=True)

    reason = await check_bin(c.id)
    if reason:
        return await m.reply(f"❌ **Can't use `{c.title}`**: __{reason}__\n\nFix it, then send the channel again.", quote=True)
    try:  # the real test: post and remove a message
        t = await bot.send_message(c.id, "✅ BIN channel connected")
        try:
            await t.delete()
        except RPCError:
            pass
    except Exception as e:
        return await m.reply(f"❌ **Can't post in `{c.title}`**: __{bin_reason(e)}__\n\nFix it, then send the channel again.", quote=True)

    db.set_setting(0, "bin_channel", str(c.id))
    db.set_setting(0, "bin_title", c.title or "")
    awaiting_bin.discard(m.from_user.id)
    text, mk = settings_view(m.from_user.id)
    await m.reply("✅ **BIN channel saved.**\n\n" + text, reply_markup=mk, quote=True)


@bot.on_callback_query(filters.regex("^noop$"))
async def noop(_, q):
    await q.answer("This button is not active in the copy.")


@bot.on_message(filters.private & filters.command("cancel"))
async def cancel(_, m):
    jobs = jobs_by_user.get(m.from_user.id, [])
    for j in jobs:
        j.cancelled = True
    await m.reply(f"**Cancelling {len(jobs)} job(s)**" if jobs else "**Nothing to cancel**", quote=True)


def owner_only(func):
    async def wrapper(c, m):
        if m.from_user.id != OWNER:
            return
        await func(c, m)
    return wrapper


def arg_id(m):
    try:
        return int(m.command[1])
    except (IndexError, ValueError):
        return None


@bot.on_message(filters.private & filters.command("adduser"))
@owner_only
async def adduser(_, m):
    uid = arg_id(m)
    if uid is None:
        return await m.reply("Usage: `/adduser <user_id>`")
    db.add_user(uid)
    await m.reply(f"Added `{uid}`")


@bot.on_message(filters.private & filters.command("deluser"))
@owner_only
async def deluser(_, m):
    uid = arg_id(m)
    if uid is None:
        return await m.reply("Usage: `/deluser <user_id>`")
    db.del_user(uid)
    await m.reply(f"Removed `{uid}`")


@bot.on_message(filters.private & filters.command("users"))
@owner_only
async def users(_, m):
    await m.reply("\n".join(f"`{u}`" for u in db.users()) or "No users")


@bot.on_message(filters.private & filters.command("stats"))
@owner_only
async def stats(_, m):
    by_status, cached, n_users = db.stats()
    lines = [f"{k}: {v}" for k, v in by_status.items()] or ["no jobs yet"]
    await m.reply(f"**Users**: {n_users}\n**Cached files**: {cached}\n" + "\n".join(lines))


# ---------- main text handler ----------
CMDS = ["start", "settings", "cancel", "adduser", "deluser", "users", "stats"]


@bot.on_message(filters.private & (filters.text | filters.forwarded) & ~filters.command(CMDS), group=-1)
async def bin_input(_, m):
    if not m.from_user or m.from_user.id != OWNER or OWNER not in awaiting_bin:
        return  # not waiting for a channel: fall through to the normal handler
    await handle_bin_input(m)
    m.stop_propagation()


@bot.on_message(filters.private & filters.text & ~filters.command(CMDS))
async def on_text(_, m):
    uid = m.from_user.id
    if not allowed(uid):
        return await m.reply(f"**Not authorized.** Your ID: `{uid}`", quote=True)

    invite = INVITE_RE.search(m.text)
    if invite:
        return await join(m, invite.group(0))

    specs = []
    for mt in LINK_RE.finditer(m.text):
        nums = [int(n) for n in mt.group("nums").strip("/").split("/")]
        start_id = nums[-1]                       # with a topic id, the last number is the message
        end_id = int(mt.group("end") or start_id)
        if end_id < start_id:
            return await m.reply("**Invalid range**", quote=True)
        if end_id - start_id + 1 > MAX_RANGE:
            return await m.reply(f"**Max {MAX_RANGE} posts per link**", quote=True)
        specs.append((mt.group("kind"), mt.group("ref"), start_id, end_id))
    if not specs:
        return await m.reply("**Send a t.me post link** (see /start)", quote=True)

    if len(jobs_by_user.get(uid, [])) >= MAX_PER_USER:
        return await m.reply(f"**Too many pending jobs** (max {MAX_PER_USER}). Use /cancel", quote=True)

    mode = db.get_setting(uid, "bin", "off") if bin_channel() else "off"
    if mode == "ask":  # ask before queueing
        now = time.monotonic()
        for k in [k for k, v in pending.items() if now - v[4] > 600]:
            del pending[k]
        token = f"{m.chat.id}_{m.id}"
        pending[token] = (uid, m.chat.id, m.id, specs, now)
        return await m.reply(
            "**Also send a copy to the BIN channel?**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes", callback_data=f"bin:1:{token}"),
                InlineKeyboardButton("❌ No", callback_data=f"bin:0:{token}"),
            ]]),
            quote=True,
        )

    note = await enqueue(uid, m.chat.id, m.id, specs, mode == "on")
    if note:
        await m.reply(note, quote=True)


async def enqueue(uid, chat_id, reply_id, specs, use_bin):
    """Queue a job. Returns a message to show the user (error or queue position), or None."""
    if len(jobs_by_user.get(uid, [])) >= MAX_PER_USER:
        return f"**Too many pending jobs** (max {MAX_PER_USER}). Use /cancel"
    ahead = queue.qsize() + busy
    job = Job(uid, chat_id, reply_id, specs, bin=use_bin)
    jobs_by_user.setdefault(uid, []).append(job)
    await queue.put(job)
    if ahead >= WORKERS:
        return f"__Queued (position {ahead - WORKERS + 1})__"
    return None


@bot.on_callback_query(filters.regex(r"^bin:([01]):(.+)$"))
async def on_bin_choice(_, q):
    use_bin, token = q.matches[0].group(1) == "1", q.matches[0].group(2)
    p = pending.get(token)
    if not p:
        return await q.answer("Expired, send the link again.", show_alert=True)
    uid, chat_id, reply_id, specs, _ = p
    if q.from_user.id != uid:
        return await q.answer("Not your request.", show_alert=True)
    del pending[token]
    note = await enqueue(uid, chat_id, reply_id, specs, use_bin)
    await q.message.edit_text(note or ("__Queued + BIN copy__" if use_bin else "__Queued__"))
    await q.answer()


async def join(m, link):
    if acc is None:
        return await m.reply("**String Session is not Set**", quote=True)
    try:
        await acc.join_chat(link)
        await m.reply("**Chat Joined**", quote=True)
    except UserAlreadyParticipant:
        await m.reply("**Chat already Joined**", quote=True)
    except (InviteHashExpired, InviteHashInvalid):
        await m.reply("**Invalid Link**", quote=True)
    except RPCError as e:
        await m.reply(f"**Error** : __{e}__", quote=True)


# ---------- worker ----------
async def worker():
    while True:
        job = await queue.get()
        global busy
        busy += 1
        try:
            if not job.cancelled:
                await run_job(job)
        except Exception:
            log.exception("job crashed")
        finally:
            busy -= 1
            jobs_by_user[job.user_id].remove(job)
            queue.task_done()


async def run_job(job):
    job.status = await bot.send_message(job.chat_id, "__Working__", **rp(job.reply_id))

    async def say(text):
        await bot.send_message(job.chat_id, text, **rp(job.reply_id))

    try:
        for kind, ref, first, last in job.specs:
            for mid in range(first, last + 1):
                if job.cancelled:
                    return await say("**Cancelled**")
                chat_ref = f"{kind or ''}:{ref}".lower()
                try:
                    how = await process_one(job, kind, ref, mid)
                    db.log(job.user_id, chat_ref, mid, how)
                except StopTransmission:
                    db.log(job.user_id, chat_ref, mid, "cancelled")
                    return await say("**Cancelled**")
                except UserError as e:
                    db.log(job.user_id, chat_ref, mid, "error")
                    await say(f"**Error** : __{e}__")
                except FloodWait as e:
                    db.log(job.user_id, chat_ref, mid, "floodwait")
                    return await say(f"**Telegram flood wait: retry in {e.value}s**")
                except Exception as e:
                    log.exception("failed %s/%s", ref, mid)
                    db.log(job.user_id, chat_ref, mid, "error")
                    await say(f"**Error** : __{e}__")
                else:
                    if how == "downloaded":
                        await asyncio.sleep(DELAY)
    finally:
        try:
            await job.status.delete()
        except RPCError:
            pass


async def process_one(job, kind, ref, msg_id):
    chat = int(f"-100{ref}") if kind == "c" else ref
    chat_ref = f"{kind or ''}:{ref}".lower()
    rid = job.reply_id

    cached = db.cache_get(chat_ref, msg_id)
    if cached:
        ckind, file_id, cap = cached
        sent = await bot.send_cached_media(job.chat_id, file_id, caption=cap, parse_mode=ParseMode.HTML,
                                           **rp(rid))
        await to_bin(job, sent)
        return "cached"

    # public chats without protection: plain copy, no account needed
    if kind is None:
        try:
            orig = await bot.get_messages(chat, msg_id)
            mk = clean_markup(orig)
            sent = await orig.copy(job.chat_id, **rp(rid), reply_markup=mk)
            await to_bin(job, sent, mk)
            return "copied"
        except (UsernameNotOccupied, UsernameInvalid):
            raise UserError("The username is not occupied by anyone")
        except FloodWait:
            raise
        except (RPCError, ValueError):
            pass  # restricted or private -> use the account

    if acc is None:
        raise UserError("String session is not set")

    try:
        msg = await acc.get_messages(chat, msg_id)
    except (ChannelPrivate, ChannelInvalid, PeerIdInvalid):
        raise UserError("Account is not a member of this chat. Send the invite link first")
    except (UsernameNotOccupied, UsernameInvalid):
        raise UserError("The username is not occupied by anyone")
    if msg.empty:
        raise UserError("Message not found or deleted")

    if msg.text:
        sent = await bot.send_message(job.chat_id, msg.text.html, parse_mode=ParseMode.HTML,
                                      **no_preview(), **rp(rid),
                                      reply_markup=clean_markup(msg))
        await to_bin(job, sent, clean_markup(msg))
        return "downloaded"

    mkind = next((k for k in MEDIA_KINDS if getattr(msg, k, None)), None)
    if mkind is None:
        raise UserError("Unsupported message type")

    await transfer(job, msg, mkind, chat_ref)
    return "downloaded"


async def transfer(job, msg, mkind, chat_ref):
    tmp = tempfile.mkdtemp(prefix="srb_")
    try:
        path = await acc.download_media(msg, file_name=tmp + "/",
                                        progress=make_progress(acc, job, "Downloading"))
        if path is None:  # pyrogram swallows all download errors, incl. StopTransmission
            if job.cancelled:
                raise StopTransmission
            raise UserError("Download failed")

        media = getattr(msg, mkind)
        thumb = None
        thumbs = getattr(media, "thumbs", None) if mkind in THUMB_KINDS else None
        if thumbs:
            try:
                thumb = await acc.download_media(thumbs[0].file_id, file_name=tmp + "/")
            except RPCError:
                thumb = None

        cap_html = msg.caption.html if msg.caption else None
        extra = None
        if cap_html and len(msg.caption) > 1024:  # bot caption limit
            extra, cap_html = cap_html, None
        cap = dict(caption=cap_html, parse_mode=ParseMode.HTML)
        mk = clean_markup(msg)
        base = dict(**rp(job.reply_id), reply_markup=mk,
                    progress=make_progress(bot, job, "Uploading"))
        cid = job.chat_id

        if mkind == "document":
            sent = await bot.send_document(cid, path, thumb=thumb, **cap, **base)
        elif mkind == "video":
            sent = await bot.send_video(cid, path, duration=media.duration, width=media.width,
                                        height=media.height, thumb=thumb, supports_streaming=True, **cap, **base)
        elif mkind == "animation":
            sent = await bot.send_animation(cid, path, duration=media.duration, width=media.width,
                                            height=media.height, thumb=thumb, **cap, **base)
        elif mkind == "audio":
            sent = await bot.send_audio(cid, path, duration=media.duration, performer=media.performer,
                                        title=media.title, thumb=thumb, **cap, **base)
        elif mkind == "voice":
            sent = await bot.send_voice(cid, path, duration=media.duration, **cap, **base)
        elif mkind == "video_note":
            sent = await bot.send_video_note(cid, path, duration=media.duration, length=media.length,
                                             thumb=thumb, **base)
        elif mkind == "sticker":
            sent = await bot.send_sticker(cid, path, **base)
        else:
            sent = await bot.send_photo(cid, path, **cap, **base)

        if sent is None:  # upload was stopped
            raise StopTransmission

        await to_bin(job, sent, mk)
        if extra:
            sent_extra = await bot.send_message(cid, extra, parse_mode=ParseMode.HTML, **rp(sent.id))
            await to_bin(job, sent_extra)

        sent_media = getattr(sent, mkind, None)
        if sent_media and not extra and not mk:  # long captions / buttons aren't stored in the cache
            db.cache_put(chat_ref, msg.id, mkind, sent_media.file_id, cap_html)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await bot.start()
    try:  # the "/" command menu
        await bot.set_bot_commands([
            BotCommand("start", "How to use the bot"),
            BotCommand("settings", "Open settings"),
            BotCommand("cancel", "Stop your running/queued jobs"),
        ])
        await bot.set_bot_commands(
            [BotCommand("start", "How to use the bot"), BotCommand("settings", "Open settings"),
             BotCommand("cancel", "Stop your running/queued jobs"), BotCommand("adduser", "Authorize a user"),
             BotCommand("deluser", "Remove a user"), BotCommand("users", "List users"),
             BotCommand("stats", "Bot statistics")],
            scope=BotCommandScopeChat(OWNER))
    except Exception as e:
        log.warning("could not set bot commands: %s", e)
    cid = bin_channel()
    if cid:
        reason = await check_bin(cid)
        if reason:
            log.warning("BIN channel %s unusable: %s", cid, reason)
            await alert_owner(f"⚠️ **BIN channel problem**: __{reason}__\n\nSet it again in /settings.")
    if acc:
        await acc.start()
    tasks = [asyncio.create_task(worker()) for _ in range(WORKERS)]
    log.info("started: %d workers, account=%s", WORKERS, bool(acc))
    await idle()
    for t in tasks:
        t.cancel()
    if acc:
        await acc.stop()
    await bot.stop()


if __name__ == "__main__":
    bot.run(main())
