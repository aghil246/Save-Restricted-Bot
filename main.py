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
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

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

**COMMANDS**: /cancel — stop your running/queued jobs"""


class UserError(Exception):
    pass


@dataclass
class Job:
    user_id: int
    chat_id: int
    reply_id: int
    specs: list
    cancelled: bool = False
    status: object = field(default=None, repr=False)


queue: asyncio.Queue = asyncio.Queue()
jobs_by_user: dict = {}
busy = 0  # workers currently running a job


def allowed(uid):
    return uid == OWNER or db.is_user(uid)


def clean_markup(msg):
    """Rebuild the inline keyboard: URL buttons stay links, every other button becomes a text-only "noop" button."""
    mk = getattr(msg, "reply_markup", None)
    if not isinstance(mk, InlineKeyboardMarkup):
        return None
    rows = [[InlineKeyboardButton(b.text, url=b.url) if b.url else InlineKeyboardButton(b.text, callback_data="noop")
             for b in row] for row in mk.inline_keyboard]
    rows = [r for r in rows if r]
    return InlineKeyboardMarkup(rows) if rows else None


async def to_bin(sent, mk=None):
    """Copy a delivered message to the bin channel. Never raises."""
    if not BIN_CHANNEL or sent is None:
        return
    try:
        await sent.copy(BIN_CHANNEL, reply_markup=mk)  # sent msgs lack the markup, pass it explicitly
    except Exception as e:
        log.warning("bin channel copy failed: %s", e)


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
CMDS = ["start", "cancel", "adduser", "deluser", "users", "stats"]


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

    ahead = queue.qsize() + busy
    job = Job(uid, m.chat.id, m.id, specs)
    jobs_by_user.setdefault(uid, []).append(job)
    await queue.put(job)
    if ahead >= WORKERS:
        await m.reply(f"__Queued (position {ahead - WORKERS + 1})__", quote=True)


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
    job.status = await bot.send_message(job.chat_id, "__Working__", reply_to_message_id=job.reply_id)

    async def say(text):
        await bot.send_message(job.chat_id, text, reply_to_message_id=job.reply_id)

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
                                           reply_to_message_id=rid)
        await to_bin(sent)
        return "cached"

    # public chats without protection: plain copy, no account needed
    if kind is None:
        try:
            orig = await bot.get_messages(chat, msg_id)
            mk = clean_markup(orig)
            sent = await orig.copy(job.chat_id, reply_to_message_id=rid, reply_markup=mk)
            await to_bin(sent, mk)
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
                                      disable_web_page_preview=True, reply_to_message_id=rid,
                                      reply_markup=clean_markup(msg))
        await to_bin(sent, clean_markup(msg))
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
        base = dict(reply_to_message_id=job.reply_id, reply_markup=mk,
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

        await to_bin(sent, mk)
        if extra:
            sent_extra = await bot.send_message(cid, extra, parse_mode=ParseMode.HTML, reply_to_message_id=sent.id)
            await to_bin(sent_extra)

        sent_media = getattr(sent, mkind, None)
        if sent_media and not extra and not mk:  # long captions / buttons aren't stored in the cache
            db.cache_put(chat_ref, msg.id, mkind, sent_media.file_id, cap_html)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await bot.start()
    if BIN_CHANNEL:
        try:
            await bot.get_chat(BIN_CHANNEL)
        except Exception as e:
            log.warning("BIN_CHANNEL %s not reachable (add the bot as channel admin): %s", BIN_CHANNEL, e)
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
