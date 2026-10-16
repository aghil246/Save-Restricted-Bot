# Save Restricted Bot

*A Telegram Bot, Which can send you restricted content by it's post link*

---

## Variables

- `HASH` Your API Hash from my.telegram.org
- `ID` Your API ID from my.telegram.org
- `TOKEN` Your bot token from @BotFather
- `OWNER` Your Telegram user ID (admin; authorizes other users)
- `BIN_CHANNEL` _(optional)_ Channel ID (`-100…`); add the bot as admin there. Each user chooses in `/settings` whether results are copied: Off (default), Always on, or Ask each time
- `STRING` Your session string, you can get it at [gist](https://gist.github.com/bipinkrish/0940b30ed66a5537ae1b5aaaee716897#file-main-py) and run it locally

---

# Usage

__FOR PUBLIC CHATS__

_just send post/s link_


__FOR PRIVATE CHATS__

_first send invite link of the chat (unnecessary if the account of string session already member of the chat)
then send post/s link_


__FOR BOT CHATS__

_send link with '/b/', bot's username and message id, you might want to install some unofficial client to get the id like below_

```
https://t.me/b/botusername/4321
```

__MULTI POSTS__

_send public/private posts link as explained above with formate "from - to" to send multiple messages like below_


```
https://t.me/xxxx/1001-1010

https://t.me/c/xxxx/101 - 120
```

_note that space in between doesn't matter_

---

# Access & Commands

_Only the owner and users added with `/adduser` can use the bot (data is stored in `bot.db`, SQLite)._

- `/adduser <id>`, `/deluser <id>`, `/users`, `/stats` — owner only
- `/settings` — per-user options (BIN channel copy mode)
- `/cancel` — stop your running/queued jobs

_Optional variables: `WORKERS` (2), `MAX_PER_USER` (3), `MAX_RANGE` (100), `DELAY` (3), `DB_PATH` (bot.db)_
