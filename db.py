import sqlite3
import time


class DB:
    def __init__(self, path="bot.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                added_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cache (
                chat_ref TEXT NOT NULL,
                msg_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                file_id TEXT NOT NULL,
                caption TEXT,
                PRIMARY KEY (chat_ref, msg_id)
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_ref TEXT NOT NULL,
                msg_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            """
        )
        self.conn.commit()

    # users
    def add_user(self, uid):
        self.conn.execute("INSERT OR IGNORE INTO users VALUES (?, ?)", (uid, int(time.time())))
        self.conn.commit()

    def del_user(self, uid):
        self.conn.execute("DELETE FROM users WHERE id = ?", (uid,))
        self.conn.commit()

    def is_user(self, uid):
        return self.conn.execute("SELECT 1 FROM users WHERE id = ?", (uid,)).fetchone() is not None

    def users(self):
        return [r[0] for r in self.conn.execute("SELECT id FROM users ORDER BY added_at")]

    # file_id cache (resend without download/upload)
    def cache_get(self, chat_ref, msg_id):
        return self.conn.execute(
            "SELECT kind, file_id, caption FROM cache WHERE chat_ref = ? AND msg_id = ?",
            (chat_ref, msg_id),
        ).fetchone()

    def cache_put(self, chat_ref, msg_id, kind, file_id, caption):
        self.conn.execute(
            "INSERT OR REPLACE INTO cache VALUES (?, ?, ?, ?, ?)",
            (chat_ref, msg_id, kind, file_id, caption),
        )
        self.conn.commit()

    # job log / stats
    def log(self, user_id, chat_ref, msg_id, status):
        self.conn.execute(
            "INSERT INTO jobs (user_id, chat_ref, msg_id, status, ts) VALUES (?, ?, ?, ?, ?)",
            (user_id, chat_ref, msg_id, status, int(time.time())),
        )
        self.conn.commit()

    def stats(self):
        by_status = dict(self.conn.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status"))
        cached = self.conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
        users = self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        return by_status, cached, users
