"""Transactional event intake. The unique key is the concurrency boundary."""

import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4


class KeyConflict(Exception):
    """An idempotency key was reused for different bytes."""


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, event_key TEXT UNIQUE NOT NULL,
                digest TEXT NOT NULL, payload BLOB NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=5)

    def accept(self, key: str, digest: str, payload: bytes) -> tuple[str, bool]:
        # INSERT first: checking existence first would race with another request.
        db = self.connect()
        try:
            with db:
                event_id = str(uuid4())
                cursor = db.execute(
                    "INSERT OR IGNORE INTO events(id,event_key,digest,payload) VALUES (?,?,?,?)",
                    (event_id, key, digest, payload),
                )
                if cursor.rowcount == 1:
                    return event_id, False
                row = db.execute(
                    "SELECT id,digest FROM events WHERE event_key=?", (key,)
                ).fetchone()
                if row is None or row[1] != digest:
                    raise KeyConflict
                return str(row[0]), True
        finally:
            db.close()

    def status(self, event_id: str) -> str:
        with closing(self.connect()) as db:
            row = db.execute("SELECT status FROM events WHERE id=?", (event_id,)).fetchone()
            return str(row[0])
