"""Durable delivery queue with expiring claims and guarded completion."""

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from uuid import uuid4

from relayguard.store import Store


@dataclass(frozen=True)
class Job:
    event_id: str
    token: str
    payload: bytes
    attempt: int


class Queue:
    def __init__(self, store: Store) -> None:
        self.store = store
        with closing(store.connect()) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS deliveries (
                event_id TEXT PRIMARY KEY REFERENCES events(id),
                tries INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
                lease_until REAL NOT NULL DEFAULT 0, token TEXT,
                generation INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY, event_id TEXT NOT NULL,
                attempt INTEGER NOT NULL, generation INTEGER NOT NULL,
                started_at REAL NOT NULL, finished_at REAL,
                outcome TEXT NOT NULL, http_status INTEGER)""")
            db.execute("""CREATE TABLE IF NOT EXISTS replays (
                id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, created_at REAL NOT NULL)""")
            db.execute("""CREATE INDEX IF NOT EXISTS attempts_event ON attempts(event_id,id)""")

    def claim(self, now: float, lease: float = 30) -> Job | None:
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            # Backfill pending events, including those created by the original starter.
            # The event itself is the durable queue; no second write at intake is needed.
            db.execute("""INSERT OR IGNORE INTO deliveries(event_id)
                SELECT id FROM events WHERE status='pending' """)
            row = db.execute(
                """SELECT e.id,e.payload,d.tries,d.generation,d.token
                FROM events e JOIN deliveries d ON d.event_id=e.id
                WHERE e.status IN ('pending','delivering') AND d.next_at<=?
                  AND d.lease_until<=? ORDER BY e.created_at,e.id LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            event_id, payload, tries, generation, old_token = row
            if old_token:
                db.execute(
                    """UPDATE attempts SET outcome='lease_expired',finished_at=?
                    WHERE event_id=? AND finished_at IS NULL""",
                    (now, event_id),
                )
            # Five claimed attempts per replay cycle, even if every worker crashes.
            if tries >= 5:
                db.execute("UPDATE events SET status='failed' WHERE id=?", (event_id,))
                db.execute("UPDATE deliveries SET token=NULL WHERE event_id=?", (event_id,))
                return None
            token = str(uuid4())
            attempt = int(tries) + 1
            db.execute(
                """UPDATE deliveries SET token=?,lease_until=?,tries=? WHERE event_id=?""",
                (token, now + lease, attempt, event_id),
            )
            db.execute("UPDATE events SET status='delivering' WHERE id=?", (event_id,))
            db.execute(
                """INSERT INTO attempts(event_id,attempt,generation,started_at,outcome)
                VALUES (?,?,?,?,'in_progress')""",
                (event_id, attempt, generation, now),
            )
            return Job(str(event_id), str(token), bytes(payload), attempt)

    def finish(
        self, job: Job, now: float, status: str, code: int | None, outcome: str, delay: float = 0
    ) -> bool:
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                """UPDATE deliveries SET token=NULL,lease_until=0,next_at=?
                WHERE event_id=? AND token=? AND lease_until>?""",
                (now + delay, job.event_id, job.token, now),
            )
            if cursor.rowcount != 1:
                return False
            db.execute("UPDATE events SET status=? WHERE id=?", (status, job.event_id))
            db.execute(
                """UPDATE attempts SET finished_at=?,outcome=?,http_status=?
                WHERE event_id=? AND finished_at IS NULL""",
                (now, outcome, code, job.event_id),
            )
            return True

    def replay(self, event_id: str, now: float) -> bool:
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                "UPDATE events SET status='pending' WHERE id=? AND status='failed'", (event_id,)
            )
            if cursor.rowcount != 1:
                return False
            db.execute(
                """UPDATE deliveries SET tries=0,next_at=?,lease_until=0,token=NULL,
                generation=generation+1 WHERE event_id=?""",
                (now, event_id),
            )
            db.execute("INSERT INTO replays(event_id,created_at) VALUES (?,?)", (event_id, now))
            return True

    def list_events(self, limit: int = 100) -> list[dict[str, object]]:
        with closing(self.store.connect()) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                """SELECT e.id,e.event_key,e.status,e.created_at,
                COALESCE(d.tries,0) AS tries,COALESCE(d.next_at,0) AS next_at
                FROM events e LEFT JOIN deliveries d ON e.id=d.event_id
                ORDER BY e.created_at DESC,e.id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def detail(self, event_id: str) -> dict[str, object] | None:
        with closing(self.store.connect()) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(
                "SELECT id,event_key,status,payload FROM events WHERE id=?", (event_id,)
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["payload"] = json.loads(result["payload"])
            result["attempts"] = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM attempts WHERE event_id=? ORDER BY id", (event_id,)
                )
            ]
            result["replays"] = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM replays WHERE event_id=? ORDER BY id", (event_id,)
                )
            ]
            return result
