"""Local failure simulator with durable deduplication of business effects."""

import hashlib
import hmac
import os
import re
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response


def create_receiver(path: Path, secret: str, failures: int = 2) -> FastAPI:
    if len(secret) < 32 or failures < 0:
        raise ValueError("Invalid receiver configuration")
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS received (
            event_id TEXT PRIMARY KEY, digest TEXT NOT NULL, calls INTEGER NOT NULL,
            applied INTEGER NOT NULL DEFAULT 0)""")
    app = FastAPI(title="RelayGuard local test receiver")

    @app.post("/webhook")
    async def webhook(request: Request) -> Response:
        event_id = request.headers.get("x-event-id", "")
        timestamp = request.headers.get("x-timestamp", "")
        signature = request.headers.get("x-signature", "")
        if (
            not timestamp.isascii()
            or not timestamp.isdigit()
            or len(timestamp) > 12
            or abs(time.time() - int(timestamp)) > 300
            or len(event_id) > 128
            or not event_id.isascii()
            or not event_id
        ):
            raise HTTPException(401, "Invalid signature")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 65536:
                raise HTTPException(413, "Too large")
            body.extend(chunk)
        raw = bytes(body)
        message = timestamp.encode() + b"." + event_id.encode() + b"." + raw
        expected = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
        if not re.fullmatch(r"[0-9a-f]{64}", signature) or not hmac.compare_digest(
            expected, signature
        ):
            raise HTTPException(401, "Invalid signature")
        digest = hashlib.sha256(raw).hexdigest()
        with closing(sqlite3.connect(path, timeout=5)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT OR IGNORE INTO received(event_id,digest,calls) VALUES (?,?,0)",
                (event_id, digest),
            )
            row = db.execute(
                "SELECT digest,calls,applied FROM received WHERE event_id=?", (event_id,)
            ).fetchone()
            if row[0] != digest:
                raise HTTPException(409, "Changed payload")
            calls = row[1] + 1
            db.execute("UPDATE received SET calls=? WHERE event_id=?", (calls, event_id))
            if row[2]:
                return Response(status_code=200)
            if calls <= failures:
                return Response(status_code=503)
            # This flag stands in for a business effect, in the same transaction.
            db.execute("UPDATE received SET applied=1 WHERE event_id=?", (event_id,))
        return Response(status_code=200)

    return app


def app_factory() -> FastAPI:
    return create_receiver(
        Path(os.getenv("RECEIVER_DB_PATH", "data/receiver.db")),
        os.environ["DELIVERY_SECRET"],
        int(os.getenv("RECEIVER_FAILURES", "2")),
    )
