"""Signed intake and authenticated operator endpoints."""

import hashlib
import hmac
import json
import os
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable
from importlib.resources import files
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from starlette.responses import Response

from relayguard.queue import Queue
from relayguard.store import KeyConflict, Store

MAX_BODY = 64 * 1024
WINDOW_SECONDS = 300


class Receipt(BaseModel):
    id: str
    duplicate: bool
    status: str = "pending"


def create_app(db_path: Path, secret: str, admin_token: str | None = None) -> FastAPI:
    if len(secret) < 32:
        raise ValueError("WEBHOOK_SECRET must be at least 32 characters")
    store = Store(db_path)
    app = FastAPI(title="RelayGuard", version="0.2.0")

    @app.middleware("http")
    async def headers(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/events", response_model=Receipt, status_code=202)
    async def accept(request: Request) -> Receipt:
        key = request.headers.get("idempotency-key", "")
        timestamp = request.headers.get("x-timestamp", "")
        signature = request.headers.get("x-signature", "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", key):
            raise HTTPException(400, "Invalid Idempotency-Key")
        if not timestamp.isascii() or not timestamp.isdigit() or len(timestamp) > 12:
            raise HTTPException(401, "Invalid signature")
        if abs(time.time() - int(timestamp)) > WINDOW_SECONDS:
            raise HTTPException(401, "Expired signature")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_BODY:
                raise HTTPException(413, "Payload exceeds 64 KiB")
            body.extend(chunk)
        raw = bytes(body)
        signed = timestamp.encode() + b"." + key.encode() + b"." + raw
        expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
        if not re.fullmatch(r"[0-9a-f]{64}", signature) or not hmac.compare_digest(
            expected, signature
        ):
            raise HTTPException(401, "Invalid signature")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(400, "Expected JSON object") from None
        if not isinstance(payload, dict):
            raise HTTPException(400, "Expected JSON object")
        try:
            # Keep blocking SQLite work off the async request loop.
            from starlette.concurrency import run_in_threadpool

            event_id, duplicate = await run_in_threadpool(
                store.accept, key, hashlib.sha256(raw).hexdigest(), raw
            )
        except KeyConflict:
            raise HTTPException(409, "Key already used for a different payload") from None
        except sqlite3.OperationalError:
            raise HTTPException(503, "Storage temporarily unavailable") from None
        return Receipt(id=event_id, duplicate=duplicate, status=store.status(event_id))

    queue = Queue(store)
    if admin_token is not None and len(admin_token) < 32:
        raise ValueError("ADMIN_TOKEN must be at least 32 characters")

    def operator(request: Request) -> None:
        if admin_token is None:
            raise HTTPException(503, "Operator access is not configured")
        if not hmac.compare_digest(
            request.headers.get("authorization", "").encode(), ("Bearer " + admin_token).encode()
        ):
            raise HTTPException(401, "Operator authentication required")

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> str:
        return files("relayguard").joinpath("dashboard.html").read_text()

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        from contextlib import closing

        try:
            with closing(store.connect()) as db:
                db.execute("SELECT 1 FROM events LIMIT 1")
        except sqlite3.OperationalError:
            raise HTTPException(503, "Storage unavailable") from None
        return {"status": "ready"}

    @app.get("/v1/operator/events", dependencies=[Depends(operator)])
    def events() -> list[dict[str, object]]:
        return queue.list_events()

    @app.get("/v1/operator/events/{event_id}", dependencies=[Depends(operator)])
    def detail(event_id: str) -> dict[str, object]:
        result = queue.detail(event_id)
        if result is None:
            raise HTTPException(404, "Event not found")
        return result

    @app.post("/v1/operator/events/{event_id}/replay", dependencies=[Depends(operator)])
    def replay(event_id: str) -> dict[str, str]:
        if not queue.replay(event_id, time.time()):
            raise HTTPException(409, "Only failed events can be replayed")
        return {"status": "pending"}

    return app


def app_factory() -> FastAPI:
    return create_app(
        Path(os.getenv("DB_PATH", "data/events.db")),
        os.environ["WEBHOOK_SECRET"],
        os.getenv("ADMIN_TOKEN"),
    )
