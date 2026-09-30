"""One bounded worker. Run separately from the API."""

import asyncio
import hashlib
import hmac
import logging
import os
import random
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from relayguard.queue import Queue
from relayguard.store import Store

logger = logging.getLogger(__name__)


async def deliver(
    queue: Queue, client: httpx.AsyncClient, destination: str, secret: str, now: float | None = None
) -> bool:
    started = time.time() if now is None else now
    job = queue.claim(started)
    if job is None:
        return False
    timestamp = str(int(started))
    message = timestamp.encode() + b"." + job.event_id.encode() + b"." + job.payload
    headers = {
        "Content-Type": "application/json",
        "X-Event-ID": job.event_id,
        "X-Timestamp": timestamp,
        "X-Signature": hmac.new(secret.encode(), message, hashlib.sha256).hexdigest(),
    }
    code: int | None = None
    retry_after = 0.0
    try:
        # Total wall-clock deadline plus per-operation HTTPX timeouts.
        async with asyncio.timeout(10):
            async with client.stream(
                "POST", destination, content=job.payload, headers=headers
            ) as r:
                code = r.status_code
                value = r.headers.get("retry-after", "")
                if value.isascii() and value.isdigit() and len(value) < 10:
                    retry_after = min(float(value), 60)
        success = 200 <= code < 300
        transient = code in (408, 429) or code >= 500
        outcome = "delivered" if success else f"http_{code}"
    except (httpx.TransportError, TimeoutError):
        success, transient, outcome = False, True, "network_error"
    finished = time.time() if now is None else now
    retry = not success and transient and job.attempt < 5
    status = "delivered" if success else ("pending" if retry else "failed")
    delay = max(retry_after, min(2**job.attempt, 60) + random.uniform(0, 1)) if retry else 0
    accepted = queue.finish(job, finished, status, code, outcome, delay)
    logger.info(
        "event=%s attempt=%s status=%s completion_accepted=%s",
        job.event_id,
        job.attempt,
        status,
        accepted,
    )
    return True


def validate_destination(destination: str) -> None:
    url = urlsplit(destination)
    if url.username or url.password or not url.hostname:
        raise ValueError("Destination must have a host and no credentials")
    if url.scheme != "https" and not (
        url.scheme == "http" and url.hostname in ("127.0.0.1", "localhost", "receiver")
    ):
        raise ValueError("Use HTTPS except for the local test receiver")


async def run() -> None:
    destination = os.getenv("DESTINATION_URL", "http://127.0.0.1:9000/webhook")
    validate_destination(destination)
    secret = os.environ["DELIVERY_SECRET"]
    if len(secret) < 32:
        raise ValueError("DELIVERY_SECRET must be at least 32 characters")
    queue = Queue(Store(Path(os.getenv("DB_PATH", "data/events.db"))))
    async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
        while True:
            try:
                worked = await deliver(queue, client, destination, secret)
            except sqlite3.OperationalError:
                logger.warning("Storage unavailable; retrying polling")
                worked = False
            if not worked:
                await asyncio.sleep(0.5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    asyncio.run(run())
