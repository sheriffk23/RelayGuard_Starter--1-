import asyncio
import hashlib
import hmac
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
from fastapi.testclient import TestClient

from relayguard.api import create_app
from relayguard.queue import Queue
from relayguard.receiver import create_receiver
from relayguard.store import Store
from relayguard.worker import deliver, validate_destination

SECRET = "s" * 32
ADMIN = "t" * 32


def queue_with_event(tmp_path):
    store = Store(tmp_path / "events.db")
    event_id, _ = store.accept("key", hashlib.sha256(b"{}").hexdigest(), b"{}")
    return Queue(store), event_id


def delivery(queue, handler, now):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await deliver(queue, client, "http://localhost/webhook", SECRET, now=now)

    return asyncio.run(run())


def test_retry_then_success(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    received = []

    def handler(request):
        received.append(request.headers["x-event-id"])
        return httpx.Response(503 if len(received) < 3 else 200)

    assert delivery(queue, handler, 100)
    assert not delivery(queue, handler, 101)
    assert delivery(queue, handler, 104)
    assert delivery(queue, handler, 110)
    detail = queue.detail(event_id)
    assert detail["status"] == "delivered"
    assert [a["http_status"] for a in detail["attempts"]] == [503, 503, 200]
    assert received == [event_id] * 3


def test_terminal_4xx_and_replay_audit(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    assert delivery(queue, lambda r: httpx.Response(400), 100)
    assert queue.detail(event_id)["status"] == "failed"
    assert queue.replay(event_id, 200)
    assert not queue.replay(event_id, 201)
    assert delivery(queue, lambda r: httpx.Response(200), 201)
    result = queue.detail(event_id)
    assert result["status"] == "delivered"
    assert len(result["attempts"]) == 2
    assert result["attempts"][1]["generation"] == 1
    assert len(result["replays"]) == 1


def test_exhaustion(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    for now in [100, 200, 300, 400, 500]:
        assert delivery(queue, lambda r: httpx.Response(503), now)
    assert queue.detail(event_id)["status"] == "failed"
    assert not delivery(queue, lambda r: httpx.Response(200), 600)


def test_timeout_retry(tmp_path):
    queue, event_id = queue_with_event(tmp_path)

    def timeout(request):
        raise httpx.ReadTimeout("simulated", request=request)

    delivery(queue, timeout, 100)
    result = queue.detail(event_id)
    assert result["status"] == "pending"
    assert result["attempts"][0]["outcome"] == "network_error"


def test_retry_after_bounded(tmp_path):
    queue, _ = queue_with_event(tmp_path)
    delivery(queue, lambda r: httpx.Response(429, headers={"Retry-After": "999999"}), 100)
    assert queue.list_events()[0]["next_at"] == 160


def test_concurrent_claims(tmp_path):
    queue, _ = queue_with_event(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = list(pool.map(lambda _: queue.claim(100), range(20)))
    assert sum(j is not None for j in jobs) == 1


def test_crash_recovery_and_stale_completion(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    first = queue.claim(100)
    assert queue.claim(129) is None
    # New Queue instance represents a restarted process.
    restarted = Queue(Store(queue.store.path))
    second = restarted.claim(131)
    assert first.token != second.token
    assert not queue.finish(first, 132, "delivered", 200, "delivered")
    assert restarted.finish(second, 132, "delivered", 200, "delivered")
    assert restarted.detail(event_id)["attempts"][0]["outcome"] == "lease_expired"


def test_expired_owner_cannot_finish(tmp_path):
    queue, _ = queue_with_event(tmp_path)
    job = queue.claim(100)
    assert not queue.finish(job, 131, "delivered", 200, "delivered")


def test_repeated_crashes_are_bounded(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    for i in range(5):
        assert queue.claim(100 + i * 31) is not None
    assert queue.claim(300) is None
    assert queue.detail(event_id)["status"] == "failed"


def test_operator_auth_and_replay(tmp_path):
    queue, event_id = queue_with_event(tmp_path)
    with TestClient(create_app(queue.store.path, SECRET, ADMIN)) as client:
        assert client.get("/v1/operator/events").status_code == 401
        assert client.post(f"/v1/operator/events/{event_id}/replay").status_code == 401
        auth = {"Authorization": "Bearer " + ADMIN}
        assert client.get("/v1/operator/events", headers=auth).status_code == 200
        assert client.get("/v1/operator/events/missing", headers=auth).status_code == 404
        assert (
            client.post(f"/v1/operator/events/{event_id}/replay", headers=auth).status_code == 409
        )
        delivery(queue, lambda r: httpx.Response(400), 100)
        assert (
            client.post(f"/v1/operator/events/{event_id}/replay", headers=auth).status_code == 200
        )
        assert client.get("/health/ready").status_code == 200
        assert "RelayGuard" in client.get("/").text


def test_receiver_failure_and_deduplication(tmp_path):
    path = tmp_path / "receiver.db"
    body, key, timestamp = b"{}", "stable-id", str(int(time.time()))
    signature = hmac.new(
        SECRET.encode(), timestamp.encode() + b"." + key.encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    headers = {"X-Event-ID": key, "X-Timestamp": timestamp, "X-Signature": signature}
    with TestClient(create_receiver(path, SECRET)) as client:
        assert [
            client.post("/webhook", content=body, headers=headers).status_code for _ in range(3)
        ] == [503, 503, 200]
    with TestClient(create_receiver(path, SECRET)) as client:
        assert client.post("/webhook", content=body, headers=headers).status_code == 200
        assert client.post("/webhook", content=b"{} ", headers=headers).status_code == 401
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT calls,applied FROM received").fetchone() == (4, 1)


def test_destination_validation():
    import pytest

    for value in ["http://example.com", "https://user:pass@example.com", "file:///tmp/a"]:
        with pytest.raises(ValueError):
            validate_destination(value)
    validate_destination("http://127.0.0.1:9000/webhook")
    validate_destination("https://example.com/webhook")
