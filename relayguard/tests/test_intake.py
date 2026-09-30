import hashlib
import hmac
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from relayguard.api import create_app
from relayguard.store import Store

SECRET = "a" * 32


def headers(body, key="order-42", timestamp=None):
    timestamp = str(timestamp if timestamp is not None else int(time.time()))
    message = timestamp.encode() + b"." + key.encode() + b"." + body
    return {
        "Idempotency-Key": key,
        "X-Timestamp": timestamp,
        "X-Signature": hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest(),
    }


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path / "events.db", SECRET)) as instance:
        yield instance


def test_duplicate(client):
    body = b'{"type":"order.created"}'
    first = client.post("/v1/events", content=body, headers=headers(body))
    second = client.post("/v1/events", content=body, headers=headers(body))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True


def test_conflict(client):
    for body, expected in [(b'{"n":1}', 202), (b'{"n":2}', 409)]:
        assert (
            client.post("/v1/events", content=body, headers=headers(body)).status_code == expected
        )


@pytest.mark.parametrize("body", [b"bad", b"[]", b"\xff"])
def test_bad_json(client, body):
    assert client.post("/v1/events", content=body, headers=headers(body)).status_code == 400


def test_signature(client):
    assert client.post("/v1/events", content=b"{} ", headers=headers(b"{}")).status_code == 401


def test_signed_key(client):
    signed = headers(b"{}")
    signed["Idempotency-Key"] = "changed"
    assert client.post("/v1/events", content=b"{}", headers=signed).status_code == 401


def test_expiry(client):
    assert (
        client.post("/v1/events", content=b"{}", headers=headers(b"{}", timestamp=0)).status_code
        == 401
    )


def test_size(client):
    body = b"x" * 65537
    assert client.post("/v1/events", content=body, headers=headers(body)).status_code == 413


def test_missing_headers(client):
    assert client.post("/v1/events", content=b"{}").status_code == 400


def test_restart(tmp_path):
    path = tmp_path / "events.db"
    receipts = []
    for _ in range(2):
        with TestClient(create_app(path, SECRET)) as instance:
            receipts.append(
                instance.post("/v1/events", content=b"{}", headers=headers(b"{}")).json()
            )
    assert receipts[0]["id"] == receipts[1]["id"]
    assert receipts[1]["duplicate"] is True


def test_concurrency(tmp_path):
    path = tmp_path / "events.db"
    store = Store(path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(lambda _: store.accept("same", "digest", b"{}"), range(20)))
    assert len({receipt[0] for receipt in receipts}) == 1
    assert sum(not receipt[1] for receipt in receipts) == 1
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_config(tmp_path):
    with pytest.raises(ValueError):
        create_app(tmp_path / "events.db", "short")
