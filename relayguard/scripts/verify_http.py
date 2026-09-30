"""Exercise real local API/worker/receiver processes using disposable databases.

Stop any running demo first. Run: python scripts/verify_http.py
"""

import hashlib
import hmac
import os
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx


def verify() -> None:
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ)
        env.update(
            WEBHOOK_SECRET=secrets.token_hex(32),
            DELIVERY_SECRET=secrets.token_hex(32),
            ADMIN_TOKEN=secrets.token_hex(32),
            DB_PATH=tmp + "/events.db",
            RECEIVER_DB_PATH=tmp + "/receiver.db",
            RECEIVER_FAILURES="2",
            DESTINATION_URL="http://127.0.0.1:19000/webhook",
        )
        commands = [
            [
                "uvicorn",
                "relayguard.api:app_factory",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                "18000",
            ],
            [
                "uvicorn",
                "relayguard.receiver:app_factory",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                "19000",
            ],
            ["relayguard.worker"],
        ]
        processes = []
        try:
            for command in commands:
                processes.append(
                    subprocess.Popen(
                        [sys.executable, "-m", *command],
                        cwd=root,
                        env=env,
                        stdout=subprocess.DEVNULL,
                    )
                )
            with httpx.Client(timeout=2, trust_env=False) as client:
                for _ in range(100):
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError("A service failed to start")
                    try:
                        ready = client.get("http://127.0.0.1:18000/health/ready").status_code == 200
                        receiver = (
                            client.get("http://127.0.0.1:19000/openapi.json").status_code == 200
                        )
                        if ready and receiver:
                            break
                    except httpx.TransportError:
                        pass
                    time.sleep(0.1)
                else:
                    raise RuntimeError("Services did not become ready")
                timestamp, body, key = (
                    str(int(time.time())),
                    b'{"type":"order.created"}',
                    "http-test",
                )
                message = timestamp.encode() + b"." + key.encode() + b"." + body
                signature = hmac.new(
                    env["WEBHOOK_SECRET"].encode(), message, hashlib.sha256
                ).hexdigest()
                headers = {
                    "Idempotency-Key": key,
                    "X-Timestamp": timestamp,
                    "X-Signature": signature,
                }
                first = client.post(
                    "http://127.0.0.1:18000/v1/events", content=body, headers=headers
                )
                first.raise_for_status()
                second = client.post(
                    "http://127.0.0.1:18000/v1/events", content=body, headers=headers
                )
                second.raise_for_status()
                assert second.json()["duplicate"] and second.json()["id"] == first.json()["id"]
                auth = {"Authorization": "Bearer " + env["ADMIN_TOKEN"]}
                for _ in range(160):
                    response = client.get(
                        "http://127.0.0.1:18000/v1/operator/events/" + first.json()["id"],
                        headers=auth,
                    )
                    response.raise_for_status()
                    detail = response.json()
                    if detail["status"] == "delivered":
                        break
                    time.sleep(0.1)
                assert detail["status"] == "delivered", detail
                assert [a["http_status"] for a in detail["attempts"]] == [503, 503, 200], detail
                print("PASS: duplicate intake, real HTTP retries 503 -> 503 -> 200, delivered")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    verify()
