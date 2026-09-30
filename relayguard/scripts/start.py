"""Start the local API, receiver, worker and demo with one Windows-friendly command."""

import os
import secrets
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser
from pathlib import Path

root = Path(__file__).resolve().parents[1]
processes: list[subprocess.Popen[bytes]] = []
env = dict(os.environ)
for key in ("WEBHOOK_SECRET", "DELIVERY_SECRET", "ADMIN_TOKEN"):
    env.setdefault(key, secrets.token_hex(32))
env.setdefault("DB_PATH", str(root / "data/events.db"))
env.setdefault("RECEIVER_DB_PATH", str(root / "data/receiver.db"))
env["DESTINATION_URL"] = "http://127.0.0.1:9000/webhook"

try:
    for port in (8000, 9000):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                raise SystemExit(f"Port {port} is in use. Stop your old API with Ctrl+C first.")
    commands = [
        [
            "uvicorn",
            "relayguard.api:app_factory",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
        ],
        [
            "uvicorn",
            "relayguard.receiver:app_factory",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
        ],
        ["relayguard.worker"],
    ]
    for command in commands:
        processes.append(subprocess.Popen([sys.executable, "-m", *command], cwd=root, env=env))
    for _ in range(100):
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("A service stopped. Check the terminal error above.")
        try:
            with urllib.request.urlopen("http://127.0.0.1:8000/health/ready", timeout=1):
                break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("API did not become ready")
    print("\nDashboard: http://127.0.0.1:8000", flush=True)
    print("Operator token (paste into dashboard): " + env["ADMIN_TOKEN"], flush=True)
    print(
        "Receiver rejects the first two attempts; expect delivery after roughly 6–10 seconds.",
        flush=True,
    )
    subprocess.run([sys.executable, "scripts/demo.py"], cwd=root, env=env, check=True)
    webbrowser.open("http://127.0.0.1:8000")
    print("Keep this terminal open. Press Ctrl+C to stop all three services.", flush=True)
    while all(p.poll() is None for p in processes):
        time.sleep(0.5)
    raise RuntimeError("A service stopped unexpectedly; see its logs above.")
except KeyboardInterrupt:
    print("\nStopping RelayGuard...")
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
