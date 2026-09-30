"""Send the same signed event twice; no external webhook provider needed."""

import hashlib
import hmac
import json
import os
import time
import urllib.request

body = json.dumps({"type": "order.created", "order_id": 42}).encode()
key = "demo-order-" + str(time.time_ns())
timestamp = str(int(time.time()))
message = timestamp.encode() + b"." + key.encode() + b"." + body
signature = hmac.new(os.environ["WEBHOOK_SECRET"].encode(), message, hashlib.sha256).hexdigest()
for _ in range(2):
    request = urllib.request.Request(
        "http://127.0.0.1:8000/v1/events",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status, response.read().decode())
