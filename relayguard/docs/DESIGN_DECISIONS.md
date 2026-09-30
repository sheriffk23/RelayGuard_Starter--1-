# Decisions in milestone two

## SQLite remains the local queue

The previous plan proposed PostgreSQL next. For this iteration we prioritised a complete runnable failure/recovery demonstration without a database installation. Processes share a local file and writers are serialised using short immediate transactions. PostgreSQL is still necessary for demonstrating multi-machine worker coordination. Do not claim this release implements PostgreSQL, row-level locking or distributed scaling.

## Small dashboard before a frontend toolchain

A dependency-free HTML/JavaScript dashboard is packaged with the Python application. It removes npm setup from this milestone and focuses review on delivery semantics. React/TypeScript remains a planned improvement rather than an implemented feature. The backend has strict types; the dashboard JavaScript does not have compile-time type checking.

## Separate worker process

The API accepts and stores events without waiting for the destination. A worker can restart independently. A worker holds no database transaction open while talking to the receiver. Lease tokens guard local completion; they cannot guarantee exactly once network effects.

## Bounded retries and recoverable failures

The worker retries known transient failures, limits both time and attempts, and retains terminal failures for explicit replay. Attempt rows are written at claim time, so a crash is visible as lease_expired. Replay increments a generation and preserves history. Anonymous operator actions are rejected. The audit identifies replay time, not a human identity, because this milestone uses a single operator token.

## Durable receiver deduplication

A stable UUID is sent in X-Event-ID. Receiver call count and applied flag are committed in one database transaction. The flag stands in for a business action. Real actions in another external system would need that system's idempotency mechanism or an outbox, not merely this local flag.

## Known limits

No tenant isolation, deployment metrics, actual PostgreSQL tests or benchmark evidence. Readiness checks readability, not ability to commit a write. Single destination for all pending events; changing it redirects all future attempts. No per-event routing/versioning. Lease expiry and remote acknowledgement loss can duplicate requests. Receiver's synchronous SQLite operations and worker's short synchronous queue transactions fit the local demo; larger deployments should account for event-loop blocking and storage contention.

## What to demonstrate in an interview

Start services; show the same signed event submitted twice; open one event in the dashboard; show 503, 503, 200; discuss acknowledgement loss and why receiver deduplication is separate from intake deduplication. Then set RECEIVER_FAILURES=99 to show bounded failure, restart with 0 and replay while retaining history. Explain the SQLite and dashboard scope choices candidly.
