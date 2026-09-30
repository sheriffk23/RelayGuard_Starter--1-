# Status after version 0.2

Signed intake, SQLite leased delivery, retries, terminal failures, replay audit, a signed failure simulator and an HTML/JavaScript operator dashboard are implemented. PostgreSQL and React/TypeScript below remain planned. See DESIGN_DECISIONS.md for the changed milestone order.

# Project selection and implementation plan

Assumption: four weeks at 12–15 hours/week (48–60 hours). At two weeks, deliver only the backend and test receiver; defer the React dashboard. This project extends Sheriff Kuye's Python, React, REST, Docker and SQL experience and differs from CORA's repository comprehension and RAG focus.

## Three ideas

**RelayGuard — reliable webhook delivery.** Small businesses connect booking, payment and inventory systems using events, but temporary downtime and retries can lose or duplicate work. Build a durable intake API, asynchronous delivery worker and operator view. Demonstrate retries, duplicate handling and recovery with a deliberately unreliable receiver. This provides a concrete distributed systems story without needing real payment credentials.

**DeployLens — deployment health and rollback advisor.** Collect health checks and release metadata from a few local container services, detect regressions after deployment and explain whether a rollback is warranted. Build baseline comparison, incident timelines and a controllable failing service. Keep rollback advisory initially. It fits DevOps applications but risks becoming an ordinary uptime monitor unless release correlation is implemented well.

**SlotSafe — concurrent booking coordination.** Help a shared workshop allocate scarce equipment without double bookings. Implement transactional reservations, expiring holds and contention tests, with a small availability UI. The interesting work is enforcing overlapping time intervals and handling payment confirmation after hold expiry. It is suitable for full-stack roles but closer to the existing leave-management project.

Recommendation: RelayGuard. It adds reliability, failure recovery and observability to the existing AI and business-app portfolio. A failure demo and defensible tests matter more than the number of services.

## Stack

| Choice | Reason |
| --- | --- |
| Python 3.12, FastAPI, Pydantic | Existing Python strength; typed HTTP contracts and generated API docs |
| SQLite for milestone one | Immediate local run, transactional constraints; deliberately limited scaling |
| PostgreSQL + SQLAlchemy + Alembic in week two | Durable multi-worker queue, row locks, schema migrations |
| React + TypeScript + Vite in week three | Existing React experience with explicit UI types |
| httpx in worker | Outbound deadlines and controllable HTTP client tests |
| pytest, Ruff, strict mypy | Behaviour tests, conventions and type checks |
| Docker Compose + GitHub Actions | Reproducible service setup and automated review checks |

Do not add Redis, Kafka, Kubernetes or Terraform to the MVP. .NET would also be a sound choice for C#-focused vacancies; keep this implementation in one language rather than rewriting it mid-project.

## MVP acceptance criteria

1. Signed events persist before acknowledgement; concurrent duplicate requests produce one row.
2. Single configured destination receives stable event IDs; no arbitrary URL entry.
3. API transaction creates event and pending delivery together.
4. Worker claims due delivery with a lease, releases database locks before HTTP calls, and records attempts.
5. Retry transient timeouts, 429 and 5xx with bounded exponential backoff plus jitter. Other 4xx fail terminally. Bound Retry-After.
6. Expired leases recover after a worker crash. Crash after receiver success may duplicate delivery; receiver deduplicates.
7. After five unsuccessful attempts, expose failed status and an authenticated replay action with audit history.
8. Authenticated dashboard shows status, attempts and timing; never exposes secrets.
9. Automated failure demo, integration tests, CI and documented measurements.

Stretch: multiple tenants with scoped keys; circuit breaker; OpenTelemetry; per-destination concurrency limits; Terraform deployment. Add only after MVP tests and demo pass.

## Weekly plan

| Week | Work | Exit evidence |
| --- | --- | --- |
| 1, 12–15h | Intake scaffold, HMAC, persistence, idempotency, CI, Docker | Signed event demo plus concurrency and restart tests; starter implements this backend portion |
| 2, 12–15h | PostgreSQL migration, delivery/attempt schema, leased worker, failing receiver | Restart worker mid-job; delivery resumes; fake-clock retry tests |
| 3, 12–15h | Operator authentication, status API, React UI, controlled replay | Failed delivery visible; replay audited; unauthorised actions rejected |
| 4, 12–15h | Fault tests, measured load test, security review, README and GIF | Repeatable failure/recovery demo; measured limits; all checks pass |

If behind: cut the UI styling, multiple destinations and telemetry integrations. Preserve lease recovery, deduplication and tests.

## Step-by-step continuation

1. Run the signed-event demo and tests. Read api.py and store.py. Change the demo payload under the same key and explain the 409.
2. Add migrations and PostgreSQL. Keep existing intake acceptance tests and add concurrent tests against the actual PostgreSQL database; SQLite tests do not prove PostgreSQL locking behaviour.
3. Add deliveries and attempts. Use a unique `(event_id, destination_id)` constraint. Claim with `FOR UPDATE SKIP LOCKED`, record lease owner/token and expiry, then commit before making network calls.
4. Add one worker and a scripted receiver returning two 503 responses then 200. Inject the clock and random jitter for deterministic retry tests.
5. Guard completion updates with the lease token so a stale worker cannot overwrite a newer worker's result. Receiver still needs deduplication because lease ownership cannot prevent every duplicate HTTP side effect.
6. Add operator endpoints and dashboard only once recovery behaviour is demonstrated.

## Interview questions and answer outlines

1. **Why this problem?** Integration downtime causes missed work and retries cause duplicates. Show outage/recovery rather than describing a hypothetical large platform. CORA showed AI; this demonstrates backend reliability.
2. **Why not exactly once?** Receiver can commit then connection or worker can fail before local acknowledgement. Retry yields at-least-once delivery; stable IDs and receiver deduplication protect business effects. Intake deduplication alone is insufficient.
3. **How do concurrent duplicates behave?** Database unique constraint; insert first within transaction. Same bytes return original ID; different bytes conflict. Cite the 20-request concurrency test, without claiming it proves unlimited load.
4. **What if the database commits but the sender sees a timeout?** Sender retries with the same key and body and gets the existing receipt. Acknowledgement follows commit.
5. **Why use a database queue?** One transaction covers event and delivery creation, avoiding a database/broker dual write. Trade-offs include polling and database load. A broker becomes useful at measured higher throughput, with an outbox to bridge storage safely.
6. **How do retries avoid an outage storm?** Bound attempts and concurrency; exponential backoff, jitter, bounded Retry-After; distinguish terminal errors. Measure queue age as well as throughput. These are planned until implemented.
7. **What if a worker dies?** Lease expiry allows reclaiming; fencing token guards stale completion. Network call occurs outside database transaction. Crash-after-delivery still needs receiver deduplication.
8. **How is it secured?** HMAC binds timestamp, key and exact bytes; constant-time comparison; body limit; secret outside repository. Future operator auth, TLS and rate limits. Fixed destination reduces SSRF exposure; arbitrary URLs would require validation on resolved IPs, redirects and egress controls.
9. **How would you scale it?** Replace SQLite with PostgreSQL; add bounded workers using skip-locked claims; index due jobs, examine query plans and queue lag; enforce fairness. Present benchmark evidence before suggesting extra infrastructure.
10. **How did you test it?** Separate validation from persistence and later delivery policy. Current tests cover restart, contention, conflicts and tampering. Next use actual PostgreSQL integration tests, fake clocks, timeout receiver, crash recovery and end-to-end outage demo. Avoid saying planned tests already exist.

## Commit practice

Start with an honest initial scaffold commit. Next commits can be `feat: persist signed events idempotently`, `test: cover concurrent intake and restart`, `feat: add leased delivery worker`, and `docs: record retry and delivery guarantees`. Do not fabricate earlier dates or split finished work solely to simulate a longer development history.
