# Non-functional requirements and the load test

This page explains the quantified non-functional requirements (NFR-01 to NFR-08) in the PRD (`.planning/REQUIREMENTS.md`), the demand model behind the numbers, the load profiles that apply them, and, plainly, which parts are verified today and which are only targets.

## Purpose

The project's core value is that booking, updating and completing an appointment stays reliable and consistent under load. That claim needs numbers: how much load, how fast, how available, and what must still be correct. The mentor review (D1, D2) asked for those numbers and for a repeatable way to apply them. This page and `loadtest/` are the answer. Measuring the system against the numbers is Phase 3 observability work.

## Single source of the numbers

`loadtest/nfr.json` is the one place the numbers live. The machine-readable table in `.planning/REQUIREMENTS.md`, the table in this page and the k6 thresholds in `loadtest/booking.js` all derive from it, and `tests/loadtest/test_nfr_consistency.py` fails if the PRD table or this page's table drifts from the JSON, or if the k6 script copies a number instead of reading it. To change a target, edit `nfr.json` first, then the two tables.

## Demand model

The project constraint is "tens of thousands of appointment requests, concurrent provider schedule updates, peak booking windows, and multi-clinic operations". The model turns that into a rate:

- 50,000 booking requests per day.
- 60 percent of them fall inside a 2-hour peak: 30,000 / 7,200 s is about 4.2 requests per second on average in the peak.
- A 5x peak minute is about 21 requests per second, rounded up to **25** for margin. This is the sustained profile (NFR-01).
- A **100 requests per second** burst for 60 s models an opening-time spike, for example a clinic network releasing a day's slots at once (NFR-02).
- Reads follow bookings: every booking is followed by status polls, so read latency is held to a tighter bound than write latency would suggest (NFR-04).

The figures are an engineering estimate, not observed traffic. If the real demand model differs, change `nfr.json` and re-run.

## The requirements

| ID | Requirement | Target | How it will be measured | Status |
|----|-------------|--------|-------------------------|--------|
| NFR-01 | Sustained booking intake | 25 req/s for 600 s (15,000 requests), at least 99 percent answered 202 | k6 `sustained` profile, `http_req_failed` on the booking endpoint | Target defined; not yet measured |
| NFR-02 | Peak-window burst | 100 req/s for 60 s (6,000 requests), 5xx below 1 percent | k6 `burst` profile | Target defined; not yet measured |
| NFR-03 | Booking accept latency under NFR-01 load | `POST /api/appointments` p95 <= 300 ms, p99 <= 800 ms | k6 Trend tagged `endpoint:book` | Target defined; not yet measured |
| NFR-04 | Read latency under NFR-01 load | `GET /api/appointments/{id}` and `GET /api/slots` p95 <= 200 ms, p99 <= 500 ms | k6 Trends tagged `read_appointment`, `read_slots` | Target defined; not yet measured |
| NFR-05 | Saga completion under NFR-01 load | 202 to terminal status p95 <= 5 s, p99 <= 15 s; every accepted booking terminal within 15 minutes across a worker restart | k6 custom Trend `booking_confirm_seconds`; the restart case by the existing worker-crash e2e test and Phase 3 dashboards | Target defined; the restart case is covered by an existing e2e test, the rest not yet measured |
| NFR-06 | Availability | 99.9 percent per month (at most 43.8 minutes); Temporal outage still accepts bookings and recovers within 5 minutes; Billing outage compensates cleanly | Phase 3 availability dashboards and outage drills; the outage behaviours are covered by existing workflow tests | Target defined; not yet measured |
| NFR-07 | Correctness under contention | 200 concurrent requests for one slot give exactly one active appointment; no duplicate side effect under retry | k6 `contention` profile with a teardown check; the concurrency pytest suites and `scripts/demo_concurrent_booking.py` | Covered by existing concurrency tests; the 200-way profile is runnable, not yet run in CI |
| NFR-08 | Durability | RPO 0 for an accepted appointment, RTO at most 15 minutes | Design property (commit before 202); recovery drills in Phase 3 | Target defined; RTO not yet measured |

The targets are checked against the same keys in this table, which must equal `loadtest/nfr.json`:

| key | value | unit |
|-----|-------|------|
| sustained_rps | 25 | requests per second |
| sustained_seconds | 600 | seconds |
| burst_rps | 100 | requests per second |
| burst_seconds | 60 | seconds |
| contention_requests | 200 | concurrent requests |
| book_p95_ms | 300 | milliseconds |
| book_p99_ms | 800 | milliseconds |
| read_p95_ms | 200 | milliseconds |
| read_p99_ms | 500 | milliseconds |
| confirm_p95_s | 5 | seconds |
| confirm_p99_s | 15 | seconds |
| error_rate_max | 0.01 | ratio |
| availability_pct | 99.9 | percent per month |

## Load profiles

`loadtest/booking.js` is a k6 script. Pick a profile with the `PROFILE` environment variable (`loadtest/run.sh <profile>`). Each profile is its own k6 run and stays under 12 minutes, because the front-desk access token lives 15 minutes.

| Profile | What it applies | Maps to |
|---------|-----------------|---------|
| `smoke` | 5 booking requests per second for 30 s (150 requests): checks the plumbing and the thresholds on a small stack | plumbing check; run twice against the local stack in plan 02.1-13 and it crossed the latency thresholds (see below) |
| `sustained` | `sustained_rps` for `sustained_seconds` | NFR-01, NFR-03, NFR-04, NFR-05 |
| `burst` | `burst_rps` for `burst_seconds` | NFR-02 |
| `contention` | `contention_requests` virtual users, one request each, all for one slot with distinct patients and distinct idempotency keys; passes on 202 or 409, and a teardown check fails unless exactly one appointment is active | NFR-07 |

In the booking profiles every iteration books the next entry of a pre-built plan (a slot and a patient that never overlaps another of that patient's slots), expects 202 only, and for about 10 percent of iterations polls the appointment every 500 ms for up to 30 s to record how long it took to reach a terminal status. A share of iterations also read `GET /api/slots` and `GET /api/appointments/{id}`. The plan is built by `scripts/loadtest_seed.py` through the gateway; see `loadtest/README.md` for the commands.

## What is verified and what is only a target

This section is deliberately blunt.

- **Delivered and verified by tests in this repository:** the numbers are single-sourced (consistency test), the seed script builds a collision-free booking plan (property tests), the seed can build a world through the gateway (integration test), and the k6 script and runner are checked statically (thresholds read from `nfr.json`, a pinned k6 image, every profile and endpoint tag present).
- **Runnable but not run here as a gate:** the k6 profiles. A smoke test runs `loadtest/run.sh smoke` when Docker and the full Compose stack are available, and skips otherwise.
- **Not yet measured:** NFR-01 to NFR-08 against the real stack. The local Compose stack is one instance of each service, with two gunicorn workers per API, a single Postgres and a single Temporal server. The `smoke` profile was run against it twice in plan 02.1-13 (through `tests/e2e/test_loadtest_smoke.py`) and **crossed its latency thresholds both times**: booking accept p95 2.0 to 6.0 s against 300 ms, saga completion p95 16.7 to 28.5 s against 5 s, reads p95 about 1 to 1.8 s against 200 ms, and in the second run 2 of 151 accepts were not 202 (the first run had 0 failed accepts and 6 dropped iterations). Every accepted booking still confirmed and none timed out. Both runs were on a developer laptop under heavy load (load average about 18, Spotlight and antivirus busy) with no Jaeger collector running, so they are two uncontrolled observations, not a measurement. The stack is **not** claimed to meet NFR-01 or NFR-02, and `sustained`, `burst` and `contention` have not been run on it. Phase 3 observability (Prometheus, Grafana, Jaeger) measures the full profiles, finds the bottleneck and sizes replicas. Until then these are targets, justified from the demand model above, not results.
- **Existing evidence that is not the load test:** the concurrency suites and `scripts/demo_concurrent_booking.py` (50 parallel bookings for one slot) already show that the database arbitrates a single slot correctly. NFR-07 asks for the same at 200-way, which the `contention` profile does.

## Running it safely

The load test sends thousands of requests and must only target a local or dedicated test stack. It needs the gateway rate budgets raised for the run; never do that on a shared or production environment. See `loadtest/README.md` for prerequisites, the exact commands and how to read the thresholds.
