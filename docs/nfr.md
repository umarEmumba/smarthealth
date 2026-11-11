# Non-functional requirements and the load test

This page explains the quantified non-functional requirements (NFR-01 to NFR-08) in the PRD (`.planning/REQUIREMENTS.md`), the demand model behind the numbers, the load profiles that apply them, and, plainly, which parts are verified today and which are only targets.

## Purpose

The project's core value is that booking, updating and completing an appointment stays reliable and consistent under load. That claim needs numbers: how much load, how fast, how available, and what must still be correct. The mentor review (D1, D2) asked for those numbers and for a repeatable way to apply them. This page and `loadtest/` are the answer. Phase 3 built the observability that reads the numbers (a histogram or metric and a Grafana panel per NFR, see "How each NFR is observed" below); running the full profiles against the finished stack is still to be done.

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

## How each NFR is observed (Phase 3)

Phase 3 added Prometheus histograms on the NFR-relevant paths and the Grafana dashboard `smarthealth-booking-nfr` (provisioned from `infra/grafana/dashboards`), whose threshold lines are generated from `loadtest/nfr.json` and checked against it by a test. The second column names what measures the NFR; the third says honestly whether a panel exists today. None of this changes a target or a status below: **sustained, burst and contention have not been run**, so nothing here is a measured result.

| ID | Metric (Prometheus) | Dashboard panel (`smarthealth-booking-nfr`) |
|----|---------------------|---------------------------------------------|
| NFR-01 | none in Prometheus; the k6 `sustained` profile's `http_req_failed` on the booking endpoint | No panel; k6 only |
| NFR-02 | `http_requests_total{status="5xx"}` ratio on the gateway job | "NFR-02 gateway 5xx ratio", threshold 0.01 |
| NFR-03 | `http_request_duration_seconds` for `POST /api/appointments` on the scheduling job | "NFR-03 booking accept latency p95" and "p99", thresholds 0.3 s and 0.8 s |
| NFR-04 | `http_request_duration_seconds` for `GET /api/appointments/{id}` and `GET /api/slots` on the scheduling job | "NFR-04 read latency p95" and "p99", thresholds 0.2 s and 0.5 s |
| NFR-05 | `booking_saga_seconds{outcome="confirmed"}` from the scheduling worker (row creation to confirm on the database clock; k6's `booking_confirm_seconds` measures the 202 to the observed final status, polling included) | "NFR-05 booking saga completion p95" and "p99", thresholds 5 s and 15 s |
| NFR-06 | `up` per scrape job exists, but there is no availability panel or month-long record | No panel; the outage behaviours are covered by the existing workflow tests and by the Phase 3 kill and outage e2e tests (Connect, consumers, the committing process) |
| NFR-07 | none; the k6 `contention` profile's teardown check, the concurrency suites and `scripts/demo_concurrent_booking.py` | No panel |
| NFR-08 | none; a design property (commit before 202), supported by the kill-after-commit tests | No panel; RTO not measured |

The booking path's own added cost is observable too: `outbox_insert_seconds{event_type}` (the only synchronous addition to booking, inside the confirm step, never on the 202 path) is on the same dashboard as "Outbox insert p95 (write-path cost)". For `appointment.booked` it read about 1 ms at p95 and about 3 ms at p99 after the Phase 3 smoke run.

**Histogram resolution.** The per-handler HTTP histogram has buckets 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 2.5 and 5 seconds, with an edge on every NFR latency threshold (one shared helper, tested). That makes a panel reliable for "above or below the threshold line"; it does not make a plotted quantile exact (a quantile with few samples in a bucket reads near that bucket's upper edge: the accept p95 plotted about 0.098 s while k6 measured 49 ms). Only `scheduling`, the service the booking panels read, was rebuilt with the new buckets; the other services pick them up on their next rebuild.

## Load profiles

`loadtest/booking.js` is a k6 script. Pick a profile with the `PROFILE` environment variable (`loadtest/run.sh <profile>`). Each profile is its own k6 run and stays under 12 minutes, because the front-desk access token lives 15 minutes.

| Profile | What it applies | Maps to |
|---------|-----------------|---------|
| `smoke` | 5 booking requests per second for 30 s (150 requests): checks the plumbing and the thresholds on a small stack | plumbing check; crossed the latency thresholds twice in plan 02.1-13, then passed every threshold in three runs (2026-11-10 and two on 2026-11-11, see below) |
| `sustained` | `sustained_rps` for `sustained_seconds` | NFR-01, NFR-03, NFR-04, NFR-05 |
| `burst` | `burst_rps` for `burst_seconds` | NFR-02 |
| `contention` | `contention_requests` virtual users, one request each, all for one slot with distinct patients and distinct idempotency keys; passes on 202 or 409, and a teardown check fails unless exactly one appointment is active | NFR-07 |

In the booking profiles every iteration books the next entry of a pre-built plan (a slot and a patient that never overlaps another of that patient's slots), expects 202 only, and for about 10 percent of iterations polls the appointment every 500 ms for up to 30 s to record how long it took to reach a terminal status. A share of iterations also read `GET /api/slots` and `GET /api/appointments/{id}`. The plan is built by `scripts/loadtest_seed.py` through the gateway; see `loadtest/README.md` for the commands.

## What is verified and what is only a target

This section is deliberately blunt.

- **Delivered and verified by tests in this repository:** the numbers are single-sourced (consistency test), the seed script builds a collision-free booking plan (property tests), the seed can build a world through the gateway (integration test), and the k6 script and runner are checked statically (thresholds read from `nfr.json`, a pinned k6 image, every profile and endpoint tag present).
- **Runnable but not run here as a gate:** the k6 profiles (`smoke` has been run by hand several times, see the dated notes below; `sustained`, `burst` and `contention` have not). A smoke test runs `loadtest/run.sh smoke` when Docker and the full Compose stack are available, and skips otherwise.
- **Not yet measured:** NFR-01 to NFR-08 against the real stack. The local Compose stack is one instance of each service, with two gunicorn workers per API, a single Postgres and a single Temporal server. The `smoke` profile was run against it twice in plan 02.1-13 (through `tests/e2e/test_loadtest_smoke.py`) and **crossed its latency thresholds both times**: booking accept p95 2.0 to 6.0 s against 300 ms, saga completion p95 16.7 to 28.5 s against 5 s, reads p95 about 1 to 1.8 s against 200 ms, and in the second run 2 of 151 accepts were not 202 (the first run had 0 failed accepts and 6 dropped iterations). Every accepted booking still confirmed and none timed out. Both runs were on a developer laptop under heavy load (load average about 18, Spotlight and antivirus busy) with no Jaeger collector running, so they are two uncontrolled observations, not a measurement. The stack is **not** claimed to meet NFR-01 or NFR-02, and `sustained`, `burst` and `contention` have not been run on it. Phase 3 built the observability (Prometheus, Grafana, Jaeger) that will read the full profiles, find the bottleneck and size replicas, but the user deferred the `sustained`, `burst` and `contention` runs until after Phase 3. Until they are run these are targets, justified from the demand model above, not results.
- **Existing evidence that is not the load test:** the concurrency suites and `scripts/demo_concurrent_booking.py` (50 parallel bookings for one slot) already show that the database arbitrates a single slot correctly. NFR-07 asks for the same at 200-way, which the `contention` profile does.

## Running it safely

The load test sends thousands of requests and must only target a local or dedicated test stack. It needs the gateway rate budgets raised for the run; never do that on a shared or production environment. See `loadtest/README.md` for prerequisites, the exact commands and how to read the thresholds.

**Latest smoke run (2026-11-10):** all thresholds passed on a quiet host (accept p95 21.8 ms, saga completion p95 0.52 s, 0 errors); the two earlier misses in plan 02.1-13 were taken on a heavily loaded laptop. `sustained`, `burst` and `contention` are deferred until after Phase 3, so NFR-01..08 stay unmeasured targets. Details: `loadtest/README.md`.

**Smoke run notes (2026-11-11, Phase 3).** The `smoke` profile (5 requests per second for 30 s, a seeded small world, gateway budgets raised) was run twice more, after the Phase 3 changes. **Every threshold passed both times, with 0 errors, 0 dropped iterations and 0 confirm timeouts.** The dated notes are kept, not merged, because the host load differed between them:

| Run | Booking accept p95 / p99 | Saga completion p95 | Reads p95 | Host |
|-----|--------------------------|---------------------|-----------|------|
| 2026-11-10, before Phase 3 (baseline above) | 21.8 / 78.9 ms | 0.52 s | 9 to 13 ms | quiet laptop |
| 2026-11-11, after the producers (plan 03-11) | 29.3 / 170.0 ms | 0.519 s | appointment 9.7 ms, slots 15.3 ms | load average about 3 |
| 2026-11-11, on the finished Phase 3 stack (plan 03-16) | 49.1 / 200.8 ms | 0.519 s | appointment 9.2 ms, slots 28.7 ms | load average 3.4 / 5.8 / 6.3 |
| Thresholds | 300 / 800 ms | 5 s (p99 15 s) | 200 ms (p99 500 ms) | |

Honest reading: accept latency is higher in each later run (p95 49.1 ms is about 2.2 times the quiet-host figure and p99 about 2.5 times), but still about 6 times and 4 times under the thresholds, and **accept latency crept up across the three runs, so it is the number to watch.** The host was busier than for the baseline and each run has only 151 booking samples, so a p99 is the slowest request or two. The cause was not isolated: the outbox insert measured about 1 ms at p95 and sits at the confirm step in the worker, not on the 202 path, so it cannot explain tens of milliseconds, and the finer histogram buckets add no work per request; but there was no A/B run. Saga completion time is unchanged. Read from Prometheus for the last run's window (bucket-interpolated, so approximate): accept p95 about 41 ms and p99 about 171 ms; saga p95 about 0.46 s and p99 about 0.60 s; end-to-end event lag p95 about 2.1 s for `analytics-projection` and 2.9 s for `notification-trigger`; notification delivery p95 about 0.05 s.

`sustained`, `burst` and `contention` were **not run** (deferred by the user until after Phase 3, decision of 2026-11-10), so NFR-01 to NFR-08 are still targets that are not yet measured. The Phase 3 smoke runs are evidence that the added outbox insert and the event pipeline did not break the plumbing or the smoke thresholds, not that the stack meets any profile.
