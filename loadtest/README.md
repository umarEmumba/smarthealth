# Load test (k6)

A runnable load test for the booking path (mentor review D2). It applies the quantified non-functional requirements (NFR-01 to NFR-08, see [`docs/nfr.md`](../docs/nfr.md) and `.planning/REQUIREMENTS.md`) to the gateway and fails with a non-zero exit code when a target is crossed.

**What this is and is not.** The numbers are *targets*. This directory delivers the test that applies them. Phase 3 built the observability that reads a run (Prometheus histograms and the Grafana dashboard `smarthealth-booking-nfr`, see "Reading a run in Grafana" below); bottleneck analysis and sizing wait for the full profiles. The local Compose stack (one instance of each service) is not claimed to meet any profile: `smoke` crossed the latency thresholds twice (plan 02.1-13) and then passed every threshold in three runs (see the dated notes at the end), while `sustained`, `burst` and `contention` have **not been run** (deferred by the user until after Phase 3). See "What is verified and what is only a target" in `docs/nfr.md`.

**Local stack only.** The profiles send thousands of requests and need the gateway rate budget raised. Never run them against a shared or production environment, and never raise the gateway budget there. The runner targets only the URL you give it.

## Files

| File | Purpose |
|------|---------|
| `nfr.json` | Single source of the numeric targets (rates, durations, latency and error thresholds). The k6 script, the PRD table and `docs/nfr.md` all derive from it |
| `booking.js` | The k6 script: four profiles, thresholds read from `nfr.json` |
| `run.sh` | Runs one profile with the pinned k6 Docker image and exits with k6's exit code |
| `../scripts/loadtest_seed.py` | Builds the world through the gateway and writes `data/plan.json` |
| `data/plan.json` | Generated, mode 0600, gitignored. Holds a generated front-desk password and the booking plan |

## Pinned k6 image

`run.sh` runs `grafana/k6:1.8.2` (a concrete version, never `latest`). The tag was verified to exist with `docker manifest inspect grafana/k6:1.8.2` on 2026-10-25, and `k6 version` inside the image reported v1.8.2. A static test fails if the tag becomes `latest` or loses its version. To upgrade, verify the new tag with `docker manifest inspect`, change it in `run.sh`, update this section, and re-run the smoke profile. No k6, Node or Python package is installed for this; only Docker is needed.

## Prerequisites

1. The Compose stack is up: `docker compose -f infra/docker-compose.yml up -d` (the gateway on port 8000, plus Temporal and the scheduling worker, because the profiles wait for bookings to confirm).
2. An admin account exists. Create one with `scripts/bootstrap_admin.py` (see the README's First-Admin Bootstrap section).
3. The gateway budgets are raised for the run. The gateway limits each client address to 300 requests per minute (`GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`) and 20 per minute on the auth routes (`GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE`). The load test comes from one address, so add to `infra/.env`:

   ```
   GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=1000000
   GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000
   ```

   then restart the gateway: `docker compose -f infra/docker-compose.yml up -d gateway`. The auth budget only needs to cover the seed script's logins and the k6 `setup()` login. Restore the defaults afterwards. A 429 during the seed or a run means the budget is still too low.

## Seed the world

```
LOADTEST_ADMIN_EMAIL=admin@example.com LOADTEST_ADMIN_PASSWORD='...' \
  .venv/bin/python scripts/loadtest_seed.py
```

Defaults: 5 providers, 600 booking patients plus 200 reserved for the contention profile, 28 days of 5-minute slots between 06:00 and 22:00 Asia/Karachi, written to `loadtest/data/plan.json`. That gives about 26,000 bookable slots, enough for one smoke, one sustained and one burst run in order (150 + 15,000 + 6,000 plan entries). The script prints counts only; credentials come from the two environment variables and are never printed. A small world for the smoke profile: `--providers 2 --patients 250 --days 2`.

Each booking run consumes plan entries, because a booked slot cannot be booked again. The profiles start at different plan offsets (smoke 0, sustained 150, burst 15,150) so one seed serves one run of each. To repeat a profile, reseed, or set `PLAN_OFFSET` to a fresh entry (`PLAN_OFFSET=21150 loadtest/run.sh smoke`). If the plan runs out, the run fails with a message saying so.

## Run a profile

```
loadtest/run.sh smoke
loadtest/run.sh sustained
loadtest/run.sh burst
loadtest/run.sh contention
```

| Profile | Applies | Duration | Maps to |
|---------|---------|----------|---------|
| `smoke` | 5 booking requests per second | 30 s | plumbing check; crossed the latency thresholds on a heavily loaded laptop (plan 02.1-13), passed every threshold in three later runs (dated notes below) |
| `sustained` | `sustained_rps` (25) per second | `sustained_seconds` (600 s, 10 minutes) | NFR-01, NFR-03, NFR-04, NFR-05 |
| `burst` | `burst_rps` (100) per second | `burst_seconds` (60 s) | NFR-02 |
| `contention` | `contention_requests` (200) concurrent requests for one slot | a few seconds | NFR-07 |

`GATEWAY_URL` selects the gateway the container calls (default `http://host.docker.internal:8000`; the runner adds `--add-host=host.docker.internal:host-gateway` so this also works on Linux).

Run each profile as its own k6 run and keep each run under 12 minutes: the front-desk access token obtained in `setup()` lives 15 minutes and the script does not refresh it. The longest profile (`sustained`) is 10 minutes plus the confirmation polls.

## How to read the result

k6 prints each threshold with a tick or a cross and exits non-zero (99) when any is crossed, so the run is usable as a gate.

| Threshold | Meaning (target from `nfr.json`) |
|-----------|----------------------------------|
| `http_req_duration{endpoint:book}` | `POST /api/appointments` p95 and p99 (NFR-03) |
| `http_req_duration{endpoint:read_appointment}`, `{endpoint:read_slots}` | read latency p95 and p99 (NFR-04) |
| `booking_confirm_seconds` | time from the 202 to a final status, sampled on about 10 percent of iterations, p95 and p99 (NFR-05). The value is in seconds, not milliseconds |
| `booking_confirm_timeouts` | sampled bookings still `pending` after 30 s; must be 0 |
| `http_req_failed{endpoint:book}` | share of booking requests not answered 202 (404 and 409 count in the booking profiles), below `error_rate_max` (NFR-01, NFR-02) |
| `dropped_iterations` | must be 0: k6 could not start requests at the named rate |
| `contention_violations` | contention profile only: after the run, exactly one active appointment must exist for the slot (NFR-07) |

The contention profile also retries each accepted request with the same `Idempotency-Key` and checks that the same appointment comes back. `booking_saga_failed` and `contention_accepted` are informational. No token or password is logged by the script.

## Reading a run in Grafana

Phase 3 provisions the dashboard **`smarthealth-booking-nfr`** (Grafana, uid `smarthealth-booking-nfr`, from `infra/grafana/dashboards/smarthealth-booking-nfr.json`). Its panels are the Prometheus view of the same thresholds k6 checks: booking accept latency p95 and p99 (NFR-03), read latency p95 and p99 (NFR-04), booking saga completion p95 and p99 from `booking_saga_seconds` (NFR-05), the gateway 5xx ratio (NFR-02) and the outbox insert p95 (the one synchronous cost Phase 3 added to booking). The threshold lines are generated from `nfr.json` and a test keeps them equal. Open it with the time range set to the run. Two cautions: the HTTP histogram buckets put an edge on every threshold, so a panel is reliable for "above or below the line" but not for the exact quantile (k6's own figures are exact), and only `scheduling` was rebuilt with the finer buckets, which is the service these panels read. Grafana is published on the host port from `GRAFANA_HOST_PORT` (default 3000); the other dashboards (events, notifications, operations) show the event pipeline during a run.

## Recording a result

Numbers from the local stack are informational, not a gate. If you run the profiles, note the date, the host, the replica counts and the k6 end-of-test summary next to the result; a number without that context is not comparable.

## Latest smoke result (2026-11-10)

Re-run after Phase 2 closeout on a quiet laptop, Compose stack of one instance per service, `--providers 2 --patients 250 --days 2` seed, gateway budgets raised. **All thresholds passed (k6 exit 0)**: 151 iterations, 0 dropped; booking accept p95 21.8 ms / p99 78.9 ms (threshold 300 / 800 ms); reads p95 9 to 13 ms; saga completion p95 0.52 s (threshold 5 s), 0 confirm timeouts; booking error rate 0.00%. This contrasts with the two earlier runs in plan 02.1-13 (accept p95 2.0 to 6.0 s, saga p95 16.7 to 28.5 s), which were taken on a heavily loaded laptop, so those misses were environmental. The `sustained`, `burst` and `contention` profiles are still not run; they are deferred until after Phase 3 (user decision, 2026-11-10) so they measure the event-driven stack. NFR-01..08 remain targets, not measured results.

## Smoke results after Phase 3 (2026-11-11)

Two more runs of the `smoke` profile, with the same small seeded world (`--providers 2 --patients 250 --days 2`) and the gateway budgets raised. Both exited 0: **every threshold passed, 0 errors, 0 dropped iterations, 0 confirm timeouts** (151 iterations each).

| Run | Accept p95 / p99 | Saga completion p95 / p99 | Read appointment p95 / p99 | Read slots p95 / p99 | Host |
|-----|------------------|---------------------------|-----------------------------|----------------------|------|
| 2026-11-10, before Phase 3 (above) | 21.8 / 78.9 ms | 0.52 s / n.r. | 9 to 13 ms / n.r. | n.r. | quiet laptop |
| 2026-11-11, after the producers (plan 03-11) | 29.3 / 170.0 ms | 0.519 / 0.521 s | 9.7 / 11.8 ms | 15.3 / 16.8 ms | load average about 3 |
| 2026-11-11, finished Phase 3 stack (plan 03-16) | 49.1 / 200.8 ms | 0.519 / 0.520 s | 9.2 / 23.8 ms | 28.7 / 68.5 ms | load average 3.4 / 5.8 / 6.3 |
| 2026-11-12, all nine APIs in multiprocess metrics mode (plan 03-25) | 41.9 / 245.5 ms | 0.533 / 0.543 s (k6 confirm time) | 15.6 / 25.0 ms | 14.7 / 31.7 ms | load average 2.7 / 4.3 / 4.4 |
| Threshold | 300 / 800 ms | 5 / 15 s | 200 / 500 ms | 200 / 500 ms | |

(n.r. = not recorded.) Accept latency rose across the three runs (p95 about 2.2 times and p99 about 2.5 times the quiet-host baseline in the last), still far under the thresholds; the host was busier than for the baseline and a 30 s run has 151 booking samples, so a p99 is the slowest request or two. The cause was not isolated (one run each, no A/B); the outbox insert measured about 1 ms at p95 and is not on the 202 path. **Accept latency is the number to watch.** Saga completion is unchanged at about 0.52 s. `sustained`, `burst` and `contention` were not run.

## Smoke result on the corrected metrics stack (2026-11-12)

One run of the `smoke` profile after plan 03-25 put all nine APIs in Prometheus multiprocess mode (two gunicorn workers each, every scrape is the sum over both; this closes the CR-02 gap where HTTP counters reset). Same small seeded world (`--providers 2 --patients 250 --days 2`), gateway budgets raised, quiet host (load average 2.7 / 4.3 / 4.4). **k6 exit 0: every threshold passed**, 151 iterations, 0 dropped, 0 failed accepts, 0 confirm timeouts. Accept p95 41.85 ms / p99 245.49 ms (max 789.5 ms), confirm time p95 0.533 s / p99 0.543 s, read appointment p95 15.6 ms, read slots p95 14.7 ms. The reset count from Prometheus over the run window was 0 for each of the nine API jobs, so this is the first smoke whose Prometheus figures come from correct counters. As an accept-path overhead check for multiprocess mode: p95 is between the two earlier Phase 3 runs (29.3 and 49.1 ms) and the p99 is the highest of the three (245.5 against 170.0 and 200.8 ms) but rests on one 789.5 ms outlier among 151 samples; no measurable change can be claimed or excluded without an A/B run. `sustained`, `burst` and `contention` were **not run** (deferred by the user until after Phase 3); NFR-01 to NFR-08 stay targets that are not yet measured.
