# Load test (k6)

A runnable load test for the booking path (mentor review D2). It applies the quantified non-functional requirements (NFR-01 to NFR-08, see [`docs/nfr.md`](../docs/nfr.md) and `.planning/REQUIREMENTS.md`) to the gateway and fails with a non-zero exit code when a target is crossed.

**What this is and is not.** The numbers are *targets*. This directory delivers the test that applies them. Measuring the system against them (dashboards, bottleneck analysis, sizing) is Phase 3 observability work, and the local Compose stack (one instance of each service) is expected to pass `smoke` but is not claimed to meet `sustained` or `burst`. See "What is verified and what is only a target" in `docs/nfr.md`.

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
| `smoke` | 5 booking requests per second | 30 s | plumbing check; the local stack is expected to pass |
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

## Recording a result

Numbers from the local stack are informational, not a gate (measurement is Phase 3). If you run the profiles, note the date, the host, the replica counts and the k6 end-of-test summary next to the result; a number without that context is not comparable.
