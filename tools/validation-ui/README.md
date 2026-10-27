# Phase 2.1 validation UI

## What this is

A hand-validation aid for the Phase 2.1 behaviour of the SmartHealth stack: a static HTML + vanilla JS page that calls the real gateway and shows the raw request and raw response of every call in one shared log. Where the point of an action is a specific refusal (401 on a refresh replay, 422 past slot, 409 cancel after check-in, identical 404s) the log entry carries a PASS/FAIL badge against the expected status.

- The gateway has no CORS, so `tools/validation-ui/serve.py` serves the page and reverse-proxies every `/api/*` call same-origin to the gateway. Standard library only, no install step.
- Loopback only: it binds `127.0.0.1` (there is no host option) and answers 403 to any Host or Origin other than `127.0.0.1:<port>` / `localhost:<port>`.
- Tokens live in this tab's `sessionStorage` and are shown in full in the log. Keep the tab private; "Clear session data" wipes them. The typed password is never stored and is shown as `(hidden)` in the log.
- All data is synthetic. This is not a substitute for the pytest suites, and there is no automated browser test: the tests under `tests/tools` cover the proxy, the seeder, the script contracts and a fake-DOM runtime smoke, not how the page renders.

## Prerequisites

1. The Compose stack is up (see the root `README.md`).
2. The gateway limits are raised, because the UI bursts calls and polls (default is 300 requests per minute per IP and 20 per minute for login/register/refresh):

   ```bash
   GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 \
     docker compose -f infra/docker-compose.yml up -d gateway
   ```

   (the same step as in the `tests/e2e/test_phase2_flows.py` docstring). A 429 in the log means this was skipped; the log note quotes `Retry-After`.
3. An admin account (public registration only creates patients). Create one with `scripts/bootstrap_admin.py`.

## Run

```bash
.venv/bin/python tools/validation-ui/serve.py        # or: python3 tools/validation-ui/serve.py
```

Open <http://127.0.0.1:8099/> (use that exact host; `localhost:8099` also works, any other Host is refused with 403).

| Setting | Env | Flag | Default |
|---|---|---|---|
| Gateway | `GATEWAY_URL` | `--gateway-url` | `http://localhost:8000` |
| Port | `VALIDATION_UI_PORT` | `--port` | `8099` |

## Demo users

Recommended: the seeder (standard library, talks to the gateway only).

```bash
DEMO_ADMIN_EMAIL=... DEMO_ADMIN_PASSWORD=... .venv/bin/python tools/validation-ui/seed_demo_users.py
```

Admin credentials come from the environment only (`BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD` also work). The seeder refuses a gateway that is not on localhost/127.0.0.1/::1 unless `--allow-non-local` is given, because it creates accounts. It creates a clinic, a department, a provider with a linked login and 09:00-12:00 templates, a front_desk user, a patient with a linked login and profile, extra staff-created patient profiles (so lists page at small limits), slots from two days back to three days ahead, and a second provider with one slot inside the check-in window. It prints a credentials table once (the demo password is generated per run, or `DEMO_PASSWORD`, and is never written to disk; the admin password is never printed), the ids, and one `CONTEXT_JSON: {...}` line (ids only). Paste that line into "Load context JSON" in the page.

Manual shortest path, with the gotchas:

- Public `POST /api/auth/register` only creates patients; provider/front_desk/admin are created with `POST /api/auth/admin/users` as an admin.
- A patient needs a patient PROFILE linked by `user_id`, otherwise `/api/patients/me` is 404 and booking is 403.
- A provider needs a provider profile linked by `user_id`, otherwise provider reads return `[]`.
- Staff booking must send `patient_id`.

## Walkthrough

Log into the UI with each seeded user (Auth panel) and switch with "Act as". "must" rows carry a PASS badge.

| Step | Act as | Panel / button | Expected |
|---|---|---|---|
| 1 | patient | Auth: Login | 200; claims show jti, rjti, role, exp |
| 2 | patient | Auth: Refresh, then Replay old refresh token | refresh 200 with a NEW refresh token; replay 401 (PASS) |
| 3 | patient | Auth: Login with email in different case | 200 |
| 4 | patient | Auth: Logout, then refresh with logged-out token, then call API with logged-out access token | 204; both 401 (PASS) |
| 5 | patient (log in again) | Me: Try with every logged-in session | patient 200, every other role 403 |
| 6 | admin | Lists: Clinics, Departments of clinic, Providers with limit 2; Next/Prev | X-Total-Count visible; limit 500 gives 422 |
| 7 | front_desk | Lists: Patients with limit 2 | X-Total-Count visible, paging works |
| 8 | patient | Booking: load slots, pick, Book | 202, appointment goes pending to confirmed |
| 9 | patient | Booking: Replay last request | 202 with the SAME appointment id (PASS) |
| 10 | patient | Booking: coverage_ref `DENY-1`, Book | ends failed, reason BillingRejected |
| 11 | front_desk | Booking c: 1. Generate past slots, 2. Load past slots, try booking | 422 "slot has already started" (PASS) |
| 12 | patient | Booking: pick the seeder's window slot (provider = `window_provider_id`), Book | confirmed |
| 13 | front_desk | Visit: Check in (admin also fills clinic_id) | 201; appointment checked_in with history |
| 14 | front_desk | Visit: Cancel, Reschedule | both 409 (PASS) |
| 15 | front_desk | Visit: Start, Complete | 200 each; appointment ends completed |
| 16 | provider | Provider view: List my appointments | only own rows (PASS) |
| 17 | provider | Provider view: Compare the two 404s with the window-slot appointment id | identical 404 (PASS) |
| 18 | patient, then provider/admin | Schedule: book a confirmed upcoming slot of the first provider; deactivate its weekday template; unpause the poller; List flagged | `schedule_change_flagged` true, reason `template_deactivated` |

Curl equivalent of the past-slot procedure (through the proxy; replace the tokens and ids):

```bash
curl -s -X POST http://127.0.0.1:8099/api/slots/generate -H "Authorization: Bearer $STAFF_TOKEN" \
  -H 'Content-Type: application/json' -d '{"provider_id":"<id>","from_date":"2026-01-01","to_date":"2026-01-02"}'
curl -s "http://127.0.0.1:8099/api/slots?provider_id=<id>&start=2026-01-01T00%3A00%3A00%2B00%3A00&end=2026-01-03T00%3A00%3A00%2B00%3A00&status=any" \
  -H "Authorization: Bearer $STAFF_TOKEN"
curl -s -i -X POST http://127.0.0.1:8099/api/appointments -H "Authorization: Bearer $STAFF_TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-past-1' \
  -d '{"slot_id":"<past slot id>","patient_id":"<patient id>"}'
```

## Caveats

- The check-in window is clock-dependent. The seeder's window slot starts 5-35 minutes from running it, check-in opens 60 minutes before the start and closes at the slot end, and the slot cannot be built when fewer than 40 minutes remain before clinic-local midnight. Re-run the seeder for a fresh window slot.
- The Temporal Schedule `scheduling-schedule-change-poller` is paused on the local Temporal (it also acknowledges Provider events that integration tests read; see the root README). Live propagation of the schedule-change flag needs it unpaused via the Temporal UI (<http://localhost:8080> -> Schedules) or the `temporal schedule toggle` CLI command (verify the flags with `--help`), and re-paused afterwards.
- The Postman collection is stale on refresh tokens (rotation makes each refresh token single use), so this UI is the reference for refresh behaviour.

## Troubleshooting

| Symptom | Cause |
|---|---|
| 429 with Retry-After | raise the gateway limits (Prerequisites) |
| 502 / 504 from the proxy | gateway or stack is down or slow |
| 403 "forbidden host" / "forbidden origin" | open the page at `127.0.0.1:8099`, not another host name |
| patient booking 403 | the patient account has no linked profile (use the seeder) |
| check-in 409 "check-in window not open" | the slot starts more than 60 minutes ahead; use the window slot |
