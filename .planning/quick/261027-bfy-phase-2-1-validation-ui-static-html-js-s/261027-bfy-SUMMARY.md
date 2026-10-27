---
phase: 261027-bfy-phase-2-1-validation-ui-static-html-js-s
plan: 01
subsystem: tooling
tags: [validation-ui, reverse-proxy, vanilla-js, stdlib, seeder]
requires:
  - phase: 02.1
    provides: refresh rotation, /patients/me, pagination, check-in state machine, schedule-change flagging, past-slot refusal
provides:
  - tools/validation-ui (serve.py proxy, 7-panel page, seeder, README)
  - tests/tools (33 stack-free tests)
affects: [phase 2.1 human verification]
status: complete
live_walkthrough: partial
commits: 3
plan_head_before: d0c3531f336b27f40056baa6f7d0dfb5efb500f6
plan_head_after: 2fb042f73c09d04de395966a7e6b7070b9ff3dc8
actuals:
  tokens: 41500
  tasks: 3
  commits: 3
tech-stack:
  added: []
  patterns: [same-origin loopback reverse proxy, DOM builder with attribute allowlist and textContent only, strict CSP]
key-files:
  created:
    - tools/validation-ui/serve.py
    - tools/validation-ui/index.html
    - tools/validation-ui/style.css
    - tools/validation-ui/lib.js
    - tools/validation-ui/core.js
    - tools/validation-ui/panels_booking.js
    - tools/validation-ui/panels_ops.js
    - tools/validation-ui/seed_demo_users.py
    - tools/validation-ui/README.md
    - tests/tools/__init__.py
    - tests/tools/test_validation_ui.py
    - tests/tools/fake_dom_smoke.js
  modified: []
requirements-completed: [C1, C3, C4, C5, M1, M2, S6, SCHED-04, VISIT-01, VISIT-02, VISIT-03]
---

# Quick 261027-bfy: Phase 2.1 validation UI Summary

Static HTML + vanilla JS validation UI served same-origin by a stdlib loopback reverse proxy in front of the gateway, with a stdlib demo-world seeder, a README walkthrough and 33 stack-free tests.

## Run command

```bash
.venv/bin/python tools/validation-ui/serve.py        # then open http://127.0.0.1:8099/
```

Prerequisites (README): stack up, gateway limits raised
(`GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 docker compose -f infra/docker-compose.yml up -d gateway`),
an existing admin. Demo users: `DEMO_ADMIN_EMAIL=... DEMO_ADMIN_PASSWORD=... .venv/bin/python tools/validation-ui/seed_demo_users.py`.

## Task commits

| Task | Commit | Content |
|------|--------|---------|
| 1 (tracer) | eb57403 | serve.py proxy, page shell, lib.js, core.js (safe DOM builder, log, api(), Auth panel), 18 proxy tests |
| 2 | 9c87a86 | panels 2-7, style.css, JS contract tests, fake-DOM runtime smoke |
| 3 | 2fb042f | seed_demo_users.py, README.md, seeder/README tests |

## Demo credentials handling

The seeder takes admin credentials only from the environment (`DEMO_ADMIN_EMAIL`/`DEMO_ADMIN_PASSWORD`, falling back to `BOOTSTRAP_ADMIN_*`); the admin password is never printed. The demo-user password is `DEMO_PASSWORD` or `secrets.token_urlsafe(12)` per run. It is printed once, in the seeder's stdout credentials table (provider, front_desk, patient), and never written to disk. The `CONTEXT_JSON:` line holds ids only. The seeder refuses a non-local gateway (exit 2) without `--allow-non-local`.

## Verification

| Check | Result |
|-------|--------|
| `OTEL_SDK_DISABLED=true .venv/bin/python -m pytest tests/tools -q -p no:cacheprovider` | 33 passed (no stack involved) |
| `node --check` lib.js, core.js, panels_booking.js, panels_ops.js | all OK |
| `serve.py --help`, `seed_demo_users.py --help` | exit 0, `--gateway-url` / `DEMO_ADMIN_EMAIL` present |
| `git status --porcelain -- services libs pyproject.toml package.json package-lock.json` | empty (gate clean) |
| serve.py also starts on system python3 (3.14) | `--help` OK |
| Full repository test suite | NOT run (out of scope; no service code changed) |

The fake-DOM smoke (`tests/tools/fake_dom_smoke.js`) loads the four scripts with a minimal DOM stub and a stub fetch and drives login, upper-case login, refresh, replay, Me and a list load. It is a runtime smoke, not a browser rendering check.

## Live walkthrough: PARTIAL

The Compose stack was down at the start (only unrelated `tjo-*` containers ran) and no SmartHealth images existed locally. I pulled and built the stack with `docker compose -f infra/docker-compose.yml up -d --build` and the raised gateway limits. First attempt failed on host port 3001 (Grafana; held by an unrelated host `node` process, which I left alone); the second `up -d` used `GRAFANA_HOST_PORT=3010` for this invocation only, no file changed. All services reached healthy; `/health` returned `{"status":"ok","service":"gateway"}`.

**Admin credentials were not available to me.** The project's local env files are protected by a secret-read guard hook that blocked reading them, and I did not work around it nor create a new admin account. So the seeder was NOT run against the live stack, and every admin- or staff-dependent step was NOT run: clinics/departments/providers/patients lists and pagination (X-Total-Count, limit 500 -> 422), booking and replay with the same key, polling to confirmed, DENY- coverage, past-slot 422, window slot, check-in, cancel/reschedule 409, start/complete, provider own-view and identical 404s, schedule-change flagging. Also not run: any visual browser check of the page rendering.

What I ran, through `serve.py` on 127.0.0.1:8099 with a public self-registered synthetic patient (`validation-ui-<hex>@example.com`, random password not recorded) and HTTP calls only (driver kept in the session scratchpad, not committed):

| Step | Observed status |
|------|-----------------|
| GET / (CSP header present) | 200 |
| GET /_ui/config | 200 `{"gateway_url": "http://localhost:8000"}` |
| Host `evil.example` | 403 forbidden host |
| Origin `http://evil.example` | 403 forbidden origin |
| login unknown user | 401 "invalid email or password" (gateway answer through the proxy) |
| register patient (public) | 201, role patient |
| login | 200; claims contain jti, rjti, role, exp; access.rjti equals refresh.jti |
| login with upper-cased email (C3) | 200 |
| refresh (rotation) | 200, new refresh token differs |
| replay OLD refresh token (C1) | 401 "refresh token already used or revoked" |
| GET /api/patients/me (patient with no linked profile) | 404 "patient profile not found" |
| GET /api/clinics?limit=500 as patient | 403 "insufficient role for this operation" |
| GET /api/slots without provider_id | 422 |
| POST /api/appointments random slot as unlinked patient | 403 "patients may only book for themselves" |
| logout | 204 |
| refresh with the logged-out refresh token | 401 |
| GET /api/appointments with logged-out access token | 401 "token has been revoked" |
| GET /api/nothing-here | 404 (gateway answer) |

21 of 21 checks matched expectations. One extra check (bad Idempotency-Key characters) was inconclusive: the body failed UUID validation first (422), so the key rule was not exercised.

Schedule `scheduling-schedule-change-poller`: on this freshly started stack it was created UNPAUSED (Paused: false); I paused it (`temporal schedule toggle --pause` inside the temporal container) and confirmed `Paused: true`. It was not unpaused for any test. The other five Schedules were left as the workers created them. The stack is left running; serve.py was stopped.

To finish the live walkthrough, the user runs the seeder with an existing admin, starts `serve.py`, and follows the README table (steps 6-18), unpausing the poller only for step 18 and re-pausing it afterwards.

## Deviations from Plan

### Auto-fixed / judgement calls

**1. [Rule 2 - Missing critical] Extra allowed attributes in VUI.el**
- Added `placeholder`, `autocomplete`, `spellcheck`, `size`, `maxlength` to the allowlist: the plan requires the password input to say autocomplete off, and all are inert. `on*`, href, src, style, formaction etc. still throw (smoke test asserts it).

**2. [Rule 2 - Security] Password redacted in the log pane**
- The log shows `"password": "(hidden)"` in request bodies instead of the typed password (display only; the request itself carries it). The plan said raw bodies; tokens remain fully visible as specified.

**3. [Rule 3 - Blocking] Path literals for the contract test**
- The Visit panel builds `/cancel`, `/reschedule`, `/start`, `/complete` suffixes as literals (`path + suffix`) so `test_js_api_paths_match_gateway_route_table` can see them. Behaviour unchanged.

**4. Additions beyond the plan**
- `VUI.ctx.onChange` (context change notifications, keeps panel inputs and the context strip two-way bound) and `tests/tools/fake_dom_smoke.js` plus its pytest wrapper (runtime smoke).
- serve.py takes the request target from the raw request line so a leading `//` is not collapsed into `/`; the 413 path drains up to 16 MiB so the client can read the response.

**5. Environment, not code:** the live bring-up needed `GRAFANA_HOST_PORT=3010` (host port conflict), see above. No repo file changed for it.

## Known Stubs

None.

## Threat Flags

None beyond the plan's threat model (loopback proxy, Host/Origin guard, allowlisted statics, textContent-only rendering, env-only admin credentials in the seeder). T-bfy-01..08 mitigations are covered by tests/tools.

## Self-Check: PASSED

Files verified present (tools/validation-ui/*, tests/tools/*), commits eb57403, 9c87a86, 2fb042f exist, `git status` gate clean for services/, libs/ and dependency manifests.
