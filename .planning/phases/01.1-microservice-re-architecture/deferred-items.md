# Phase 01.1 — Deferred items

Items found or carried while executing the phase that were deliberately NOT fixed in the plan that found them. Each names an owner so it is not lost.

## Gateway request-body size cap (from 01.1-03 threat flag `unbounded-request-body`, 01.1-04)

- **What:** `services/gateway/proxy.py::forward` does `await request.body()` with no limit, so one client can make the gateway buffer an arbitrarily large body. The per-IP rate limiter bounds request count, not size.
- **Why deferred in 01.1-09:** not in plan 01.1-09's files, requirements or threat register. A correct cap needs a streaming read that counts bytes (a `Content-Length` check alone is bypassed by chunked bodies), a new setting (for example `GATEWAY_MAX_BODY_BYTES`) and tests; 01.1-10 is documentation only.
- **Suggested owner:** a small gap-closure plan in Phase 01.1 (`/gsd-plan-phase 01.1 --gaps`) or the first plan of Phase 2, before any public exposure.

## Shared Redis (recorded risk, accepted for this phase)

- Rate-limit counters (gateway) and the JWT jti denylist (Identity-written, every service reads) live in ONE Redis. A Redis outage fails the limiter open (visible warning + `gateway_ratelimit_fail_open_total`) and makes token verification fail closed with 503. Flush or eviction of that Redis would also drop revocations. Acceptable for local/assignment scope; a production split would use separate instances or ACL users per concern.

## Service-token `aud` gap (recorded risk, from 01.1-08)

- Service tokens (`type=service`, `sub=service:identity`, 60 s) carry no `aud` claim, so any service holding a verified token could in principle replay it to another internal endpoint within its 60 s life. Mitigated by: tokens are minted only by Identity, accepted only by `require_internal_caller`, never returned to clients, and internal routes are not routed by the gateway. Add an `aud` per target service before Phase 2 Scheduling starts calling Provider.

## Application containers run as root (from 01.1-08)

- Hardening item, not a regression from Phase 1; needs explicit secret uid/mode for the bind-mounted key.

## Legacy single database `smarthealth` on the Postgres volume

- Left untouched on purpose (non-destructive transition), including the stray empty `users` / `alembic_version` objects from a pre-fix migration run. Disposal is an operator decision documented by plan 01.1-10; nothing was dropped by 01.1-09.

---

# Code-review findings deferred until after Phase 2

**Decision (user, 2026-10-12):** the findings and risks from `01.1-REVIEW.md` (standard depth, 2 critical / 15 warning / 10 info) are recorded here and deliberately deferred. They are to be revisited after Phase 2 (Scheduling + Service Workflow) is implemented, most likely as a Phase 01.1 gap-closure plan (`/gsd-plan-phase 01.1 --gaps`) or an early hardening plan. Full detail, file:line references and suggested fixes are in `01.1-REVIEW.md`; this section is the index and the interim risk statement.

**Interim risk statement:** until the Critical and High-impact items below are fixed, the stack must be treated as a local/dev environment only. Do not expose it beyond localhost, do not load real patient data, and do not describe logout, PHI log-safety or the per-service database isolation as fully hardened in the README, dashboard or any mentor-facing material. The reviewer did not run the suite or a live stack; CR-02's trigger is reasoned, not reproduced against Postgres.

## Critical (fix first when revisited)

| ID | Finding | Where |
|----|---------|-------|
| CR-01 | Logout revokes only the access-token jti; refresh tokens are never checked against the denylist or rotated, so a 7-day refresh token keeps minting access tokens after logout. `docs/architecture.md` calls this "real logout" — that wording is currently inaccurate. | `services/identity/api/auth.py:384-454` |
| CR-02 | Patient PHI (name, DOB, phone, address) can reach stdout logs and trace exception events: the shared engine lacks `hide_parameters=True`, and the Patient router re-raises DB errors. | `libs/db/postgres.py:50`, `services/patient/api/patients.py:207-212` |

## Warnings

| ID | Finding | Priority note |
|----|---------|---------------|
| WR-01 | Gateway drops `Host`, so redirects return `Location: http://organization:8000/...` (internal hostname leak; reproduced). | Medium |
| WR-02 | Service tokens skip the internal role allowlists and carry no `aud`/scope (docs mention `aud` only). | High — fix before Phase 2 Scheduling calls Provider |
| WR-03 | `exp` not required in token verification (`libs/auth/verification.py:100`); a token without `exp` never expires. | High |
| WR-04 | Audit write happens after DB commit and is not atomic with it; Mongo failure = 500 for an already-committed mutation with no audit. | Medium |
| WR-05 | Specialty get-or-create can deadlock under concurrent provider creation (insert order). | Medium |
| WR-06 | Specialty names not validated (length/blank/normalization). | Low |
| WR-07 | `provider_slots` exclusion constraint bypassed by zero-length slots; no `CHECK (end_time > start_time)`. | High — Phase 2 depends on this constraint |
| WR-08 | Infra ports (Postgres, Mongo, Redis, etc.) published on all interfaces with default credentials / no auth; per-service DB isolation is moot from the host. | High |
| WR-09 | `env_file: .env` injects the whole `.env` into every container (contents not checked). | Medium |
| WR-10 | Rate limiter fails open for login/register/refresh; no per-account login throttle. | Medium |
| WR-11 | Gateway buffers whole request bodies before auth (same item as the body-size cap above). | Medium |
| WR-12 | Prometheus metrics are per gunicorn worker (no multiprocess mode); inconsistent scrapes. | Low — matters for Phase 3 observability |
| WR-13 | Clinic `timezone` documented as IANA but never validated. | Low — matters for Phase 2 scheduling |
| WR-14 | Per-request Redis client has no socket timeouts; hung Redis hangs authenticated requests. | Medium |
| WR-15 | Containers run as root and keep the compiler toolchain (same as the root-containers item above). | Low |

## Info (IN-01 .. IN-10)

IN-01 account links don't check linked account's role/clinic; IN-02 registration email enumeration and case-sensitive uniqueness; IN-03 `bootstrap_admin.py` has no password policy / no clean failure if audit write fails after commit; IN-04 test fixtures `flushdb()` the dev Redis and leave known-password admins; IN-05 stale docstrings pointing at deleted files (incl. `services/provider/migrations/env.py` citing `PATIENT_DATABASE_URL`); IN-06 availability templates have no duration upper bound / overlap guard (PPL-03 assumption); IN-07 model/migration index mismatch on `users.clinic_id`; IN-08 departments creatable under an inactive clinic; IN-09 PATCH cannot clear optional fields; IN-10 `clinic_scope_permits` fails open on a missing clinic claim (Patient is fail-closed).

## Also noted (outside the review)

- Compose Temporal image is `auto-setup:1.29.7` while `.claude/CLAUDE.md` recommends 1.31.x (1.31.2 does not exist on Docker Hub; Phase 1 substitution). The project CLAUDE.md tag table still needs correcting for `redis` and `temporalio/auto-setup`.

## Verification status carried with this deferral

`01.1-VERIFICATION.md` is `human_needed` (5/5 success criteria verified, no gaps). Seven human checks remain open: real-socket outage behaviour, cold start from a clean volume, Jaeger UI view, dashboard opened from disk, manual `.env.example` / `infra/.env.example` edits, ORG-03/PPL-03/PPL-04 assumptions, and `bootstrap_admin.py` creation branch. `verification.status` currently reports `stale` even though the recorded digest matches the current files; cause not yet confirmed (suspect the date-only `verified:` timestamp). Phase 1.1 has therefore NOT been marked complete.
