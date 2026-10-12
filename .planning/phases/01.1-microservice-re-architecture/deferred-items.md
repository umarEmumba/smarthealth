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
