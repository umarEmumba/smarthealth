# Phase 2: Scheduling + Service Workflow - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-10-12
**Phase:** 2-Scheduling + Service Workflow
**Areas discussed:** Slot supply & holds, Service boundaries & Temporal topology, Booking contract & failure behavior, Waitlist/cancel/reschedule & visit rules

---

**How this session ran:** The gray-area question went unanswered twice (the interactive prompt was dismissed, then a plain-text list was offered). The user then replied "do what is best practice". Claude treated that as delegation: all four areas were selected and every question was resolved with the recommended default (the `--auto` convention). **No option below was chosen by the user**; each "Selected" mark is Claude's recommendation.

## Slot supply & holds

| Option | Description | Selected |
|--------|-------------|----------|
| Rolling-horizon generation on demand (28 days), idempotent | Generated from templates over internal REST; admin/front-desk trigger plus on-demand extension | ✓ |
| Pre-generate a fixed long horizon at template creation | Simple, but creates many rows and goes stale when templates change | |
| Virtual slots computed at query time | No rows to hold, but the exclusion constraint and holds need real rows | |

**Choice:** Rolling horizon, on-demand (recommended default)
**Notes:** Double-booking guarded by a partial unique index on active appointments (slot_id), in addition to the existing slot-overlap exclusion constraint. Holds use a 10-minute `held_until` with a reaper; alternatives were no hold (race window between reserve and confirm) and a Redis lock (not durable, a failure window the project's own research rules out as the only guard).

---

## Service boundaries & Temporal topology

| Option | Description | Selected |
|--------|-------------|----------|
| Scheduling (appointments+slots+waitlist), separate Visit and Billing services; worker per service | Follows the roadmap wording and the 01.1 finer-boundary preference; reschedule stays one local transaction | ✓ |
| Everything inside Scheduling | Fewest moving parts, but contradicts the roadmap's "own services" line | |
| Appointments in their own service, apart from slots | Cleaner naming, but turns reschedule into a distributed transaction | |

**Choice:** Three services, workflow with the aggregate owner (recommended default)
**Notes:** Activities use service tokens, never the user JWT; workflow histories carry IDs only; deterministic workflow IDs. A single shared worker was considered and rejected as it couples deployments.

---

## Booking contract & failure behavior

| Option | Description | Selected |
|--------|-------------|----------|
| 202 Accepted + poll, mandatory Idempotency-Key | Matches an async saga; confirmed only after the saga completes | ✓ |
| Synchronous wait for saga result | Simpler client, but ties HTTP latency to the saga and its retries | |
| Optional sync wait via query flag | Two code paths to test | |

**Choice:** 202 + poll (recommended default)
**Notes:** Idempotency keys in Postgres (durable), 24h expiry, same key with different body returns 422. Billing pre-check is a deterministic rule-based simulation (no randomness) so failure tests are reproducible. Notifications are stubbed behind one function and are best-effort (never fail a booking). Compensation is forward transitions with audit, never deletes.

---

## Waitlist, cancel/reschedule & visit rules

| Option | Description | Selected |
|--------|-------------|----------|
| FIFO by provider+window, auto-book via the booking saga | Reuses idempotency and compensation; no offer-expiry state machine | ✓ |
| Offer/accept with expiry | Better patient control, adds a second state machine and timers | |
| Department/specialty-wide with priority tiers | Richer matching, beyond the requirement | |

**Choice:** FIFO auto-book per provider (recommended default)
**Notes:** Cancel has a 24h refund cutoff (configurable, waivable by staff). Reschedule is reserve-then-swap in one local transaction, never cancel-then-rebook. Visit states are checked_in → in_progress → completed with idempotent repeats (200) and 409 for skipped states. Completion fan-out uses a commit-time marker plus a sweeper, as an interim until the Phase 3 outbox. Visit status is polled, not pushed.

---

## Claude's Discretion

All decisions in CONTEXT.md are delegated defaults. Planner-level choices (table names, retry values, schedule intervals, API path shapes) are explicitly left to the planner.

## Deferred Ideas

- Waitlist offer/accept flow, wider matching and priority tiers
- Synchronous booking mode, WebSocket/SSE visit status
- Scheduled slot generation (Phase 3)
- Outbox/Kafka/Celery replacing the sweeper and notification stubs (Phase 3)
- SCHEDX-01/02 (v2)
- Temporal production topology (dashboard note only)
