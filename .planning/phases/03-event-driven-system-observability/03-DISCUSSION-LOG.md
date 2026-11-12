# Phase 3: Event-Driven System + Observability - Discussion Log

> Audit trail only. Not consumed by downstream agents; decisions are in 03-CONTEXT.md.

**Date:** 2026-11-10
**Areas considered:** outbox relay, reminder scheduling, interim mechanisms, NFR measurement timing (plus defaults for the rest)

| Question | Options presented | User decision |
|---|---|---|
| Outbox relay | Poller (Claude's lean) vs CDC (Debezium) | **CDC**, "don't use poller" |
| Reminder scheduling | Celery scheduled tasks vs Temporal timers | **Temporal** |
| Interim mechanisms | Retire sweeper/starter vs keep as safety nets (Claude's lean: keep) | **Retire the visit sweeper** only |
| NFR measurement timing | During vs after Phase 3 | **After Phase 3**, but keep NFRs in mind while building |

The gray-area multi-select was declined by the user, who asked what Claude was unsure about; Claude listed four open questions and proposed defaults for the rest (Avro, per-service outbox, projection store, fake notification channel). Those defaults are tagged `[auto-default]` in 03-CONTEXT.md.

**Deferred:** NFR k6 profiles (after Phase 3); poller unpause; Dockerfile USER hardening.
