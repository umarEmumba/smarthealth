# Phase 3: Event-Driven System + Observability - Context

**Gathered:** 2026-11-10
**Status:** Ready for planning

<domain>
## Phase Boundary

Every state change becomes a durable, replayable Kafka event through a transactional outbox with no dual-write gap; notifications, reminders, follow-ups and projections run as background work independent of user-facing flows with at-least-once delivery, retries and a dead-letter path; the five operational metrics are derived from the event stream; every service exposes Prometheus metrics (Grafana), a booking is traceable across HTTP -> Temporal -> Kafka -> Celery as one trace, and the Architecture & Decision dashboard is closed out (data/event flows plus evaluation Q&A).

Requirements: EVENT-01..05, NOTIF-01..04, ANLY-01..06, OBS-01..03, DASH-03, DASH-04 (see ROADMAP.md Phase 3 success criteria). Out of scope: capacity-aware scheduling and overbooking (SCHEDX-01/02, v2), a live payment gateway, a real SMS/email provider.

</domain>

<decisions>
## Implementation Decisions

### User-confirmed (2026-11-10)
- **D-01:** The transactional outbox is relayed to Kafka by **CDC (Debezium reading the Postgres WAL), not by a polling publisher.** Services only insert an outbox row in the same transaction as the state change; no service-side relay loop. — **Reversibility:** costly — needs `wal_level=logical`, replication slots, and a Kafka Connect service in Compose; switching to a poller later means writing a relay and removing the connector config. Note: Compose Postgres currently runs with default `wal_level` (infra/docker-compose.yml sets only `max_connections=300`), and Scheduling/Visit/Billing each have their own database, so the plan must cover the config change, a connector per outbox database (or one connector instance with several), and what the existing data volume needs.
- **D-02:** **Appointment reminders (and follow-up reminders) are scheduled with Temporal** (durable timers / workflows), not Celery ETA or beat. Delivery of the actual send still goes through Celery + RabbitMQ workers (see D-09) because ROADMAP success criterion 2 and EVENT-04 require background delivery on Celery. — **Reversibility:** reversible — only the scheduling layer changes; delivery tasks stay the same.
- **D-03:** **Retire the visit sweeper.** The visit-completion downstream trigger moves to the outbox: `visit.completed` is committed with the transition and consumed to start the downstream work, replacing the `visit-downstream-sweeper` Temporal Schedule, its run-on-start pass, and the `downstream_started_at IS NULL` marker's sweeping role (02-11, D-16). The other interim mechanisms (hold reaper, pending-starter, scheduling and visit reconcilers) are NOT retired by this decision; keep them as safety nets unless planning shows one is fully subsumed, and record the reason in the docs. — **Reversibility:** costly — removes a safety net that Phase 2 verification relied on; the replacement must be proven with a kill-between-commit-and-publish test.
- **D-04:** **NFR measurement happens after Phase 3** (k6 `sustained`, `burst`, `contention` run afterwards on the finished stack). While building, keep NFR-01..08 in mind: no extra synchronous work on the booking accept path (the outbox insert is the only addition), saga completion stays within NFR-05, and the `smoke` profile must still pass after the Phase 3 changes (it passed 2026-11-10: accept p95 21.8 ms, saga p95 0.52 s). Add Prometheus histograms on the NFR-relevant paths so Phase 3 observability can measure them later. — **Reversibility:** reversible.
- **D-13:** **Analytics start at cutover** (user decision 2026-11-10, plan-check W-3). Events are emitted only for new activity, so the five metrics count from Phase 3 go-live; patients, appointments and visits that predate it are not backfilled into the event stream (the only backfill is 03-12's completed-visit downstream backfill). State this limitation in the docs and the dashboard. The phase-goal phrase "every state change" is scoped to the D-07 event set (billing transitions, waitlist changes, visit in-progress and reconciler or hold-reaper actions are not evented). — **Reversibility:** reversible — a history backfill can be added later without changing the contract.

### Auto-defaults (`[auto-default]`, chosen by Claude, not user-confirmed; overrule by editing this file)
- **D-05 `[auto-default]`:** Event payloads are **Avro with Confluent Schema Registry** (stack docs already point at `confluent_kafka.schema_registry`); compatibility mode BACKWARD; one subject per event type. Registration happens in code/CI, not by hand. — **Reversibility:** one-way — a registered subject and consumers depend on it; changing format later needs migration.
- **D-06 `[auto-default]`:** **One outbox table per service, in that service's own database** (no cross-database writes, matching the Phase 1.1 database-per-service rule). Debezium's outbox event router publishes to topics by event type; message key = aggregate id (appointment id, visit id) so per-aggregate order is preserved. Rows carry event id (UUID, the consumer idempotency key), type, aggregate id, occurred_at, schema version and the W3C `traceparent` of the producing request.
- **D-07 `[auto-default]`:** Event set: appointment booked / cancelled / rescheduled, appointment checked_in / completed / no_show, provider schedule changed, visit completed, plus what analytics needs (patient registered). Payloads are **PHI-free: ids, types, timestamps only** (same rule as logs and audit).
- **D-08 `[auto-default]`:** **Consumers are idempotent** via a processed-event table keyed on event id, written in the same transaction as the consumer's effect; consumer groups are one per role (analytics, notification-trigger, visit-downstream), each with a dead-letter topic.
- **D-09 `[auto-default]`:** **Notification delivery:** a Kafka consumer turns notification-worthy events into Celery tasks on RabbitMQ (queues per type, manual ack, `acks_late`, exponential retry, dead-letter exchange for poison messages). The Temporal reminder workflow (D-02) enqueues the same Celery send task when its timer fires. The channel is a **fake provider** that records sends (no real SMS/email). Dedupe key = (notification type, appointment id, event id) so a retry never double-sends. The Phase 2 stub (decision D-11 in 02-CONTEXT.md, the notification activity called from `services/scheduling/workflows/`) is the single swap point; workflows must not change shape.
- **D-10 `[auto-default]`:** **Analytics projections** live in a small relational projection store (Postgres, own database) updated by an event consumer; the five metrics (total patients, appointments over time, completed visits, cancellation rate, average wait time) are served by a read-only analytics API and graphed in Grafana. Average wait time is defined as appointment start time minus check-in time (actual wait in clinic); document the definition. Projections can be rebuilt by replaying the topics.
- **D-11 `[auto-default]`:** **Tracing:** `traceparent` travels in the outbox row, Kafka headers and Celery headers so one booking is one connected trace in Jaeger (HTTP -> Temporal -> Kafka -> Celery). **Metrics:** every service exposes `/metrics`; Grafana dashboards are provisioned from files in the repo, not built by hand. **Logs:** structlog JSON with trace ids for booking, availability sync, billing, notifications and workers; PHI-free.
- **D-12 `[auto-default]`:** Close out the dashboard (DASH-03/04): event-flow diagrams for booking, cancel/reschedule, visit lifecycle, notifications and analytics, the "why CDC outbox" and "why Temporal reminders" reasoning, and an updated evaluation Q&A. Docs state only what the code does; interim mechanisms stay labelled.


### Research refinements (2026-11-10, Claude; see 03-RESEARCH.md). These adjust `[auto-default]` items only; D-01..D-04 are unchanged.
- **D-06 revised `[auto-default]`:** topics are **one per aggregate** (e.g. `smarthealth.appointment.events`, `smarthealth.visit.events`), key = aggregate id, event type carried in the Avro record name (still one Schema Registry subject per event type). Per-event-type topics would lose per-aggregate ordering.
- **D-05 detail `[auto-default]`:** services serialize Avro themselves (`confluent_kafka` `AvroSerializer`, `auto.register.schemas=False`, schemas registered at deploy/startup) into a `bytea` payload column; Debezium forwards the bytes with `BinaryDataConverter`. Debezium cannot generate controlled, evolvable Avro from a JSON payload, so it is not used that way.
- **D-01 detail:** CDC runs on `quay.io/debezium/connect:3.7.1.Final` for Kafka Connect only. `cp-kafka-connect:7.6.13` cannot be used (Java 11 cannot load Debezium 3.x). This deliberately deviates from the "same Confluent Platform line" rule; the broker and Schema Registry stay on 7.6.13. Document it. One connector per outbox database; `wal_level=logical` (Postgres restart, data survives); init and tests must check connector **task** state, not just connector state.
- **Delivery semantics (D-01/D-08):** Debezium is at-least-once on the wire. "Never lost or duplicated" means no loss plus **effect-level exactly-once through idempotent consumers**; tests must force a redelivery (e.g. kill Connect) and assert no duplicate effect.
- **D-03 detail:** the `visit-downstream-sweeper` Temporal Schedule already exists on the server and `ensure_visit_schedules` leaves existing Schedules alone, so the plan must **delete the Schedule explicitly**, keep the workflow class registered until that ships, and backfill (or document) completed visits that never started downstream work.
- **D-09 revised `[auto-default]`:** notification sending is **event-driven only** (Kafka consumer -> Celery send; Temporal reminder workflow -> same Celery send). The Phase 2 notification stub activities and workflow shapes stay unchanged (no `workflow.patched` needed in Phase 3) so nothing double-sends. Celery send dedupe via a unique `dedupe_key`; Temporal starts use deterministic workflow ids with `REJECT_DUPLICATE`.
- **D-08 refined:** the processed-event table is transactional only for database effects (analytics projections). For Temporal and Celery effects use deterministic workflow ids and the unique `dedupe_key`.
- **D-10 revised `[auto-default]`:** average wait time is defined as **check-in to visit start** (time the patient waited in clinic), not appointment start minus check-in (zero or negative for on-time arrivals). Carry check-in, visit-start and appointment-start timestamps on the events so the definition can change. Add `patient.deactivated` so total patients can decrease. Outbox rows are insert-only (no pruning); they are the replay source.
- **Celery on RabbitMQ 4.3:** needs remote control, events, mingle, gossip and heartbeat disabled and `transient_nonexcl_queues` handled; threads pool, classic queues for dev (quorum queues are the documented production path).
- **Tracing:** do not install `opentelemetry-instrumentation-confluent-kafka` (it creates a Link, not a parent, so it breaks the single-trace criterion). Extract `traceparent` from Kafka headers manually (about 30 lines). `opentelemetry-instrumentation-celery` 0.65b0 is fine.
- **Packages (blocking human-verify checkpoint before install, per repo convention 01.1-04/02-06):** `confluent-kafka[avro,schemaregistry]==2.15.0`, `fastavro==1.13.1`, `celery==5.6.3`, `kombu==5.6.2`, `opentelemetry-instrumentation-celery==0.65b0`, `prometheus-client==0.26.0`. The legitimacy check flagged all as SUS only for `too-new` and `unknown-downloads`; do not float to Celery 5.7.0.
- **Deferred by default:** the S3 audit-via-outbox follow-up; Scheduling consuming the provider schedule-change event (the poller stays paused either way).

### Claude's Discretion
- Whether the S3 follow-up (post-commit audit writes that are best effort today, 02.1-VERIFICATION known limitation 1) moves into the outbox in this phase; it is the natural fix but must not widen the booking accept path.
- Debezium and Kafka Connect image and version pins (must be compatible with the pinned `cp-kafka:7.6.13` and `cp-schema-registry:7.6.13`), connector config details, topic partition counts, retention.
- Celery queue layout and worker concurrency.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Scope and requirements
- `.planning/ROADMAP.md` (Phase 3 section) - goal and five success criteria
- `.planning/REQUIREMENTS.md` - EVENT-*, NOTIF-*, ANLY-*, OBS-*, DASH-03/04 and NFR-01..08
- `.claude/CLAUDE.md` - mandated stack, Temporal-vs-Celery split, Kafka/Schema Registry version-matching rule, KRaft, `auto-setup` caveat
- `Smart Health Requirements.md` and `SmartHealth — Execution Guidelines.md` (repo root) - original assignment

### Prior decisions that Phase 3 replaces or must respect
- `.planning/phases/02-scheduling-service-workflow/02-CONTEXT.md` - D-11 (notification stub as the single swap point), D-16 (visit downstream trigger and sweeper)
- `.planning/phases/02-scheduling-service-workflow/02-12-SUMMARY.md` - what Compose runs today; Kafka/RabbitMQ/Schema Registry are scaffolding
- `.planning/phases/02.1-week-1-2-mentor-review-remediation/02.1-CONTEXT.md` and `02.1-VERIFICATION.md` - S3 limitation, reconcilers, interim mechanisms
- `.planning/phases/01.1-microservice-re-architecture/01.1-CONTEXT.md` - database-per-service, IDs-only reads, no Kafka yet
- `.planning/codebase/CONCERNS.md` - "Kafka events are not published from any mutation" entry

### Docs to extend
- `docs/architecture.md`, `docs/architecture-dashboard.html`, `docs/erd.md`, `docs/nfr.md`, `loadtest/README.md`

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `libs/temporal.py`, `libs/temporal_policies.py`: Temporal client and retry policies shared by the workers.
- `libs/observability/otel.py`, `logging.py`: tracing setup and structured logging to extend for Kafka/Celery propagation.
- `libs/audit/writer.py`: best-effort audit writer used after commits (S3).
- Compose already runs Kafka (KRaft, `cp-kafka:7.6.13`), Schema Registry, RabbitMQ, Jaeger, Prometheus, Grafana, Temporal, Mongo, Redis, plus API and worker containers for Scheduling and Visit (healthy as of 2026-11-10).
- Loadtest assets: `loadtest/booking.js`, `loadtest/nfr.json`, `scripts/loadtest_seed.py`.

### Established Patterns
- Database-per-service; no shared tables; IDs-only cross-service reads.
- Temporal owns multi-step workflows with `workflow.patched` plus replay tests; new workflow behaviour needs a patch id and a recorded-history test.
- Deterministic workflow ids and `start_once` for idempotent starts.
- PHI never in logs, audit, traces or events (ids and types only).
- Workers reuse their API image with a command override and a heartbeat-file healthcheck.

### Integration Points
- Notification stub activity called from `services/scheduling/workflows/` (`booking.py`, `cancel.py`, `reschedule.py`, `waitlist_advance.py`) and `services/scheduling/activities/`.
- Visit completion downstream: `services/visit/downstream.py`, `services/visit/worker.py`, `services/visit/app.py` (sweeper Schedule and run-on-start).
- Scheduling state changes: `services/scheduling/repository/appointment_repo.py` (where outbox inserts join the existing transactions).
- Compose Postgres service (`infra/docker-compose.yml`, default `wal_level`).

</code_context>

<specifics>
## Specific Ideas

- The mentor weighs defensible architecture reasoning: the docs should explain why CDC over a poller (no relay code in services, ordering from the WAL, no polling load on Postgres) and the cost accepted (Kafka Connect plus logical replication to operate).
- Keep NFR-01..08 in mind throughout; measure afterwards, do not regress `smoke`.

</specifics>

<deferred>
## Deferred Ideas

- k6 `sustained`, `burst` and `contention` runs and NFR-01..08 measurement: after Phase 3 (user decision 2026-11-10).
- Temporal poller `scheduling-schedule-change-poller` is paused on purpose; decide when to unpause (user decision, 2026-11-10).
- Dockerfiles run as root (Semgrep missing-user x8): production hardening, not in this phase's scope.
- SCHEDX-01/02 (v2).

</deferred>

---

*Phase: 03-event-driven-system-observability*
*Context gathered: 2026-11-10*
