---
phase: 03-event-driven-system-observability
plan: 08
type: execute
wave: 4
depends_on: ["03-01", "03-05"]
files_modified:
  - services/patient/migrations/versions/patient_0002_outbox.py
  - infra/connect/connectors/patient-outbox.json
  - services/patient/events.py
  - services/patient/repository/patient_repo.py
  - services/patient/app.py
  - services/provider/migrations/versions/provider_0003_outbox.py
  - infra/connect/connectors/provider-outbox.json
  - services/provider/events.py
  - services/provider/repository/provider_repo.py
  - services/provider/app.py
  - tests/patient/test_outbox.py
  - tests/provider/test_outbox.py
  - tests/e2e/test_phase3_registry_events.py
autonomous: true
requirements: [EVENT-01, EVENT-02, ANLY-01]

estimate:
  tokens: 85000
  raw_tokens: 85000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "patient_db and provider_db each have an insert-only outbox table, a dbz_outbox publication and their own connector (patient-outbox, provider-outbox) whose task is RUNNING; the debezium role reads only their outbox tables, never the patients table (D-01, T-3-58)"
    - "Registering a patient commits a patient.registered outbox row in the same transaction as the patient row; deactivating commits patient.deactivated; both carry ids, clinic and timestamps only, never name, birth date, phone or address (D-07, ANLY-01)"
    - "Every provider schedule change already recorded in schedule_change_events also commits a provider.schedule_changed outbox row in that same transaction (EVENT-01: provider schedule change event)"
    - "The Phase 2.1 pull feed (schedule_change_events with its internal endpoint and acknowledgement) keeps working unchanged; the paused poller stays paused"
    - "A repeat (deactivating an inactive patient) writes no event; a rolled-back change writes none"
  artifacts:
    - path: services/patient/events.py
      provides: "PATIENT_EVENT_TYPES, emit_patient_registered, emit_patient_deactivated"
      contains: "enqueue_event"
    - path: services/provider/events.py
      provides: "PROVIDER_EVENT_TYPES, emit_schedule_changed"
      contains: "enqueue_event"
    - path: tests/patient/test_outbox.py
      provides: "atomicity and PHI-absence tests for both patient events"
  key_links:
    - from: services/patient/repository/patient_repo.py
      to: services/patient/events.py
      via: "create_patient flushes, emits, then commits; deactivate_patient emits before its commit"
      pattern: "emit_patient_"
    - from: services/provider/repository/provider_repo.py
      to: services/provider/events.py
      via: "record_schedule_change emits with discriminator = schedule_change_events.id"
      pattern: "emit_schedule_changed"
    - from: infra/connect/connectors/patient-outbox.json
      to: services/patient/migrations/versions/patient_0002_outbox.py
      via: "publication dbz_outbox and table public.outbox created by create_outbox, read by slot dbz_patient"
      pattern: "dbz_patient"
---

<objective>
Make Patient and Provider producers end to end: each service gets its own outbox table and Debezium connector (with the helper and registration script from plan 03-05), and `patient.registered`, `patient.deactivated` and `provider.schedule_changed` are committed through that outbox with the change that caused them.

Purpose: EVENT-01 names the provider schedule-change event; ANLY-01 (total patients) needs registration and deactivation events so the projection in plan 03-10 never reads patient_db. Patient is the only PHI holder, so the PHI-absence proof and the connector's outbox-only scope matter most here.

Output: two outbox migrations, two connector files, two small `events.py` modules, emission in three repository functions, registry warm-up in both apps, integration tests, and e2e tests reading the patient and provider events off Kafka.

Decisions honoured: D-06 revised (aggregates `patient` and `provider`, key = patient id / provider id), D-07 (PHI-free), D-10 revised (`patient.deactivated` so total patients can decrease). Research Open Question 3 resolved as a planner choice confirmed in plan 03-01 Task 2: the event is published only; Scheduling does not consume it in this phase and the `scheduling-schedule-change-poller` Schedule stays paused (user decision 2026-11-10).

Live stack rules: rebuild only these two services with `--no-deps` (`docker compose -f infra/docker-compose.yml up -d --build --no-deps patient` in Task 1, `... provider` in Task 3); each image runs `alembic upgrade head` at start, which creates the outbox table, publication and guarded debezium grant on the existing volume. Then register only this plan's connector by name: `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh patient-outbox` (and `provider-outbox`); the script waits for the task to be RUNNING and does not touch other connectors. No Compose edit in this plan (`connect-init` already depends on `patient` and `provider`). Never touch `tjo-*` containers (record their status before and after). Wave 4 coupling: plans 03-07, 03-09 and 03-10 run alongside; none stops, kills or recreates `connect`, `gateway`, `postgres`, `kafka` or another plan's service; if an e2e seed step fails because a sibling's service was restarting, re-run that test once after `scripts/compose_healthcheck.sh` passes. CDC checks select this plan's connectors only (`-k "patient-outbox or provider-outbox"`) because plan 03-09 may add its connector file in the same wave.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@services/patient/repository/patient_repo.py
@services/provider/repository/provider_repo.py
@services/patient/app.py
@services/provider/app.py
@libs/events/outbox.py
@.planning/phases/03-event-driven-system-observability/03-05-SUMMARY.md
@libs/events/migration.py
@infra/connect/connectors/scheduling-outbox.json
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| Migrations | Alembic | `patient_0002` (down `patient_0001`), `provider_0003` (down `provider_0002`); each calls `create_outbox` / `drop_outbox` |
| Connectors | Connect | `patient-outbox` (`patient_db`, `topic.prefix=smarthealth-patient`, slot `dbz_patient`), `provider-outbox` (`provider_db`, `smarthealth-provider`, `dbz_provider`); otherwise identical to `scheduling-outbox.json` |
| `PATIENT_EVENT_TYPES`, `emit_patient_registered(db, patient)`, `emit_patient_deactivated(db, patient)` | module | `services/patient/events.py`; discriminators `registered` and the deactivation timestamp in ISO form |
| `PROVIDER_EVENT_TYPES`, `emit_schedule_changed(db, change_event)` | module | `services/provider/events.py`; discriminator = `schedule_change_events.id` |
| hooks | code | `create_patient` (flush, emit, commit), `deactivate_patient` (emit before commit, only when it actually deactivates), `record_schedule_change` (emit after its flush) |
| warm-up | code | `await warm_up(...)` in both app lifespans |
| tests | tests | `tests/patient/test_outbox.py`, `tests/provider/test_outbox.py`, `tests/e2e/test_phase3_registry_events.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — registering a patient through the gateway publishes PHI-free patient.registered on Kafka through the new patient outbox and connector</name>
  <reversibility rating="costly">D-01: a replication slot on the only PHI database; dropping it later means deleting the connector and the slot (deleting a connector does not drop its slot).</reversibility>
  <files>services/patient/migrations/versions/patient_0002_outbox.py, infra/connect/connectors/patient-outbox.json, services/patient/events.py, services/patient/repository/patient_repo.py, services/patient/app.py, tests/e2e/test_phase3_registry_events.py</files>
  <behavior>
    - After the rebuild and registration, the `patient-outbox` connector task is RUNNING, slot `dbz_patient` is active, and the debezium role gets `permission denied` on `patients` (plan 03-05's parametrized checks for `patient-outbox`)
    - POST /api/patients through the gateway (front desk or admin of a fresh clinic) returns 201 and, within 60 s, `smarthealth.patient.events` carries one message with key = patient id and `eventType=patient.registered`
    - The decoded value has `patient_id`, `clinic_id`, `registered_at`; the raw message value bytes contain none of the submitted full name, date of birth, phone or address strings
  </behavior>
  <read_first>
    - libs/events/migration.py, services/scheduling/migrations/versions/scheduling_0006_outbox.py, infra/connect/connectors/scheduling-outbox.json (plan 03-05: copy the shape)
    - services/patient/migrations/versions/patient_0001_patients.py (revision id `patient_0001`)
    - services/patient/repository/patient_repo.py (`create_patient` adds then commits; the id exists only after flush)
    - services/patient/repository/models.py (patient id, clinic_id, created_at, deactivated_at)
    - services/patient/app.py (lifespan)
    - libs/events/outbox.py, libs/events/registry.py
    - tests/e2e/phase2_stack.py and tests/e2e/phase3_stack.py (seeding through the gateway, `consume_matching`)
  </read_first>
  <action>
    Outbox and connector: `patient_0002_outbox.py` (revision `patient_0002`, down `patient_0001`) calling `create_outbox` / `drop_outbox`; `infra/connect/connectors/patient-outbox.json` identical to the scheduling file except `database.dbname=patient_db`, `topic.prefix=smarthealth-patient`, `slot.name=dbz_patient` (keep `table.include.list=public.outbox`: the connector must never capture the patients table).

    events.py: `PATIENT_EVENT_TYPES = ("patient.registered", "patient.deactivated")`; `emit_patient_registered(db, patient)` -> `enqueue_event(db, event_type="patient.registered", aggregate_id=str(patient.id), payload={patient_id, clinic_id, registered_at=created_at}, discriminator="registered")`. Read only `id`, `clinic_id`, timestamps from the ORM object; never touch the PHI attributes.

    create_patient: after `db.add(patient)`, `await db.flush()` (assigns the id and server defaults), then `emit_patient_registered`, then the existing commit and refresh. Lifespan: `await warm_up(PATIENT_EVENT_TYPES)`.

    e2e test (marker e2e): register a patient with distinctive synthetic PHI strings in a fresh clinic, consume the event, assert the behaviors (raw `msg.value()` bytes searched for each PHI string).

    Apply: rebuild `patient` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps patient` (Alembic creates the outbox at start), wait healthy, then `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh patient-outbox`, then run the tests.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "patient-outbox" -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_registry_events.py -m e2e -k registered -x -q</automated>
    <fails_when>Either pytest run exits non-zero, the patient-outbox task is not RUNNING, the debezium role can read `patients`, the event does not arrive within 60 s, or any submitted PHI string is found in the message bytes.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/alembic -c services/patient/migrations/alembic.ini upgrade head` exits 0 on the host
    - the three parametrized CDC checks for `patient-outbox` pass
    - the tracer test passes on the rebuilt stack
    - `grep -c "emit_patient_registered" services/patient/repository/patient_repo.py` is 1
    - the e2e assertion searches the raw Kafka message bytes for each of the four submitted PHI strings and finds none
  </acceptance_criteria>
  <done>A registered patient becomes a PHI-free event on Kafka through the outbox.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: patient.deactivated, atomicity and the PHI proof at the table</name>
  <files>services/patient/events.py, services/patient/repository/patient_repo.py, tests/patient/test_outbox.py</files>
  <behavior>
    - `create_patient` writes exactly one `patient.registered` outbox row, committed with the patient; a failure before the commit (simulated by raising from a patched `emit` after the flush) leaves neither row
    - `deactivate_patient` on an active patient writes one `patient.deactivated` row with `deactivated_at`; on an already-inactive patient writes nothing; out of clinic scope writes nothing
    - For both events, the stored payload bytes contain none of the seeded full name, date of birth, phone or address
  </behavior>
  <read_first>
    - services/patient/repository/patient_repo.py (`deactivate_patient`: commit only when `is_active` flips)
    - tests/patient/test_patients.py and tests/patient/test_patient_tracer.py (live-DB fixtures and seeding)
  </read_first>
  <action>
    `emit_patient_deactivated(db, patient)` with payload `{patient_id, clinic_id, deactivated_at}` and discriminator `patient.deactivated_at.isoformat()`; call it inside the `if patient.is_active:` branch after setting the fields and before the commit. Tests (marker `integration`, live patient_db, unique clinic per test) for every behavior, reading `outbox` rows by `aggregateid`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/patient -x -q</automated>
    <fails_when>pytest exits non-zero, a repeat deactivation writes a row, or PHI bytes are found in a payload.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/patient/test_outbox.py -q` exits 0 with at least 5 passed
    - `.venv/bin/python -m pytest tests/patient -q` exits 0
  </acceptance_criteria>
  <done>Both patient events are atomic with their change and provably PHI-free.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Provider outbox and connector, and provider.schedule_changed alongside the existing schedule-change feed</name>
  <files>services/provider/migrations/versions/provider_0003_outbox.py, infra/connect/connectors/provider-outbox.json, services/provider/events.py, services/provider/repository/provider_repo.py, services/provider/app.py, tests/provider/test_outbox.py, tests/e2e/test_phase3_registry_events.py</files>
  <behavior>
    - After the rebuild and registration, the `provider-outbox` connector task is RUNNING, slot `dbz_provider` is active, and the debezium role gets `permission denied` on `providers`
    - Each caller of `record_schedule_change` (template create, update, deactivate; provider deactivate) results in one `schedule_change_events` row AND one `provider.schedule_changed` outbox row with `change_event_id` = that row's id, `change_type`, `provider_id`, optional `template_id`, committed together; a rollback leaves neither
    - The internal feed (`/internal/schedule-changes` and its acknowledgement) returns and acknowledges the same rows as before (existing tests stay green)
    - e2e: updating an availability template through the gateway publishes one `provider.schedule_changed` on `smarthealth.provider.events` keyed by provider id
  </behavior>
  <read_first>
    - services/provider/repository/provider_repo.py (`record_schedule_change` flushes only; its callers commit)
    - services/provider/api/availability.py and services/provider/api/providers.py (callers)
    - tests/provider/test_schedule_change_feed.py (feed behaviour that must not change)
    - services/provider/migrations/versions/provider_0002_schedule_change_events.py (revision id `provider_0002`) and infra/connect/connectors/patient-outbox.json (Task 1)
  </read_first>
  <action>
    Outbox and connector: `provider_0003_outbox.py` (revision `provider_0003`, down `provider_0002`) calling `create_outbox` / `drop_outbox`; `infra/connect/connectors/provider-outbox.json` identical to the scheduling file except `database.dbname=provider_db`, `topic.prefix=smarthealth-provider`, `slot.name=dbz_provider`.

    events.py: `PROVIDER_EVENT_TYPES = ("provider.schedule_changed",)`, `emit_schedule_changed(db, event)` with payload `{provider_id, change_event_id=event.id, change_type, template_id, changed_at=event.created_at}` and discriminator `str(event.id)`. In `record_schedule_change`, call it after the existing flush (the row's id and `created_at` are set by then; refresh `created_at` if the server default is not loaded). Lifespan: `await warm_up(PROVIDER_EVENT_TYPES)`. Tests (marker `integration`) for the behaviors; add the e2e case to `tests/e2e/test_phase3_registry_events.py`.

    Apply: rebuild `provider` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps provider`, wait healthy, then `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh provider-outbox`, then run the tests.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/provider -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "patient-outbox or provider-outbox" -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_registry_events.py -m e2e -x -q</automated>
    <fails_when>Any pytest run exits non-zero, the feed tests change behaviour, a patient or provider connector task is not RUNNING, the debezium role can read a non-outbox table, or no provider.schedule_changed message arrives within 60 s.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/alembic -c services/provider/migrations/alembic.ini upgrade head` exits 0 on the host
    - `.venv/bin/python -m pytest tests/provider -q` exits 0 (including `tests/provider/test_schedule_change_feed.py` unchanged)
    - `.venv/bin/python -m pytest tests/provider/test_outbox.py -q` reports at least 4 passed
    - the e2e file passes with at least 2 passed
  </acceptance_criteria>
  <done>Provider schedule changes are published as events in the same transaction as the existing feed rows.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Patient service (only PHI holder) -> outbox -> Kafka | the one place PHI could leak into the event stream |
| patient_db WAL -> Debezium | a replication slot on the only PHI database |
| Provider service -> outbox -> Kafka | schedule changes become replayable events |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-24 | Information Disclosure | patient event payloads | high | mitigate | Builders read only id, clinic and timestamps; static grep and two tests (table bytes and Kafka bytes) prove the submitted PHI strings are absent; schemas have no PHI fields (plan 03-03 scan) |
| T-3-58 | Information Disclosure | `patient-outbox` connector on patient_db | high | mitigate | `table.include.list=public.outbox` and a publication on `outbox` only (static test from plan 03-05 checks every connector file); the debezium role has SELECT on `outbox` only and the parametrized e2e check proves `permission denied` on `patients`; no Connect host port |
| T-3-25 | Tampering | duplicate patient or schedule-change events | low | mitigate | Deterministic discriminators (`registered`, deactivation time, change-row id) with `ON CONFLICT (id) DO NOTHING`; only state-changing branches emit |
| T-3-26 | Repudiation | divergence between the feed table and the event | low | mitigate | Both rows are written in one transaction and the event carries `change_event_id`, so every event maps to exactly one feed row |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/patient tests/provider tests/infra/test_cdc_config.py -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_registry_events.py -m e2e -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "patient-outbox or provider-outbox" -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/patient services/provider`; record status in the SUMMARY
</verification>

<success_criteria>
- Patient registration/deactivation and provider schedule changes are published atomically and PHI-free
- The Phase 2.1 feed and paused poller are unchanged
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-08-SUMMARY.md` when done
</output>
