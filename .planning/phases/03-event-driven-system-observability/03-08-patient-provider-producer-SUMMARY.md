---
phase: 03-event-driven-system-observability
plan: 08
subsystem: patient-provider
tags: [transactional-outbox, avro, debezium, kafka, phi-free-events, schedule-changes]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-01 enqueue_event / warm_up, 03-03 PatientRegistered, PatientDeactivated and ProviderScheduleChanged Avro schemas, 03-05 create_outbox migration helper, connector registration script and CDC tests"
provides:
  - services/patient/events.py (PATIENT_EVENT_TYPES, emit_patient_registered, emit_patient_deactivated)
  - services/provider/events.py (PROVIDER_EVENT_TYPES, emit_schedule_changed)
  - patient_0002 and provider_0003 outbox migrations (table, dbz_outbox publication, guarded debezium grant)
  - patient-outbox and provider-outbox Debezium connectors (slots dbz_patient, dbz_provider), both RUNNING
  - outbox emission in create_patient, deactivate_patient and record_schedule_change, same transaction as the change
  - schema-id warm-up in the Patient and Provider lifespans
  - 11 patient and 10 provider live-DB tests, 2 gateway e2e tests (Kafka reads)
affects: [03-09, 03-10, 03-11, 03-13, 03-14, 03-17]
tech-stack:
  added: []
  patterns:
    - "Hook at the repository function that commits: flush, emit, commit; emit only in the branch that actually changes state"
    - "Server-default timestamps are fetched with a targeted refresh only when the flush left them unloaded (created_at), so events never trigger an async lazy load"
    - "Provider event keeps the feed row id as change_event_id and as the uuid5 discriminator: one event per feed row"
    - "PHI proof twice: payload bytes in the outbox table, and raw Kafka message bytes plus headers"
key-files:
  created:
    - services/patient/events.py
    - services/patient/migrations/versions/patient_0002_outbox.py
    - services/provider/events.py
    - services/provider/migrations/versions/provider_0003_outbox.py
    - infra/connect/connectors/patient-outbox.json
    - infra/connect/connectors/provider-outbox.json
    - tests/patient/test_outbox.py
    - tests/provider/test_outbox.py
    - tests/e2e/test_phase3_registry_events.py
  modified:
    - services/patient/repository/patient_repo.py
    - services/patient/app.py
    - services/provider/repository/provider_repo.py
    - services/provider/app.py
key-decisions:
  - "patient.deactivated discriminator is the deactivation timestamp in ISO form (microsecond precision from the database round trip); the Avro field is timestamp-millis, so the decoded value is the millisecond truncation of the stored time while the outbox occurred_at column keeps full precision"
  - "Only create_patient and the deactivating branch of deactivate_patient emit; update_patient (profile edits) does not (D-07: only registration and deactivation are patient events)"
  - "record_schedule_change refreshes created_at only when unloaded, then emits; its three callers are unchanged and still own the commit"
requirements-completed: []
status: complete
duration: about 35 min wall time (two image builds, registration, about 5 min of regression runs)
completed: 2026-11-11
actuals:
  tokens: 14400
  tasks: 3
  commits: 3
plan_head_before: bd233fd2bbc7f74634577e39c167c43ca944022a
plan_head_after: 392fea8826035e3e9956f36f6c84eed64fcc8f1a
---

# Phase 3 Plan 08: Patient and Provider Producers Summary

**Patient and Provider are now event producers: `patient.registered`, `patient.deactivated` and `provider.schedule_changed` are committed through each service's own outbox in the same transaction as the change, relayed by their own Debezium connectors (patient-outbox, provider-outbox, both RUNNING, outbox table only), and proven PHI-free at the table and in the raw Kafka bytes.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (EVENT-01, EVENT-02, ANLY-01 stay for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 650c2ab | patient_0002, patient-outbox.json, patient `events.py` (registered only), `create_patient` flush/emit/commit, Patient lifespan warm-up, e2e file with the PHI byte test |
| 2 | c63820c | `emit_patient_deactivated`, hook in the deactivating branch, `tests/patient/test_outbox.py` (11 tests) |
| 3 | 392fea8 | provider_0003, provider-outbox.json, provider `events.py`, hook in `record_schedule_change`, Provider lifespan warm-up, `tests/provider/test_outbox.py` (10 tests), provider e2e case |

## What was built

- **Outbox and connectors.** `patient_0002` and `provider_0003` call `create_outbox` / `drop_outbox`. `patient-outbox.json` and `provider-outbox.json` differ from `scheduling-outbox.json` only in `database.dbname`, `topic.prefix` and `slot.name`; `table.include.list` stays `public.outbox`, so neither connector can capture `patients` or `providers`.
- **Patient events.** `emit_patient_registered` (discriminator `registered`, payload `patient_id`, `clinic_id`, `registered_at`) and `emit_patient_deactivated` (discriminator = the deactivation time in ISO form, payload adds `deactivated_at`). They read `id`, `clinic_id` and timestamps only; `full_name`, `date_of_birth`, `phone`, `address` and `user_id` are never touched.
- **Provider event.** `emit_schedule_changed(db, event)` with `provider_id`, `change_event_id` (the `schedule_change_events` row id), `change_type`, optional `template_id`, `changed_at` (the feed row's `created_at`); discriminator = the feed row id. Called from `record_schedule_change` after its flush, so the template update, template deactivation and provider deactivation callers all emit with no change of their own. The internal feed, its acknowledgement and the paused poller are untouched.
- **Warm-up.** `await warm_up(PATIENT_EVENT_TYPES)` and `await warm_up(PROVIDER_EVENT_TYPES)` in the two lifespans.

## Test results (honest)

- `tests/patient/test_outbox.py`: **11 passed** (plan needed at least 5). One row per change with uuid5 ids, patient row visible in the same commit, repeat and out-of-scope deactivation write nothing, failure before the commit (patched emit raising after the real emit) leaves neither the patient nor the event, same for deactivation, double emission is one row, profile edits emit nothing, payload bytes and decoded field set are PHI-free for both events (full name, DOB, phone, address and distinctive fragments).
- `tests/provider/test_outbox.py`: **10 passed** (plan needed at least 4). Each caller gives one feed row plus one event with `change_event_id` = the feed row id (one-to-one check), the uuid5 id and `occurred_at` match, no-op PATCH and repeat deactivations write neither, rollback leaves neither, failure after the emit leaves neither and the template unchanged, the pull feed still lists and acknowledges the same rows (`delivered`, then `already`) and acknowledging does not touch the outbox, payload has only ids, type and timestamps.
- `tests/e2e/test_phase3_registry_events.py`: **2 passed** (tracer and provider case; Kafka reads from a fresh consumer group, distinct `id` headers counted). The tracer registers a patient with four distinctive synthetic PHI strings and searches the raw message value bytes and every header for each string and for fragments of them: none found.
- CDC checks for this plan's connectors: `tests/e2e/test_phase3_outbox_cdc.py -k "patient-outbox or provider-outbox"`: **6 passed** (connector task RUNNING, slot and publication on `outbox` only, debezium role gets `permission denied` on the business tables), so with the registry file **8 passed** in 12.8 s.
- Static `tests/infra`: passes, it covers every connector JSON present (19 passed for `test_cdc_config.py`).
- Regression: `tests/patient tests/libs/test_service_isolation.py` **91 passed**; `tests/provider tests/infra tests/libs/test_service_isolation.py` **135 passed, 1 failed**. The failure is the known unrelated `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` (the identical specialty-ordering assertion: `Tracer Internal Medicine`/`Tracer Cardiology` order); not touched, not made worse. `tests/provider/test_schedule_change_feed.py` is unchanged and passes. The full non-e2e suite was not run (the orchestrator said it is not required).
- Semgrep (`--config p/python --metrics=off`, `--config auto` is refused with metrics off as in earlier Phase 3 plans) over the 6 changed service modules and the 2 migrations: 151 rules, **0 findings**. The tests were not scanned.

## Live-stack operations record

- Host: `alembic upgrade head` for patient (`patient_0001 -> patient_0002`) and provider (`provider_0002 -> provider_0003`) ran against the live databases (the acceptance criterion); the container start's `alembic upgrade head` was then a no-op.
- Compose: `up -d --build --no-deps patient` (Task 1), the same again after Task 2 so the running image has `patient.deactivated`, and `up -d --build --no-deps provider` (Task 3). Nothing else was rebuilt, stopped, killed or recreated; `gateway`, `connect`, `postgres`, `kafka` were not touched (gateway rate budgets unchanged). No volume was touched, no `down -v`. `scripts/compose_healthcheck.sh` printed OK after every recreate.
- Connectors registered by name: `register-connectors.sh patient-outbox` then `provider-outbox`; both reported connector and all tasks RUNNING. Slots `dbz_patient` and `dbz_provider` are active (with `dbz_scheduling`).
- Live data left behind by tests: `patient_db.outbox` holds 159 `patient.registered` and 15 `patient.deactivated` rows and `provider_db.outbox` 50 `provider.schedule_changed` rows (from the regression, Phase 2/3 flows and these tests, throwaway synthetic ids). The outbox is insert-only by design (accepted veto item 5), so they remain and flow to downstream consumers as ordinary events for unknown clinics. The new integration tests delete their own patient/provider/template/feed rows at teardown (hygiene only).
- `tjo-*` before and after: `tjo-api Up (unhealthy)` and `tjo-postgres Up (healthy)`, no restart (25 h before, 26 h after; `tjo-api` was already unhealthy). No `.env*` or `infra/secrets/*` content was read or printed.

## Deviations from Plan

1. **[Plan wording] Template create is not a schedule change.** The Task 3 behavior text lists "template create, update, deactivate; provider deactivate" as callers of `record_schedule_change`. In the existing code (Phase 2.1) only a schedule-affecting template update, template deactivation and provider deactivation record a change; creating a template records none, and I did not change that (the feed must stay unchanged). The tests assert exactly this (creating a template writes no feed row and no event).
2. **[Plan wording] Acceptance grep.** `grep -c "emit_patient_registered" services/patient/repository/patient_repo.py` prints 2, not 1: the import line and the single call. There is one call site.
3. **[Rule 1 - Bug in my own tests, fixed before commit]** The first version of two patient tests compared Avro `timestamp-millis` values to the microsecond database timestamps; the decoded value is the millisecond truncation. The tests now compare the truncation and check that the outbox `occurred_at` column keeps the full value.
4. **[Plan wording] Second patient rebuild.** The plan names one patient rebuild (Task 1). After Task 2 added `patient.deactivated`, I rebuilt `patient` once more with the same `--no-deps` command so the running service matches the committed code (no other service, no registration needed).
5. **Task 2 split.** `events.py` and `create_patient`/`deactivate_patient` were first written complete, then reduced to the registered-only slice for the Task 1 commit and restored for Task 2, so the three commits stay atomic and Task 1's tracer ran on exactly what it commits.
6. **Semgrep ruleset** `p/python` with `--metrics=off` instead of `--config auto` (see above).

No auth gates, no architectural changes, no package installs.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-3-24 (PHI): builders read ids and timestamps only, the Avro schemas have no PHI fields, and three tests prove absence (decoded field set, table bytes, raw Kafka bytes and headers). T-3-58: `table.include.list=public.outbox`, publication on `outbox` only, the parametrized e2e checks prove `permission denied` on the business tables for both connectors, no Connect host port. T-3-25: deterministic discriminators, only state-changing branches emit, double-emission test. T-3-26: both rows in one transaction, `change_event_id` maps each event to one feed row (tested one-to-one).

## Notes for later plans

- `smarthealth.patient.events` carries `patient.registered` and `patient.deactivated` (key = patient id); `smarthealth.provider.events` carries `provider.schedule_changed` (key = provider id). Plan 03-10 can count total patients from these only; per D-13 the count starts at go-live, but the live outbox already holds roughly 160 registered and 15 deactivated test events from this plan's and earlier regression runs, so a projection built from offset 0 will include those synthetic clinics' patients.
- Event ids: registered = `uuid5(<patient>:patient.registered:registered)`; deactivated = `uuid5(<patient>:patient.deactivated:<deactivated_at ISO>)`; schedule change = `uuid5(<provider>:provider.schedule_changed:<feed row id>)`. Consumers must dedupe on the `id` header (Debezium is at-least-once, see 03-05).
- Avro timestamps are millisecond precision; use the `id` header, not timestamps, for identity.

## Self-Check: PASSED

- FOUND: services/patient/events.py, services/provider/events.py, both migrations, both connector files, tests/patient/test_outbox.py, tests/provider/test_outbox.py, tests/e2e/test_phase3_registry_events.py (all committed).
- FOUND commits 650c2ab, c63820c, 392fea8 on gsd/phase-02-scheduling-service-workflow (`git rev-list --count bd233fd..HEAD` = 3 before the docs commit).
