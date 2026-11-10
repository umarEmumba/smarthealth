---
phase: 03-event-driven-system-observability
plan: 03
type: execute
wave: 2
depends_on: ["03-01"]
files_modified:
  - libs/events/catalog.py
  - libs/events/register.py
  - libs/events/schemas/AppointmentCancelled.avsc
  - libs/events/schemas/AppointmentRescheduled.avsc
  - libs/events/schemas/AppointmentCheckedIn.avsc
  - libs/events/schemas/AppointmentCompleted.avsc
  - libs/events/schemas/AppointmentNoShow.avsc
  - libs/events/schemas/VisitCompleted.avsc
  - libs/events/schemas/ProviderScheduleChanged.avsc
  - libs/events/schemas/PatientRegistered.avsc
  - libs/events/schemas/PatientDeactivated.avsc
  - tests/events/test_schemas.py
  - scripts/generate_infra_secrets.py
  - tests/tools/test_generate_infra_secrets.py
autonomous: true
requirements: [EVENT-01]

estimate:
  tokens: 60000
  raw_tokens: 60000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Every event type of D-07 (plus patient.deactivated from the research refinements) has a catalogue entry and a schema file with ids, types and timestamps only, enforced by a test that scans every schema for PHI field names (D-07)"
    - "Services never register schemas at runtime; `python -m libs.events.register`, run at deploy time, is the only writer, sets BACKWARD per subject and refuses an incompatible schema (D-05)"
    - "Every event type serializes through the strict serializer and decodes back, and a second registration run returns the same schema ids"
    - "The Debezium, Grafana read-only and notification broker credentials exist only as gitignored files written with mode 0600"
  artifacts:
    - path: libs/events/register.py
      provides: "python -m libs.events.register [--url] [--check]: BACKWARD compatibility per subject, idempotent registration"
      contains: "BACKWARD"
    - path: libs/events/catalog.py
      provides: "EVENT_CATALOG with all ten event types"
      contains: "patient.deactivated"
    - path: tests/events/test_schemas.py
      provides: "catalogue/file parity, envelope order, PHI field-name scan, round trip per type, registration idempotency and --check failure"
    - path: scripts/generate_infra_secrets.py
      provides: "infra/secrets/infra/{debezium.properties,grafana_ro.secret,notification_broker.secret}"
  key_links:
    - from: libs/events/register.py
      to: libs/events/registry.py
      via: "register_all / get_registry_client from plan 03-01"
      pattern: "register_all"
    - from: libs/events/catalog.py
      to: libs/events/schemas
      via: "each EventSpec.schema_file names an existing .avsc"
      pattern: "avsc"
---

<objective>
Complete the event contract confirmed in plan 03-01 Task 2: the other nine Avro schemas and their catalogue entries, the deploy-time registration CLI (the only registry writer), the PHI field-name scan, and the gitignored infrastructure secret files the CDC plan and Compose need.

Purpose: EVENT-01 (domain events with a registered schema). Plan 03-05's `schema-init` job runs the CLI on the live registry; producers (plans 03-07, 03-08, 03-09) serialize with these schemas; plan 03-05 and plan 03-14 mount the secret files.

Output: nine `.avsc` files, nine catalogue entries, `libs/events/register.py`, `tests/events/test_schemas.py`, `scripts/generate_infra_secrets.py` and its test, and the generated (ignored) secret files.

Decisions honoured: D-05 (Avro + Schema Registry, BACKWARD, one subject per event type, registered in code at deploy time, never at runtime), D-06 revised (record names `com.smarthealth.events.<PascalName>`, topic per aggregate already in the catalogue), D-07 (event set, PHI-free), D-10 revised (`checked_in_at`, `started_at` on `visit.completed`; `slot_start` on appointment events; `patient.deactivated`). Use the names exactly as recorded in the plan 03-01 SUMMARY under "D-05 contract decision" if the user edited the contract there.

Wave 2 coupling: plans 03-02 and 03-04 run alongside. 03-04's fixtures register every catalogue entry through `mock_registry`, so write each `.avsc` file before adding its catalogue entry (the catalogue must never name a missing file). No plan in wave 2 recreates a container.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@libs/events/catalog.py
@libs/events/registry.py
@libs/events/schemas/AppointmentBooked.avsc
@scripts/generate_service_secrets.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `EVENT_CATALOG` additions | module | `appointment.cancelled`, `.rescheduled`, `.checked_in`, `.completed`, `.no_show`, `visit.completed`, `provider.schedule_changed`, `patient.registered`, `patient.deactivated` |
| `libs/events/schemas/*.avsc` | Avro | `com.smarthealth.events.{AppointmentCancelled, AppointmentRescheduled, AppointmentCheckedIn, AppointmentCompleted, AppointmentNoShow, VisitCompleted, ProviderScheduleChanged, PatientRegistered, PatientDeactivated}` (AppointmentBooked exists from plan 03-01) |
| `libs/events/register.py` | CLI module | `python -m libs.events.register [--url] [--check]`; `main(argv) -> int` |
| `scripts/generate_infra_secrets.py` | script | writes `infra/secrets/infra/debezium.properties` (`password=...`), `grafana_ro.secret`, `notification_broker.secret` (0600, directory 0700) |
| tests | tests | `tests/events/test_schemas.py`, `tests/tools/test_generate_infra_secrets.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — appointment.cancelled goes from schema file through the catalogue and the deploy-time CLI to a BACKWARD subject, and round-trips</name>
  <reversibility rating="costly">This plan registers only on `mock://` registries, so schema files can still change here; they become one-way (D-05) when plan 03-05's schema-init registers them on the live registry and consumers depend on them. That one-way door was confirmed at plan 03-01 Task 2's checkpoint:decision; changing names after this plan means editing every producer and consumer plan that uses them.</reversibility>
  <files>libs/events/schemas/AppointmentCancelled.avsc, libs/events/catalog.py, libs/events/register.py, tests/events/test_schemas.py</files>
  <behavior>
    - `register.main(["--url", "mock://p3-register"])` registers one subject per catalogue entry (two at this point: `com.smarthealth.events.AppointmentBooked`, `com.smarthealth.events.AppointmentCancelled`; the test counts against `EVENT_CATALOG`, not a literal, so Task 2's additions keep it valid), sets BACKWARD on each, prints exactly one `subject -> id` line per subject and nothing else, and exits 0
    - A second run against the same registry returns the same ids
    - `register.main([..., "--check"])` registers nothing; with a fake client whose compatibility test returns False it exits 1 and names the subject
    - A registry error exits 1 printing only the exception class name (no URL, no schema body, no traceback)
    - An `appointment.cancelled` sample serializes through `serializer_for` and decodes back with `deserializer()` on the mock registry
  </behavior>
  <read_first>
    - libs/events/catalog.py, libs/events/registry.py, libs/events/schemas/AppointmentBooked.avsc (plan 03-01: envelope, style, `register_all(client, event_types=None)`)
    - tests/events/conftest.py (`mock_registry`)
    - services/scheduling/repository/models.py (Appointment columns available to events)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 1, Pitfall 15)
  </read_first>
  <action>
    Schema first: `AppointmentCancelled.avsc`, same envelope as AppointmentBooked (`event_id` uuid string, `event_type`, `schema_version` int, `occurred_at` timestamp-millis, `aggregate_id`) then appointment, clinic, patient, provider and slot ids, `slot_start`, `cancelled_at`. Then add its `EventSpec` to `EVENT_CATALOG` (aggregate `appointment`).

    Registration module (libs/events/register.py, runnable as `python -m libs.events.register`): arguments `--url` (default `shared_settings.schema_registry_url`) and `--check` (compatibility check only, registers nothing). For each catalogue entry: if the subject exists, test compatibility of the local schema against the latest version and stop with exit 1 naming the subject when incompatible; set the subject's compatibility to BACKWARD; register. Print `subject -> schema id` per subject. Never print schema bodies, URLs with credentials or tracebacks; exit 1 on any registry error with the exception class name only. Module docstring: this is the only writer of the registry (D-05); plan 03-05 runs it as the Compose `schema-init` job.

    Tests (tests/events/test_schemas.py): the behaviors above, using a spy on the mock client to observe the BACKWARD calls and a fake client for the `--check` failure and the error path.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_schemas.py -x -q && .venv/bin/python -m libs.events.register --help</automated>
    <fails_when>pytest exits non-zero, a second run returns different ids, `--check` registers anything, an error path prints a URL or traceback, or `--help` exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_schemas.py -q` exits 0 with at least 5 passed
    - `grep -c "BACKWARD" libs/events/register.py` is at least 1
    - `.venv/bin/python -m libs.events.register --help` exits 0
  </acceptance_criteria>
  <done>A second event type is registered through the deploy-time CLI with BACKWARD compatibility and round-trips through the strict serializer.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The other eight schemas and catalogue entries, parity and the PHI field-name scan</name>
  <files>libs/events/schemas/AppointmentRescheduled.avsc, libs/events/schemas/AppointmentCheckedIn.avsc, libs/events/schemas/AppointmentCompleted.avsc, libs/events/schemas/AppointmentNoShow.avsc, libs/events/schemas/VisitCompleted.avsc, libs/events/schemas/ProviderScheduleChanged.avsc, libs/events/schemas/PatientRegistered.avsc, libs/events/schemas/PatientDeactivated.avsc, libs/events/catalog.py, tests/events/test_schemas.py</files>
  <behavior>
    - Every `EVENT_CATALOG` entry has exactly one `.avsc`, and every `.avsc` belongs to a catalogue entry (ten of each); each parses with `fastavro.parse_schema`; namespace is `com.smarthealth.events`; the five envelope fields come first in order
    - No schema contains a field whose name contains any of: name, birth, dob, phone, address, email, coverage, ssn, insurance, diagnosis, note
    - Each of the ten event types serializes a sample through `serializer_for` and decodes back with `deserializer()` on the mock registry
    - `register.main(["--url", "mock://p3-register-all"])` prints exactly ten `subject -> id` lines
  </behavior>
  <read_first>
    - libs/events/schemas/AppointmentCancelled.avsc and libs/events/catalog.py (Task 1)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9 projection columns, Pattern 7 reminder inputs, Pitfall 15)
    - services/scheduling/repository/models.py (Appointment), services/visit/repository/models.py (Visit: appointment_id, clinic_id, patient_id, provider_id, checked_in_at, started_at, completed_at), services/provider/repository/models.py (ScheduleChangeEvent: id, provider_id, change_type, template_id), services/patient/repository/models.py (patient id, clinic_id, deactivated_at)
  </read_first>
  <action>
    Schemas (same envelope; optional fields are `["null", type]` with default null so later additions stay BACKWARD-compatible; ids are uuid-logical strings; timestamps are long timestamp-millis): AppointmentRescheduled (appointment, clinic, patient, provider ids, `old_slot_id`, `new_slot_id`, `old_slot_start`, `new_slot_start`, `new_slot_end`, `rescheduled_at`); AppointmentCheckedIn (appointment, clinic, patient, provider ids, `slot_start`, `checked_in_at`); AppointmentCompleted (appointment, clinic, patient, provider ids, `slot_start`, `completed_at`); AppointmentNoShow (appointment, clinic, patient, provider ids, `slot_start`, `marked_at`); VisitCompleted (`visit_id`, `appointment_id`, `clinic_id`, `patient_id`, `provider_id`, `checked_in_at`, optional `started_at`, `completed_at`); ProviderScheduleChanged (`provider_id`, `change_event_id`, `change_type` string, optional `template_id`, `changed_at`); PatientRegistered (`patient_id`, `clinic_id`, `registered_at`); PatientDeactivated (`patient_id`, `clinic_id`, `deactivated_at`).

    Add each catalogue entry only after its schema file exists (wave-2 coupling with plan 03-04's fixtures): appointment.* to aggregate `appointment`, visit.completed to `visit`, provider.schedule_changed to `provider`, patient.* to `patient`.

    Tests: extend `tests/events/test_schemas.py` with the parity, parse, envelope-order, PHI field-name scan and per-type round-trip cases above (parametrized over the catalogue).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events -x -q</automated>
    <fails_when>pytest exits non-zero, a PHI field name is found, the catalogue and the schema files disagree, or any type fails to round-trip.</fails_when>
  </verify>
  <acceptance_criteria>
    - `ls libs/events/schemas/*.avsc | wc -l` prints 10
    - `.venv/bin/python -m pytest tests/events/test_schemas.py -q` exits 0 with at least 25 passed (parametrized cases over ten types)
    - `.venv/bin/python -m pytest tests/events -q` exits 0 (plan 03-01's round trip and plan 03-04's fixtures still pass)
  </acceptance_criteria>
  <done>All ten PHI-free schemas exist, match the catalogue one to one, and round-trip.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: The infrastructure secret files (Debezium, Grafana read-only, notification broker)</name>
  <files>scripts/generate_infra_secrets.py, tests/tools/test_generate_infra_secrets.py</files>
  <behavior>
    - `generate_infra_secrets.main(["--output-dir", tmp])` writes `debezium.properties` (single line `password=<value>`), `grafana_ro.secret` and `notification_broker.secret` with mode 0600 in a 0700 directory
    - It prints paths only (stdout never contains a generated value)
    - It refuses to overwrite without `--force` (exit 1, nothing written); with `--force` it replaces the files
  </behavior>
  <read_first>
    - scripts/generate_service_secrets.py (safety properties and output style to mirror)
    - tests/tools/ (existing test style for the service secrets script)
    - .gitignore (confirm `infra/secrets/` is ignored)
  </read_first>
  <action>
    Secrets script (scripts/generate_infra_secrets.py): a sibling of `generate_service_secrets.py` with the same safety properties (0700 directory, 0600 files via `os.open` with O_EXCL unless `--force`, refuse to overwrite, print paths only, `secrets.token_urlsafe(32)`), default output `infra/secrets/infra`, files `debezium.properties` (content `password=<value>` plus newline, the format Kafka Connect's FileConfigProvider reads), `grafana_ro.secret`, `notification_broker.secret`. Document in the module docstring which Compose service reads which file (db-init and connect: debezium, plan 03-05; db-init and grafana: grafana_ro, plans 03-05 and 03-16; notification processes and rabbitmq-init: notification_broker, plan 03-14).

    Tests (tests/tools/test_generate_infra_secrets.py): modes, content format, refusal without `--force`, stdout contains paths and no secret value.

    After the tests pass, run `.venv/bin/python scripts/generate_infra_secrets.py` once so the files exist for plans 03-05 and 03-14, and confirm with `git check-ignore` that they are ignored (never `cat` them or print their contents).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/tools/test_generate_infra_secrets.py -x -q</automated>
    <fails_when>pytest exits non-zero, a file is written with a mode other than 0600, stdout contains a generated value, or an existing file is overwritten without `--force`.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/tools/test_generate_infra_secrets.py -q` exits 0 with at least 4 passed
    - `git check-ignore -q infra/secrets/infra/debezium.properties && git check-ignore -q infra/secrets/infra/grafana_ro.secret && git check-ignore -q infra/secrets/infra/notification_broker.secret` exits 0
    - `stat -f '%Lp' infra/secrets/infra/debezium.properties` prints 600
  </acceptance_criteria>
  <done>The three infrastructure secrets exist as ignored 0600 files, generated by a tested script that never prints them.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| deploy job -> Schema Registry | only the registration module may write subjects |
| schema files -> every consumer | the schema decides which fields can ever reach Kafka |
| developer host -> secret files | generated credentials for Debezium, Grafana and the broker |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-02 | Tampering | Schema Registry subjects | medium | mitigate | Registration only through `python -m libs.events.register` (services run strict serializers, T-3-57); BACKWARD set per subject and an incompatible schema refused before registering; `--check` mode for CI |
| T-3-56 | Information Disclosure | the nine new `.avsc` schemas | high | mitigate | Ids, types and timestamps only; parametrized test scans every schema for PHI field-name fragments (name, birth, dob, phone, address, email, coverage, ssn, insurance, diagnosis, note); the strict serializer drops nothing silently (unknown fields fail) |
| T-3-03 | Information Disclosure | `infra/secrets/infra/*` | high | mitigate | 0600 files in a 0700 gitignored directory, O_EXCL creation, refusal to overwrite, script prints paths only; files reach containers only as Compose secrets (plans 03-05, 03-14, 03-16) |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan; fastavro and confluent-kafka were approved and installed in plan 03-01 Task 1 |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/events tests/tools/test_generate_infra_secrets.py -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (do not set OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto libs/events scripts/generate_infra_secrets.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- Ten PHI-free schemas match the catalogue one to one and round-trip
- A deploy-time registration CLI sets BACKWARD per subject and is the only registry writer
- The infrastructure secret files exist, ignored and 0600
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-03-SUMMARY.md` when done
</output>
