---
phase: 03-event-driven-system-observability
plan: 03
subsystem: events
tags: [avro, schema-registry, backward-compatibility, phi-scan, secrets]
requires:
  - phase: 03-event-driven-system-observability
    provides: libs/events catalogue, strict registry client (register_all), AppointmentBooked schema, mock_registry fixture (plan 03-01)
provides:
  - ten PHI-free Avro schemas matching EVENT_CATALOG one to one
  - deploy-time registration CLI (python -m libs.events.register [--url] [--check]), the only registry writer
  - generate_infra_secrets script and gitignored 0600 Debezium, Grafana read-only and notification broker secret files
affects: [03-04, 03-05, 03-07, 03-08, 03-09, 03-14, 03-16]
tech-stack:
  added: []
  patterns:
    - "Registration is a deploy-time CLI: BACKWARD set per subject, incompatible schema refused before register, errors print the exception class name only"
    - "Optional Avro fields are [null, type] with default null so later additions stay BACKWARD-compatible"
    - "Catalogue entry added only after its .avsc exists; parity enforced by test"
key-files:
  created:
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
  modified:
    - libs/events/catalog.py
key-decisions:
  - "Event names, record names and topic-per-aggregate used exactly as confirmed in plan 03-01 (D-05 contract unchanged)"
  - "The owner-only os.chmod(dir, 0o700) is annotated nosemgrep as a false positive of the insecure-file-permissions rule (0700 is the restrictive mode, not a weakening)"
requirements-completed: []
status: complete
duration: not measured (executor interrupted by a laptop sleep; finalised by a separate agent)
completed: 2026-11-11
actuals:
  tokens: 9500
  tasks: 3
  commits: 4
plan_head_before: 92d34ba04a7f5951341adf6aed58a30f6092bef8
plan_head_after: 8ebff3f3974638bf4fca4cfa2cbc7f617abdc183
---

# Phase 3 Plan 03: Event Schemas Summary

**Ten PHI-free Avro schemas matched one to one to EVENT_CATALOG, a deploy-time `libs.events.register` CLI that is the only registry writer (BACKWARD per subject, refuses incompatible schemas, `--check` mode), and a tested script that generates the three infrastructure secret files as 0600 files in a 0700 gitignored directory.**

This SUMMARY was written by a finalise-only agent. The original executor completed all three tasks and committed the code, then was killed when the laptop went to sleep, before writing this file. No code was changed during finalisation; acceptance criteria were re-checked against the committed code with short read-only commands.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 305b68c | `appointment.cancelled` schema, catalogue entry, `libs/events/register.py`, first tests |
| 2 | fb7f7e0 | Remaining eight schemas and catalogue entries, parity, envelope-order, PHI field-name scan, per-type round trip |
| 3 | c18e0ba | `scripts/generate_infra_secrets.py` and its tests |
| 3 (follow-up) | 8ebff3f | Semgrep false-positive annotation on the owner-only 0700 chmod |

## Acceptance criteria check

| Criterion | Result |
|-----------|--------|
| T1: `tests/events/test_schemas.py` exits 0 with at least 5 passed | Confirmed (test file passes; 75 passed at finalisation) |
| T1: `grep -c BACKWARD libs/events/register.py` is at least 1 | Confirmed, 4 |
| T1: `python -m libs.events.register --help` exits 0 | Confirmed |
| T2: `ls libs/events/schemas/*.avsc \| wc -l` prints 10 | Confirmed, 10 |
| T2: test_schemas at least 25 passed | Confirmed, 75 passed |
| T2: `pytest tests/events` exits 0 | Confirmed by orchestrator (see Verification) |
| T3: `test_generate_infra_secrets.py` at least 4 passed | Confirmed, 6 passed |
| T3: all three secret files git-ignored | Confirmed (`git check-ignore` exit 0) |
| T3: `debezium.properties` mode 600 | Confirmed; the other two files are 600 and the directory is 700 |
| Catalogue contains `patient.deactivated` | Confirmed (`PATIENT_DEACTIVATED` in `libs/events/catalog.py`) |

Not independently re-run by the finalising agent: the individual behaviours inside the tests (second-run id equality, `--check` failure naming the subject, error path printing only the class name, ten `subject -> id` lines). They are covered by the test file, which passes; the finalising agent did not read each assertion.

## Verification

Measured by the orchestrator after the interruption:

- `.venv/bin/python -m pytest -q tests/events tests/tools tests/libs/test_service_isolation.py`: 139 passed, 0 failed, 21.7 s, OTEL_SDK_DISABLED unset.
- `semgrep --config p/python --metrics=off libs/events scripts/generate_infra_secrets.py`: 151 rules, 6 files, 0 findings.
- Stack healthy via `scripts/compose_healthcheck.sh`.

Re-checked by the finalising agent: `tests/events/test_schemas.py` 75 passed, `tests/tools/test_generate_infra_secrets.py` 6 passed.

Not claimed: a clean full-suite run (`pytest tests -m "not e2e"`) for this plan. The interrupted executor's full-suite run spanned a laptop sleep with stale connections and was discarded as invalid. The phase gate in 03-20 and later wave checks run the full regression.

Semgrep note: the plan asked for `--config auto`; the run recorded above used `--config p/python --metrics=off` (metrics disabled, no auto config fetch). This is a narrower ruleset than the plan text.

## Deviations from Plan

- **Semgrep ruleset:** run with `p/python --metrics=off` instead of `--config auto` (see above). Result 0 findings.
- **Owner-only chmod annotation (commit 8ebff3f):** a Semgrep insecure-file-permissions finding on `os.chmod(output_dir, 0o700)` was annotated `nosemgrep` as a false positive (0700 is the restrictive mode). Not in the plan; Rule 2 style hygiene so the Semgrep status is clean.
- **Full-suite verification not run for this plan** (invalid interrupted run discarded; see Verification).
- **Secret generation run:** the plan's "run the script once" step is evidenced by the three ignored 0600 files existing in `infra/secrets/infra` (directory 0700); the finalising agent did not read their contents.

Otherwise the plan executed as written. No auth gates.

## Known Stubs

None. The only "placeholder" match in the touched files is the `{value}` format-string note in `scripts/generate_infra_secrets.py`, which is a real template, not a stub.

## Deferred Issues

- `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` is a known unrelated failure that also fails on the pre-Phase-3 baseline commit 0770e86. Out of scope for this plan.

## Threat Flags

None. No new network endpoint, auth path or trust-boundary surface beyond the plan's threat model (T-3-02, T-3-56, T-3-03 mitigations are implemented: BACKWARD per subject with refusal, PHI field-name scan over every schema, 0600/0700 O_EXCL secret files that never print values).

## Self-Check: PASSED

- Created files exist: libs/events/register.py, ten .avsc files, tests/events/test_schemas.py, scripts/generate_infra_secrets.py, tests/tools/test_generate_infra_secrets.py (verified via `ls` and test runs).
- Commits 305b68c, fb7f7e0, c18e0ba, 8ebff3f exist on branch gsd/phase-02-scheduling-service-workflow (`git rev-list --count 92d34ba..HEAD` = 4).
