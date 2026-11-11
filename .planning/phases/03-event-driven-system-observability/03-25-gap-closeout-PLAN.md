---
phase: 03-event-driven-system-observability
plan: 25
type: execute
wave: 18
depends_on: ["03-21", "03-22", "03-23", "03-24"]
files_modified:
  - docs/architecture.md
  - docs/architecture-dashboard.html
  - README.md
  - docs/nfr.md
  - loadtest/README.md
  - tests/docs/test_phase3_docs.py
  - tests/docs/test_dashboard.py
  - .planning/STATE.md
autonomous: true
gap_closure: true
requirements: [EVENT-05, OBS-01, DASH-03, DASH-04]

estimate:
  tokens: 80000
  raw_tokens: 80000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 docs: docs/architecture.md section 10.2 no longer claims that an infrastructure outage is never dead-lettered; it states what the code now does (SQLSTATE-class classification, capped retries, TransientExhausted after both thresholds, commit retry) and the recovery route per consumer group (visit reconciler and backfill, analytics rebuild, operator re-produce for notification-trigger), and sections 4f, 10.4, 10.7 and 10.9 match plans 03-21..03-24"
    - "The architecture dashboard repeats none of the false claim: exactly three passages are corrected (the consumer-groups node, the sweeper-retired card's tradeoff, the Q&A outage answer), nothing else in the visually approved page changes, and the SUMMARY lists each changed sentence before and after"
    - "Guard tests in tests/docs fail if the false claim returns to the docs or the dashboard"
    - "The k6 smoke profile (and only smoke; sustained, burst and contention stay deferred, D-04) passes on the stack with multiprocess metrics, run with the plan 03-16 admin procedure, and its result is recorded in docs/nfr.md and loadtest/README.md next to the earlier runs; over the smoke window the nine API jobs show zero http_requests_total resets"
    - "The full non-e2e and e2e suites were run on the final stack (background, sleep-checked), every failure is classified, the connection peak is recorded, Semgrep status is recorded, and the gateway is back on its default budget (20 and 300) as the very last step"
    - "STATE.md and the SUMMARY carry the open follow-ups: WR-01, WR-04, WR-06, IN-02..IN-06, the dev max_connections not raised, Jaeger memory not capped"
  artifacts:
    - path: docs/architecture.md
      provides: "corrected 10.2 delivery semantics, reconciler lost-trigger pass, multiprocess metrics, connection budget"
      contains: "TransientExhausted"
    - path: tests/docs/test_phase3_docs.py
      provides: "stale-claim guard for the outage sentence; positive checks for the new terms"
    - path: tests/docs/test_dashboard.py
      provides: "stale-text guard for the two dashboard sentences"
    - path: docs/nfr.md
      provides: "the gap-closure smoke run next to the earlier three"
  key_links:
    - from: docs/architecture.md
      to: libs/events/consumer.py
      via: "names TransientExhausted, event_commit_failures_total and dlq_events_total exactly as the code declares them (tests/docs metric and name checks)"
      pattern: "event_commit_failures_total"
    - from: docs/architecture-dashboard.html
      to: services/visit/downstream.py
      via: "the corrected sentences describe the reconciler's lost-trigger restart that restart_lost_triggers implements"
      pattern: "reconciler"
---

<objective>
Finish the gap closure: make the documents state what the code now does (verification gap 1's last "missing" item: correct `docs/architecture.md` 10.2 and the dashboard), rerun the k6 `smoke` profile on the stack with correct HTTP metrics (user decision: smoke only), run the final regression on the finished stack, record the open follow-ups, and restore the gateway's default rate budget as the last action of the gap closure.

Purpose: D-12 says docs state only what the code does; the verifier found 10.2 contradicted by the code. D-04 requires the smoke profile to keep passing; CR-02 made the earlier smoke's Prometheus view untrustworthy, so the rerun is the first one read from correct counters.

Dashboard rule: `docs/architecture-dashboard.html` was visually approved by the user. Change exactly the three passages named in Task 1, keep the markup structure and the version marker, and list every changed sentence (before and after) in the SUMMARY.

Live stack rules:
- Nothing is rebuilt in this plan. The only recreate is the gateway: kept at the raised budget for the smoke and the suites, and restored to the defaults with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=20 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=300 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` as the final step of Task 3 (explicit values, so a host `.env` cannot keep the raised budget). Never `down -v`, never remove a volume, never touch `tjo-*` containers (record status before and after).
- Load test credentials: the plan 03-16 procedure only (Task 2). Never sustained, burst or contention.
- Long suites run in the background with polling and a 20 s wall-clock heartbeat; a gap above 60 s (laptop sleep) invalidates the run, which is then rerun. One pytest process at a time. `tests/visit` writes real visit rows that the live `visit-consumer` would race, so stop it (`docker compose -f infra/docker-compose.yml stop visit-consumer`) for the whole non-e2e run and start it again (wait for `healthy`) before the e2e run.
- No `.env*` or `infra/secrets/*` reads, no package installs, no REQUIREMENTS.md edits, no requirement ticked.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-VERIFICATION.md
@.planning/phases/03-event-driven-system-observability/03-REVIEW.md
@.planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-22-visit-trigger-recovery-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-23-connection-budget-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-SUMMARY.md
@docs/architecture.md
@tests/docs/test_phase3_docs.py
@tests/docs/test_dashboard.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `docs/architecture.md` 4f, 10.2, 10.4, 10.7, 10.9 | docs | corrected delivery semantics and recovery routes; multiprocess metrics on all nine APIs; connection budget and measured peak; follow-ups |
| `docs/architecture-dashboard.html` | docs | three sentences corrected (consumer-groups node, sweeper-retired card tradeoff, Q&A outage answer) |
| `README.md` Phase 3 settings table | docs | rows for `DB_POOL_SIZE` (`5`) and `DB_MAX_OVERFLOW` (`10`), Compose values in the description |
| `docs/nfr.md`, `loadtest/README.md` | docs | the gap-closure smoke run, dated, next to the earlier runs |
| `STALE_CLAIMS` additions, dashboard stale strings | tests | guard against the false outage claim |
| `.planning/STATE.md` | planning | Phase 3 follow-ups updated |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a guard test rejects the false outage claim, then the docs and the dashboard are corrected to what the code does</name>
  <files>docs/architecture.md, docs/architecture-dashboard.html, README.md, tests/docs/test_phase3_docs.py, tests/docs/test_dashboard.py</files>
  <behavior>
    - New guard patterns (added first, run red against the current docs): `STALE_CLAIMS` in tests/docs/test_phase3_docs.py gains a pattern for the 10.2 sentence that says an infrastructure outage (Schema Registry, Postgres, Temporal, RabbitMQ) is never dead-lettered; `test_stale_text_removed` in tests/docs/test_dashboard.py gains the two dashboard phrasings (the consumer-groups node's "retried and never dead-lettered" and the Q&A's "the message is never dead-lettered")
    - A positive test: the Phase 3 part of architecture.md names `TransientExhausted`, `event_commit_failures_total`, `list_lost_triggers` or `restart_lost_triggers`, and `MultiProcessCollector`; the dashboard Q&A section contains "TransientExhausted"
    - After the edits: `pytest tests/docs` is green, including every existing name, metric, setting, topic and honesty check (nothing weakened, the version marker unchanged, "outage" still in the Q&A)
  </behavior>
  <read_first>
    - docs/architecture.md: 4f (lines 605-613, the visit reconciler paragraph that says never-started visits are only the consumer's job), 10.2 (lines 1071-1078, the consumer base paragraph), 10.4 (lines 1150-1164, visit lifecycle and backfill), 10.7 (lines 1299-1311, "Only `scheduling` was rebuilt with the new buckets"), 10.9 (lines 1423-1456, test-environment capacity, "not done", "Not verified")
    - docs/architecture-dashboard.html: line 504 (Three consumer groups node), line 680 (sweeper-retired card, Tradeoff), line 1406 (Q&A answer on failures)
    - README.md "### Phase 3 settings" table (the README test parses its rows up to "Retired in Phase 1.1")
    - tests/docs/test_phase3_docs.py (`STALE_CLAIMS`, `test_metric_names_the_docs_cite_exist_in_the_code_or_the_dashboards`, `test_every_setting_the_phase3_sections_name_exists_in_the_code`, the README settings tests)
    - tests/docs/test_dashboard.py (`test_stale_text_removed`, `test_qa_covers_phase3_themes`, `test_phase3_honesty_items_present`)
    - the four SUMMARYs of plans 03-21..03-24 (the measured numbers to quote: cap defaults, connection peak and budget, resets 0)
  </read_first>
  <action>
    Add the guard patterns and the positive test first and run `tests/docs` to see them fail on the current text (record it). Then edit:

    architecture.md 10.2: replace the outage sentence of the consumer-base paragraph with the current behaviour: failures are classified by one module (`libs/events/classify.py`): SQLSTATE classes 08, 53, 57 and 40 (for example 53300 too many connections, 57P03 cannot connect now, 40P01 deadlock), SQLAlchemy connection errors and pool timeouts, Schema Registry 408, 429 and 5xx, Temporal UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED, broker, Mongo and network errors are retried with capped backoff; when a message has failed transiently at least 10 times over at least 15 minutes (the defaults) it is dead-lettered with `dlq.error_type` `TransientExhausted`, counted in `dlq_events_total`, and recovered per group (`visit-downstream`: the visit reconciler's lost-trigger pass after `VISIT_RECONCILE_GRACE_MINUTES`, or the backfill script; `analytics-projection`: `services.analytics.rebuild`; `notification-trigger`: the DLQ record keeps the original key, value and headers, and re-producing it to `dlq.original_topic` is a manual operator step, absorbed by the dedupe keys and deterministic workflow ids); a failed offset commit is retried, counted in `event_commit_failures_total` and never stops the consumer unless the error is fatal. Add one sentence that this paragraph used to claim the opposite and that the gap-closure verification found it false for asyncpg resource and connection errors (CR-01).

    4f: the visit reconciler also restarts lost triggers (completed, marker NULL, past the grace) through `ensure_downstream_started`; the sweeper stays retired. 10.4: the recovery route and the backfill's `stale_queued` / `restarted` counts and `--stale-after-minutes`. 10.7: the nine APIs run two gunicorn workers in Prometheus multiprocess mode (`MultiProcessCollector`, directory emptied at start, `child_exit` marks dead workers), all nine were rebuilt, measured zero counter resets; drop the "only scheduling was rebuilt" sentence. 10.9: the capacity item becomes current: closed-loop engines are retired, pool sizes are settings (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`), the Compose worst case is 237 of 297 connections, the measured peak from plan 03-23 and from Task 3 of this plan, and what is still not done (raising the dev `max_connections`, capping Jaeger memory); the "Not verified" list says the registry 408/429/5xx path is unit-tested, not exercised live.

    Dashboard: change only the three passages: the consumer-groups node now says an outage is retried with capped backoff and, after the cap, dead-lettered as TransientExhausted for recovery; the sweeper-retired card's Tradeoff gains one sentence that a dead-lettered or dropped `visit.completed` is restarted by the visit reconciler after its grace; the Q&A outage answer says the offset is held and retried for up to the cap, after which the message is dead-lettered as TransientExhausted and recovered by the visit reconciler, the analytics rebuild or an operator replay. Keep the word "outage" in that answer and every other sentence as it is.

    README: add `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` rows to the Phase 3 settings table (defaults `5` and `10`; Compose sets 5 and 5 for APIs and workers, 2 and 2 for the two consumers).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>pytest exits non-zero: the false claim is still present in a document or the dashboard, a named metric, setting, topic or group does not exist in the code, an honesty or Q&A needle is gone, or the dashboard version marker changed.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY records the red run of the new guard tests and the green run of `tests/docs`
    - `grep -c "TransientExhausted" docs/architecture.md` prints at least 2 and `grep -c "TransientExhausted" docs/architecture-dashboard.html` prints at least 2
    - `git diff --numstat HEAD -- docs/architecture-dashboard.html` shows only the three edited lines' worth of change (about 3 to 6 lines added and removed), listed sentence by sentence in the SUMMARY
  </acceptance_criteria>
  <done>The documents say what the code does about outages, dead letters and recovery, and the tests keep it that way.</done>
</task>

<task type="auto">
  <name>Task 2: k6 smoke on the stack with correct HTTP metrics, recorded</name>
  <precondition>All nine APIs run the plan 03-24 images (`grep -l "python:libs.observability.gunicorn_conf" services/*/Dockerfile | wc -l` prints 9 and all nine containers are healthy) and the live gateway prints 1000 and 100000 for its two rate-limit variables.</precondition>
  <files>docs/nfr.md, loadtest/README.md</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md (deviation 1: the admin procedure; the k6 and Prometheus figures and how they were read)
    - loadtest/README.md (seed options, `run.sh` profiles, the earlier smoke entries) and docs/nfr.md (the smoke section)
    - scripts/loadtest_seed.py (reads `LOADTEST_ADMIN_EMAIL` / `LOADTEST_ADMIN_PASSWORD`, prints counts only)
    - services/identity/security/passwords.py (`hash_password`) and services/identity/repository/models.py (`users.hashed_password`, `role`)
    - tests/docs/test_phase3_docs.py (`test_the_nfr_page_records_the_phase3_smoke_runs_and_keeps_the_unmeasured_status`)
  </read_first>
  <action>
    Quiet stack first: `scripts/compose_healthcheck.sh` OK, no pytest process running; record the load average. Admin credential, plan 03-16 procedure only: with `.venv/bin/python`, generate a password with `secrets.token_urlsafe(24)`, hash it with `services.identity.security.passwords.hash_password`, and run one UPDATE of `users.hashed_password` WHERE `email = 'loadtest-admin@example.com' AND role = 'admin'` on identity_db through the host `IdentitySettings` URL; abort unless exactly one row changed; write the password to a 0600 file in the executor's scratchpad directory; never print it. Seed with `LOADTEST_ADMIN_EMAIL=loadtest-admin@example.com LOADTEST_ADMIN_PASSWORD="$(cat <that file>)" .venv/bin/python scripts/loadtest_seed.py --providers 2 --patients 250 --days 2`, then run `loadtest/run.sh smoke` and require exit 0. Delete the password file and `loadtest/data/plan.json` immediately afterwards and confirm both are gone. Do not run `sustained`, `burst` or `contention` (D-04, deferred by the user).

    Read back from Prometheus for the run window: accept p95 / p99 (`http_request_duration_seconds`, scheduling, `POST /api/appointments`), saga p95 / p99 (`booking_saga_seconds`), the gateway 5xx ratio, and `sum by (job)(resets(http_requests_total[...]))` over the window for the nine API jobs (must be 0). Add a dated entry for this run to `docs/nfr.md` and `loadtest/README.md` next to the earlier runs: k6 figures, the Prometheus figures (first ones read from multiprocess counters), host load; keep every existing figure and the statements that the NFRs are not yet measured and that the three other profiles were not run.
  </action>
  <verify>
    <automated>test ! -e loadtest/data/plan.json && .venv/bin/python -m pytest tests/docs/test_phase3_docs.py tests/loadtest -q</automated>
    <fails_when>k6 exited non-zero (recorded in the SUMMARY), the plan file was left behind, a resets value is above 0, or the docs tests fail (a missing earlier figure, a missing "not run" / "not yet measured" statement).</fails_when>
  </verify>
  <acceptance_criteria>
    - `loadtest/run.sh smoke` exited 0 and the SUMMARY records the k6 accept p95 / p99, saga p95 / p99, error rate, iterations and the Prometheus figures
    - the password file no longer exists and was never printed; only the `loadtest-admin@example.com` row was updated (UPDATE 1)
    - `git diff HEAD -- docs/nfr.md loadtest/README.md` adds figures and dates only (no credential, password or token), checked by reading the diff before the commit
  </acceptance_criteria>
  <done>The smoke profile passes on the corrected stack and its Prometheus view is trustworthy.</done>
</task>

<task type="auto">
  <name>Task 3: Final gap verification on the finished stack, follow-ups recorded, gateway budget restored last</name>
  <files>.planning/STATE.md</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md (gate procedure, failure classes, connection-ceiling measurement, Semgrep triage table)
    - tests/e2e/phase3_stack.py (`psql_superuser`, `compose`)
    - scripts/compose_healthcheck.sh
    - .planning/STATE.md ("Phase 3 follow-ups" under Pending Todos)
    - the SUMMARYs of plans 03-21..03-24 (what each closed, with which test)
  </read_first>
  <action>
    Record `tjo-*` status. Run, one at a time, in the background with polling, a 20 s heartbeat file and a 15 s `pg_stat_activity` sampler (superuser over the container socket, as in `tests/e2e/phase3_stack.py` `psql_superuser`): (1) with `visit-consumer` stopped, `.venv/bin/python -m pytest tests -m "not e2e" -q` (do NOT set `OTEL_SDK_DISABLED`), then start `visit-consumer` and wait for `healthy`; (2) `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q`. Save both logs and the sampler output under `.planning/phases/03-event-driven-system-observability/closeout-logs/` with a `gap-` prefix (leave them untracked if they contain a dev credential in a traceback, as plan 03-20 did). Classify every failure: the two known unrelated ones (`tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway`, `tests/identity/test_email_migration.py`), a macOS fork SIGTRAP (exit -5, rerun alone), or new; a `TooManyConnectionsError` or any failure in a file touched by plans 03-21..03-24 is new and must be fixed or reported, not accepted. Record the connection peak next to the gate's 304.

    Gap checklist (each with its evidence in the SUMMARY): CR-01 classification (03-21 tests), CR-01 recovery (`test_lost_visit_trigger_is_recovered_by_the_reconciler` in the e2e log), WR-02, WR-03, WR-07 (03-21 tests), WR-05 (03-23 test and peaks), CR-02 (`test_http_request_counters_have_no_resets_and_never_decrease` for nine jobs), docs (Task 1). Run Semgrep over the changed paths: `semgrep --config p/python --config p/security-audit --metrics=off libs/events libs/db libs/observability libs/config.py services/visit services/analytics/consumer.py services/notification/triggers.py services/gateway/app.py scripts/backfill_visit_completed_events.py`; triage and record (the Dockerfile `missing-user` findings stay user-accepted).

    Update `.planning/STATE.md` Phase 3 follow-ups: mark done the engine disposal and the gateway restore; keep open WR-01 (reminder reschedule-signal race, needs `workflow.patched` plus a replay test), WR-04 (debezium role privileges, Connect REST auth, FileConfigProvider allowed paths; PHI-relevant), WR-06 (backfill or replay can send follow-ups for old visits), IN-02..IN-06, raising the dev `max_connections`, capping Jaeger memory, the deferred k6 profiles. Do not edit REQUIREMENTS.md.

    LAST step of the gap closure: restore the gateway budget with the explicit-defaults command from the objective, wait for `healthy`, and confirm `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` prints 20 and 300. Run `scripts/compose_healthcheck.sh` and record `tjo-*` status again. Nothing runs against the gateway after this step.
  </action>
  <verify>
    <automated>grep -qE "[0-9]+ passed" .planning/phases/03-event-driven-system-observability/closeout-logs/gap-non-e2e.txt && grep -qE "[0-9]+ passed" .planning/phases/03-event-driven-system-observability/closeout-logs/gap-e2e.txt && test "$(docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE | tr -d '\r')" = "20" && test "$(docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE | tr -d '\r')" = "300" && .venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>A log is missing or has no pytest summary, a failure outside the two known unrelated tests and the SIGTRAP class is left unexplained, a TooManyConnectionsError appears, the gateway still runs a raised budget, or the docs suite fails.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY has the gap checklist with evidence per item, the failure classification table, the connection peak, the Semgrep triage and the dashboard sentence list from Task 1
    - the e2e log shows `test_lost_visit_trigger_is_recovered_by_the_reconciler` and the nine `test_http_request_counters_have_no_resets_and_never_decrease` cases passed
    - the gateway prints 20 and 300; `scripts/compose_healthcheck.sh` is OK; tjo-* status lines before and after are identical apart from uptime
    - `git diff HEAD -- .planning/REQUIREMENTS.md` is empty
  </acceptance_criteria>
  <done>Both verification gaps are closed with evidence on the finished stack, the follow-ups are recorded, and the gateway is back on its defaults.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| operator host -> identity_db | the smoke procedure rotates one admin password |
| public clients -> gateway | the rate budget protects the published port |
| documents -> reviewers | the docs and the dashboard are graded statements about guarantees |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-76 | Information Disclosure | load-test admin password | high | mitigate | Random `token_urlsafe(24)`, hashed with the service's own function, one row (`email` and `role` both matched, UPDATE 1 required), 0600 file in the scratchpad, never printed, deleted with `loadtest/data/plan.json` right after the run |
| T-3-77 | Elevation of Privilege / Denial of Service | gateway left on the raised test budget (1000 / 100000 per minute) | medium | mitigate | Explicit restore to 20 and 300 as the last step, verified with `printenv`; nothing uses the gateway afterwards |
| T-3-78 | Repudiation | documents overstating delivery guarantees (the CR-01 doc defect) | medium | mitigate | Guard tests for the false claim in docs and dashboard; positive checks that the new mechanisms are named; names cross-checked against the code by the existing docs tests |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs tests/loadtest -q`
- the two full-suite logs under `closeout-logs/gap-*.txt` with every failure classified
- `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` prints 20 and 300
</verification>

<success_criteria>
- Verification gaps 1 and 2 closed with live evidence; the docs and the dashboard state only what the code does
- Smoke passes on the corrected stack; sustained, burst and contention still deferred
- Gateway on its default budget; follow-ups recorded
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-25-gap-closeout-SUMMARY.md` when done
</output>
