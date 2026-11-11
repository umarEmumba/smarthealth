---
phase: 03-event-driven-system-observability
plan: 26
type: execute
wave: 19
depends_on: ["03-21", "03-22", "03-23", "03-24", "03-25"]
files_modified:
  - docs/architecture.md
  - docs/architecture-dashboard.html
  - README.md
  - tests/docs/test_phase3_docs.py
  - tests/docs/test_dashboard.py
  - .planning/STATE.md
autonomous: false
gap_closure: true
requirements: [EVENT-05, OBS-01, DASH-03, DASH-04]

estimate:
  tokens: 75000
  raw_tokens: 75000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 docs: docs/architecture.md section 10.2 no longer claims that an infrastructure outage is never dead-lettered; it states what the code now does (one classifier, SQLSTATE classes, capped retries, TransientExhausted after both thresholds, commit retry) and, per consumer group, exactly the recovery tools that exist (W4): visit-downstream: the visit reconciler's lost-trigger pass (automatic, after the grace), the backfill's stale_queued / restarted path and scripts/replay_dlq.py (manual); analytics-projection: scripts/replay_dlq.py or services.analytics.rebuild; notification-trigger: scripts/replay_dlq.py; sections 4f, 10.4, 10.7 and 10.9 match plans 03-21..03-25, and 10.9 states the proof level (I6): classification proven at the real _handle_message path at unit level plus the live lost-trigger e2e, no live 53300 injection"
    - "The architecture dashboard repeats none of the false claim: exactly the three passages quoted in this plan are changed (the consumer-groups node, the sweeper-retired card's tradeoff, the Q&A outage answer), nothing else in the visually approved page changes (D-14), and the SUMMARY lists each changed sentence before and after"
    - "Guard tests in tests/docs fail if the false claim returns to the docs or the dashboard"
    - "The full non-e2e and e2e suites were run on the final stack (background, sleep-checked) and the final logs' pytest summary lines report no failed and no error test; the one known baseline failure is deselected by node id and listed, never absorbed by a looser check; the connection peak is below 270; the lost-trigger e2e and the nine no-reset cases appear as PASSED; Semgrep status is recorded; and the gateway is back on its default budget (20 and 300) as the last automated step"
    - "STATE.md and the SUMMARY carry the open follow-ups: WR-01, WR-04, WR-06, IN-02..IN-06, the deselected provider tracer test, the dev max_connections not raised, Jaeger memory not capped, the deferred k6 profiles"
    - "The user re-checked exactly the three changed dashboard sentences (they approved the dashboard before; D-14) and the reply is recorded"
  artifacts:
    - path: docs/architecture.md
      provides: "corrected 10.2 delivery semantics and per-group recovery tools, reconciler lost-trigger pass, multiprocess metrics, connection budget, proof level"
      contains: "TransientExhausted"
    - path: tests/docs/test_phase3_docs.py
      provides: "stale-claim guard for the outage sentence; positive checks for the new terms"
    - path: tests/docs/test_dashboard.py
      provides: "stale-text guard for the two dashboard sentences"
  key_links:
    - from: docs/architecture.md
      to: libs/events/consumer.py
      via: "names TransientExhausted, event_commit_failures_total and dlq_events_total exactly as the code declares them (tests/docs metric and name checks)"
      pattern: "event_commit_failures_total"
    - from: docs/architecture-dashboard.html
      to: services/visit/downstream.py
      via: "the corrected sentences describe the reconciler's lost-trigger restart that restart_lost_triggers implements"
      pattern: "reconciler"
    - from: docs/architecture.md
      to: scripts/replay_dlq.py
      via: "10.2 names the replay script, its dry-run default and --apply exactly as the script implements them"
      pattern: "replay_dlq"
---

<objective>
Finish the gap closure: make the documents state what the code now does (verification gap 1's last "missing" item: correct `docs/architecture.md` 10.2 and the dashboard), run the final regression on the finished stack with gate checks that fail on any failed or errored test, record the open follow-ups, restore the gateway's default rate budget as the last automated action, and have the user re-check exactly the three dashboard sentences that changed. The k6 smoke rerun already happened on the finished stack in plan 03-25 Task 3; this plan cites its recorded result.

Purpose: D-12 says docs state only what the code does; the verifier found 10.2 contradicted by the code. D-14 allows at most three listed dashboard sentences to change and asks the user to re-check them.

Docs rule (W4): describe only tools and behaviour that exist after plans 03-21..03-25, by their real names (`scripts/replay_dlq.py`, `restart_lost_triggers`, `scripts/backfill_visit_completed_events.py`'s `stale_queued` / `restarted`, `services.analytics.rebuild`); nothing aspirational.

Dashboard rule (D-14): `docs/architecture-dashboard.html` was visually approved by the user. Change exactly the three passages quoted in Task 1, keep the markup structure and the version marker, list every changed sentence (before and after) in the SUMMARY, and end the plan with the user's re-check of those sentences (Task 3). One more sentence in the same Q&A answer ("Not verified: the consumer's decode-time registry-outage path has no automated test ...") now understates the coverage (plan 03-21 added unit tests for that path; it is still not exercised live); it is NOT changed here because of the three-sentence limit: say so in the SUMMARY and in the Task 3 question, and let the user decide.

Live stack rules:
- Nothing is rebuilt in this plan. The only recreate is the gateway: kept at the raised budget for the suites, and restored to the defaults with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=20 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=300 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` as the final step of Task 2 (explicit values, so a host `.env` cannot keep the raised budget). Task 3 is a human look at an HTML file and does not touch the gateway. Never `down -v`, never remove a volume, never touch `tjo-*` containers (record status before and after).
- Long suites run in the background with polling and a 20 s wall-clock heartbeat; a gap above 60 s (laptop sleep) invalidates the run, which is then rerun. One pytest process at a time. `tests/visit` writes real visit rows that the live `visit-consumer` would race, so stop it (`docker compose -f infra/docker-compose.yml stop visit-consumer`) for the whole non-e2e run and start it again with `docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer` before the e2e run.
- No direct reads of `.env*` or `infra/secrets/*` (settings modules loading `.env` implicitly is fine), no package installs, no REQUIREMENTS.md edits, no requirement ticked.
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
@.planning/phases/03-event-driven-system-observability/03-25-multiprocess-rollout-SUMMARY.md
@docs/architecture.md
@tests/docs/test_phase3_docs.py
@tests/docs/test_dashboard.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `docs/architecture.md` 4f, 10.2, 10.4, 10.7, 10.9 | docs | corrected delivery semantics and per-group recovery tools; multiprocess metrics on all nine APIs; connection budget and measured peak; proof level; follow-ups |
| `docs/architecture-dashboard.html` | docs | the three passages quoted in Task 1, and nothing else |
| `README.md` Phase 3 settings table | docs | rows for `DB_POOL_SIZE` (`5`) and `DB_MAX_OVERFLOW` (`10`), Compose values in the description |
| `STALE_CLAIMS` additions, dashboard stale strings | tests | guard against the false outage claim |
| `closeout-logs/gap-non-e2e.txt`, `gap-e2e.txt` (stdout only), `gap-*.stderr.txt`, `gap-pg-samples.txt` | logs (untracked) | final gate evidence; the last stdout line of each run is pytest's summary line |
| `.planning/STATE.md` | planning | Phase 3 follow-ups updated |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a guard test rejects the false outage claim, then the docs and the dashboard are corrected to what the code does</name>
  <files>docs/architecture.md, docs/architecture-dashboard.html, README.md, tests/docs/test_phase3_docs.py, tests/docs/test_dashboard.py</files>
  <behavior>
    - New guard patterns (added first, run red against the current docs): `STALE_CLAIMS` in tests/docs/test_phase3_docs.py gains a pattern for the 10.2 sentence that says an infrastructure outage (Schema Registry, Postgres, Temporal, RabbitMQ) is never dead-lettered; `test_stale_text_removed` in tests/docs/test_dashboard.py gains the two dashboard phrasings (the consumer-groups node's "retried and never dead-lettered" and the Q&A's "the message is never dead-lettered")
    - A positive test: the Phase 3 part of architecture.md names `TransientExhausted`, `event_commit_failures_total`, `list_lost_triggers` or `restart_lost_triggers`, `replay_dlq`, and `MultiProcessCollector`; the dashboard Q&A section contains "TransientExhausted"; every `scripts/*.py` path that architecture.md section 10 names exists in the repository (nothing aspirational, W4)
    - After the edits: `pytest tests/docs` is green, including every existing name, metric, setting, topic and honesty check (nothing weakened, the version marker unchanged, "outage" still in the Q&A)
  </behavior>
  <read_first>
    - docs/architecture.md: 4f (lines 605-613, the visit reconciler paragraph that says never-started visits are only the consumer's job), 10.2 (lines 1071-1078, the consumer base paragraph), 10.4 (lines 1150-1164, visit lifecycle and backfill), 10.7 (lines 1299-1311, "Only `scheduling` was rebuilt with the new buckets"), 10.9 (lines 1423-1456, test-environment capacity, "not done", "Not verified")
    - docs/architecture-dashboard.html: line 504 (Three consumer groups node), line 680 (sweeper-retired card, Tradeoff), line 1406 (Q&A answer on failures)
    - README.md "### Phase 3 settings" table (the README test parses its rows up to "Retired in Phase 1.1")
    - tests/docs/test_phase3_docs.py (`STALE_CLAIMS`, `test_metric_names_the_docs_cite_exist_in_the_code_or_the_dashboards`, `test_every_setting_the_phase3_sections_name_exists_in_the_code`, the README settings tests)
    - tests/docs/test_dashboard.py (`test_stale_text_removed`, `test_qa_covers_phase3_themes`, `test_phase3_honesty_items_present`)
    - the five SUMMARYs of plans 03-21..03-25 (the measured numbers to quote: cap defaults, the W3 decision, the derived connection budget and the 03-23 peak, the replay script's options, resets 0 per job, the smoke figures)
    - scripts/replay_dlq.py (module docstring and options: quote them exactly) and scripts/backfill_visit_completed_events.py (docstring)
  </read_first>
  <action>
    Add the guard patterns and the positive test first and run `tests/docs` to see them fail on the current text (record it). Then edit:

    architecture.md 10.2: replace the outage sentence of the consumer-base paragraph with the current behaviour: failures are classified by one module (`libs/events/classify.py`, guarded by a test against copies): SQLSTATE classes 08, 53, 57 and 40 (for example 53300 too many connections, 57P03 cannot connect now, 40P01 deadlock), SQLAlchemy connection errors and pool timeouts, Schema Registry 408, 429 and 5xx, the Temporal outage statuses (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN, INTERNAL) and the SDK's closed-client error, broker, Mongo and network errors are retried with capped backoff; when a message has failed transiently at least 10 times over at least 15 minutes (the defaults) it is dead-lettered with `dlq.error_type` `TransientExhausted`, counted in `dlq_events_total`, kept on the DLQ topic with its original key, value and headers, and recovered with the tools that exist, per group: `visit-downstream`: automatically by the visit reconciler's lost-trigger pass after `VISIT_RECONCILE_GRACE_MINUTES`, by hand with the backfill's `--apply` (`stale_queued` / `restarted`) or `scripts/replay_dlq.py --group visit-downstream`; `analytics-projection`: `scripts/replay_dlq.py --group analytics-projection` or `services.analytics.rebuild`; `notification-trigger`: `scripts/replay_dlq.py --group notification-trigger`. Describe the replay script as it is: dry run by default, `--apply` re-produces the original key, value and original headers to `dlq.original_topic` (only a catalogue topic), filters by error type, key, event id and a limit, never moves a consumer group's offsets; a replay reaches every group on that topic, and each absorbs the duplicate through its idempotency key (D-08). A failed offset commit is retried, counted in `event_commit_failures_total` and never stops the consumer unless the error is fatal. Add one sentence that this paragraph used to claim the opposite and that the gap-closure verification found it false for asyncpg resource and connection errors (CR-01).

    4f: the visit reconciler also restarts lost triggers (completed, marker NULL, past the grace) through `ensure_downstream_started`, at worker start and on its 60 s Schedule; the sweeper stays retired. 10.4: the recovery route and the backfill's `stale_queued` / `restarted` counts and `--stale-after-minutes`. 10.7: the nine APIs run two gunicorn workers in Prometheus multiprocess mode (`MultiProcessCollector`, directory emptied at start, `child_exit` marks dead workers; an API scrape no longer carries the default process collectors), all nine were rebuilt (plans 03-24 and 03-25), and a live test that drives requests into each API measured the full count and zero counter resets per job; drop the "only scheduling was rebuilt" sentence. 10.9: the capacity item becomes current: closed-loop engines are retired, pool sizes are settings (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`), the derived Compose worst case and ceiling from plan 03-23's SUMMARY, its measured peak, and what is still not done (raising the dev `max_connections`, capping Jaeger memory); the "Not verified" list says (I6) the CR-01 classification is proven at the real `_handle_message` path at unit level with real asyncpg exception classes plus the live lost-trigger e2e, with no live injection of 53300 into the running stack, and that the registry 408/429/5xx path is unit-tested, not exercised live.

    Dashboard (D-14): change only these three passages, each to the meaning given, and nothing else:
    (1) line about 504, the "Three consumer groups" node, current final clause "an infrastructure outage is retried and never dead-lettered." becomes: an outage is retried with capped backoff and, after the cap, dead-lettered as TransientExhausted for recovery.
    (2) line about 680, the sweeper-retired card's Tradeoff: append one sentence: a dead-lettered or dropped `visit.completed` is restarted by the visit reconciler after its grace.
    (3) line about 1406, the Q&A answer to "What happens when Kafka Connect, Schema Registry, RabbitMQ or Temporal is down?", current sentence "the offset is held, the consumer seeks back and retries with capped backoff, and the message is never dead-lettered, so an outage delays an event and never drops it." becomes: the offset is held and retried with capped backoff for up to the cap (10 attempts and 15 minutes), after which the message is dead-lettered as TransientExhausted and recovered by the visit reconciler, the analytics rebuild or the DLQ replay script. Keep the word "outage" in that answer, keep the next paragraph's "Not verified" sentence unchanged (see the dashboard rule in the objective), and keep every other sentence and the version marker as they are. Record each passage before and after, word for word, in the SUMMARY under "Dashboard sentences changed".

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
  <name>Task 2: Final gap verification on the finished stack with fail-closed gate checks, follow-ups recorded, gateway budget restored as the last automated step</name>
  <precondition>Plan 03-25 is complete: its SUMMARY exists and `tail -n 1 .planning/phases/03-event-driven-system-observability/closeout-logs/gap-smoke.txt` prints `k6_exit=0`.</precondition>
  <files>.planning/STATE.md</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md (gate procedure, failure classes, connection-ceiling measurement, Semgrep triage table)
    - .planning/phases/03-event-driven-system-observability/deferred-items.md (the known provider tracer baseline failure)
    - tests/e2e/phase3_stack.py (`psql_superuser`, `compose`)
    - scripts/compose_healthcheck.sh
    - .planning/STATE.md ("Phase 3 follow-ups" under Pending Todos)
    - the SUMMARYs of plans 03-21..03-25 (what each closed, with which test; the 03-25 smoke figures and resets)
  </read_first>
  <action>
    Record `tjo-*` status. With `L=.planning/phases/03-event-driven-system-observability/closeout-logs`, run, one at a time, in the background with polling, a 20 s heartbeat file and a 15 s `pg_stat_activity` sampler writing one line per sample `<UTC ISO time> total=<sum> <usename>=<count> ...` to `$L/gap-pg-samples.txt` (superuser over the container socket, as in `tests/e2e/phase3_stack.py` `psql_superuser`; truncate the file first):
    (1) stop `visit-consumer`, then `.venv/bin/python -m pytest tests -m "not e2e" -q -rfEs --deselect tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway > "$L/gap-non-e2e.txt" 2> "$L/gap-non-e2e.stderr.txt"` (do NOT set `OTEL_SDK_DISABLED`), then `docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer`;
    (2) apply the gateway budget rule (1000 and 100000; plan 03-25 left it raised), then `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q -rA > "$L/gap-e2e.txt" 2> "$L/gap-e2e.stderr.txt"`.
    Stdout and stderr go to separate files so the last stdout line is pytest's own summary line (stderr carries Celery and OTLP exporter noise).

    The one deselected test is the known baseline failure recorded in deferred-items.md (leftover provider data, specialty ordering); list it with that reason in the SUMMARY and keep it as a STATE.md follow-up; no other test may be deselected and the gate check is never loosened. If a run's summary line reports any failed or error test, classify each one in the SUMMARY (macOS fork SIGTRAP exit -5, environmental, or new; a `TooManyConnectionsError` or any failure in a file touched by plans 03-21..03-25 is new and must be fixed, with the fix committed, or reported as a blocker), keep that log as `gap-non-e2e-attemptN.txt` / `gap-e2e-attemptN.txt`, and rerun the whole run; at most two reruns per run; if the final run is still not clean, stop and report (the verify fails, by design). Record the connection peak from the sample file next to the gate's 304 and the 03-23 peak.

    Gap checklist (each with its evidence in the SUMMARY): CR-01 classification (03-21 tests, unit level, I6), CR-01 recovery (`test_lost_visit_trigger_is_recovered_by_the_reconciler` PASSED in the e2e log; the replay script tests of 03-22), WR-02, WR-03, WR-07 and IN-01 (03-21 tests incl. the AST guard), WR-05 (03-23 test, the 03-23 peak and this run's peak), CR-02 (the nine `test_http_request_counters_have_no_resets_and_never_decrease` cases PASSED; 03-25's smoke resets file), smoke (03-25 Task 3: k6_exit=0 and nine zero resets), docs (Task 1). Run Semgrep over the changed paths: `semgrep --config p/python --config p/security-audit --metrics=off libs/events libs/db libs/observability libs/config.py services/visit services/analytics/consumer.py services/notification/triggers.py services/gateway/app.py scripts/backfill_visit_completed_events.py scripts/replay_dlq.py`; triage and record (the Dockerfile `missing-user` findings stay user-accepted).

    Update `.planning/STATE.md` Phase 3 follow-ups: mark done the engine disposal and the gateway restore; keep open WR-01 (reminder reschedule-signal race, needs `workflow.patched` plus a replay test), WR-04 (debezium role privileges, Connect REST auth, FileConfigProvider allowed paths; PHI-relevant), WR-06 (backfill can send follow-ups for old visits; a replay keeps the event id and is absorbed), IN-02..IN-06, the deselected provider tracer test, raising the dev `max_connections`, capping Jaeger memory, the deferred k6 profiles, and the dashboard's understated decode-time "Not verified" sentence unless the user asks for it in Task 3. Do not edit REQUIREMENTS.md.

    LAST automated step of the gap closure: restore the gateway budget with the explicit-defaults command from the objective, wait for `healthy`, and confirm `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` prints 20 and 300. Run `scripts/compose_healthcheck.sh` and record `tjo-*` status again. Nothing runs against the gateway after this step.
  </action>
  <verify>
    <automated>L=.planning/phases/03-event-driven-system-observability/closeout-logs && tail -n 1 "$L/gap-non-e2e.txt" | grep -qE "[0-9]+ passed" && ! tail -n 1 "$L/gap-non-e2e.txt" | grep -qE "[0-9]+ (failed|error)" && tail -n 1 "$L/gap-e2e.txt" | grep -qE "[0-9]+ passed" && ! tail -n 1 "$L/gap-e2e.txt" | grep -qE "[0-9]+ (failed|error)" && grep -q "PASSED tests/e2e/test_phase3_visit_downstream.py::test_lost_visit_trigger_is_recovered_by_the_reconciler" "$L/gap-e2e.txt" && .venv/bin/python -c "import sys; n=sum(1 for l in open(sys.argv[1]).read().splitlines() if l.startswith('PASSED tests/e2e/test_phase3_metrics.py::test_http_request_counters_have_no_resets_and_never_decrease[')); print(n); sys.exit(0 if n == 9 else 1)" "$L/gap-e2e.txt" && .venv/bin/python -c "import re,sys; t=[int(v) for v in re.findall(r'total=(\d+)', open(sys.argv[1]).read())]; print(len(t), max(t) if t else None); sys.exit(0 if len(t) >= 40 and max(t) < 270 else 1)" "$L/gap-pg-samples.txt" && test "$(docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE | tr -d '\r')" = "20" && test "$(docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE | tr -d '\r')" = "300" && .venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>A gate log is missing, its last line is not a pytest summary with passed tests, or that summary line reports any failed or error test (the known provider tracer failure is deselected by node id, never tolerated by the check); the lost-trigger e2e or any of the nine no-reset cases is not listed as PASSED; the connection sample file has fewer than 40 samples or a peak of 270 or more; the gateway still runs a raised budget; or the docs suite fails.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY has the gap checklist with evidence per item, the deselected test with its reason, the failure classification of any earlier attempt, the connection peak, the Semgrep triage and the dashboard sentence list from Task 1
    - the gateway prints 20 and 300; `scripts/compose_healthcheck.sh` is OK; tjo-* status lines before and after are identical apart from uptime
    - `git diff HEAD -- .planning/REQUIREMENTS.md` is empty
  </acceptance_criteria>
  <done>Both verification gaps are closed with fail-closed evidence on the finished stack, the follow-ups are recorded, and the gateway is back on its defaults.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 3: User re-check of exactly the three changed dashboard sentences (D-14)</name>
  <what-built>Plans 03-21..03-25 closed both verification gaps and Task 2 recorded the evidence. You approved the architecture dashboard before; per D-14 Task 1 changed exactly three passages in `docs/architecture-dashboard.html` and nothing else: (1) the "Three consumer groups" node's last clause, (2) one sentence appended to the sweeper-retired card's Tradeoff, (3) the outage sentence of the Q&A answer "What happens when Kafka Connect, Schema Registry, RabbitMQ or Temporal is down?". The before and after text of each is listed in the 03-26 SUMMARY under "Dashboard sentences changed".</what-built>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-26-gap-closeout-SUMMARY.md ("Dashboard sentences changed")
  </read_first>
  <how-to-verify>
    1. Open `docs/architecture-dashboard.html` offline in a browser and read the three passages listed in the SUMMARY; confirm each reads well and says what the code does (outages retried up to a cap, then dead-lettered as TransientExhausted and recovered by the visit reconciler, the analytics rebuild or the DLQ replay script).
    2. Optional, one line: the next sentence of the same Q&A answer ("Not verified: the consumer's decode-time registry-outage path has no automated test ...") now understates the coverage (unit tests were added in plan 03-21; still no live registry outage). It was left unchanged because of the three-sentence limit. Reply "leave it" or "update it".
  </how-to-verify>
  <resume-signal>Reply "approved" (and "leave it" or "update it" for step 2), or quote the sentence that reads wrong and what you would change</resume-signal>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs/test_dashboard.py tests/docs/test_phase3_docs.py -q</automated>
    <fails_when>The docs tests exit non-zero, meaning the dashboard or the docs changed after Task 1 in a way that breaks a guard, a needle or the version marker.</fails_when>
  </verify>
  <acceptance_criteria>
    - the user's reply for steps 1 and 2 is recorded in the SUMMARY under "Human sign-off"
    - any change the user asks for is applied to the named sentence only, with `tests/docs` rerun green, and recorded; "update it" is applied as a fourth sentence only with that explicit reply
  </acceptance_criteria>
  <done>The user has re-checked the three changed sentences and the reply is recorded.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| public clients -> gateway | the rate budget protects the published port |
| documents -> reviewers | the docs and the dashboard are graded statements about guarantees |
| gate logs -> repository | full-suite output may echo a dev credential in a traceback |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-82 | Elevation of Privilege / Denial of Service | gateway left on the raised test budget (1000 / 100000 per minute) | medium | mitigate | Explicit restore to 20 and 300 as the last automated step, verified with `printenv` in the verify; the human checkpoint after it does not use the gateway |
| T-3-83 | Repudiation | documents overstating delivery guarantees (the CR-01 doc defect) or naming tools that do not exist | medium | mitigate | Guard tests for the false claim in docs and dashboard; positive checks that the new mechanisms are named; a check that every script path section 10 names exists; names cross-checked against the code by the existing docs tests; the user re-checks the three dashboard sentences |
| T-3-84 | Information Disclosure | gate logs (`closeout-logs/gap-*`) | low | mitigate | The directory stays untracked (never committed), as in plan 03-20; the SUMMARY quotes summary lines and test ids only |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs tests/loadtest -q`
- the two full-suite stdout logs under `closeout-logs/gap-non-e2e.txt` and `gap-e2e.txt` end in a pytest summary line with no failed or error test (the Task 2 verify), with any earlier attempt classified
- `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` prints 20 and 300
- the user's re-check of the three dashboard sentences recorded (Task 3)
</verification>

<success_criteria>
- Verification gaps 1 and 2 closed with fail-closed live evidence; the docs and the dashboard state only what the code does, naming only tools that exist
- Smoke result from plan 03-25 cited; sustained, burst and contention still deferred
- Gateway on its default budget; follow-ups recorded; the user re-checked the three changed dashboard sentences
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-26-gap-closeout-SUMMARY.md` when done
</output>
