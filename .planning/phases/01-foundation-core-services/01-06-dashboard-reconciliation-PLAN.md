---
phase: 01-foundation-core-services
plan: 06
type: execute
wave: 6
depends_on: ["01-03", "01-05"]
files_modified:
  - docs/architecture-dashboard.html
autonomous: true
requirements: [DASH-01, DASH-02]

estimate:
  tokens: 40000
  raw_tokens: 26000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "docs/architecture-dashboard.html opens standalone in a browser (no server, no external network dependency) and renders (DASH-01)"
    - "The #architecture section's system diagram reflects the ACTUAL Phase-1 build: Postgres system-of-record, MongoDB audit trail, Redis (cache + jti denylist), and the Kafka/Temporal/RabbitMQ/observability scaffolding-only (not-yet-wired) status (DASH-01)"
    - "The #decisions section documents each major Phase-1 technology/design decision WITH alternatives considered and the reasoning (DASH-02)"
    - "The dashboard records the two flagged Phase-1 decisions as delivered: (a) access+refresh JWT + Redis jti-denylist auth model, and (b) availability-templates-now / provider_slots-exclusion-constraint-designed-now (Phase-2 double-booking forward-compatibility)"
    - "The dashboard notes the Temporal auto-setup image as a documented non-production tradeoff (per CLAUDE.md), and the users-vs-patients separation assumption"
    - { statement: "Every decision card that existed before this plan and drifted from the actual Phase-1 build is corrected — the reconciliation updates only where drift exists, it does not rebuild the file from scratch", verification: backstop }
  artifacts:
    - path: docs/architecture-dashboard.html
      provides: "Self-contained Architecture & Decision dashboard reconciled to Phase-1's actual build (DASH-01 diagram + DASH-02 decisions)"
      contains: "denylist"
  key_links:
    - from: docs/architecture-dashboard.html
      to: .planning/phases/01-foundation-core-services/01-03-SUMMARY.md
      via: "the auth-model decision card matches the delivered JWT claim shape + Redis denylist from Plan 03"
      pattern: "refresh|denylist|jti"
    - from: docs/architecture-dashboard.html
      to: .planning/phases/01-foundation-core-services/01-02-SUMMARY.md
      via: "the slot-model decision card matches the delivered availability-templates + provider_slots exclusion constraint from Plan 02"
      pattern: "exclusion|availability|slot"
  prohibitions:
    - "The dashboard MUST NOT be rebuilt from scratch — the existing 710-line file is substantially complete (RESEARCH Open Question 2 recommends verify/extend, not rebuild); only drift is corrected"
    - "Decision cards MUST NOT present a technology choice without its alternatives-considered and reasoning — DASH-02 requires the tradeoff reasoning, not just the pick"
    - "The dashboard MUST remain self-contained — no external CSS/JS/CDN/font dependency that breaks offline standalone opening (DASH-01)"
---

<objective>
Reconcile the EXISTING self-contained Architecture & Decision dashboard (docs/architecture-dashboard.html, 710 lines, already present) against Phase 1's ACTUAL build decisions, correcting drift where the delivered auth/slot/stack decisions differ from what the dashboard currently states. Verify — do not rebuild.

Purpose: Satisfies DASH-01 (self-contained HTML architecture diagram) and DASH-02 (each major technology/design decision documented with alternatives + reasoning). The dashboard already exists and appears substantially complete; per RESEARCH Open Question 2, the correct action is a verification-and-reconciliation pass, not authoring from scratch (which would waste effort and risk regressing a good artifact). This plan runs after the two consequential decisions are locked in code (Plan 03 auth model, Plan 02/05 slot model) so the dashboard can be reconciled against reality.

Output: an updated docs/architecture-dashboard.html whose #architecture diagram and #decisions cards match the delivered Phase-1 system, including the two flagged decisions and the documented Temporal-auto-setup tradeoff.
</objective>

<execution_context>
@~/.claude/gsd-core/workflows/execute-plan.md
@~/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.claude/CLAUDE.md
@.planning/phases/01-foundation-core-services/01-RESEARCH.md
@.planning/phases/01-foundation-core-services/01-02-SUMMARY.md
@.planning/phases/01-foundation-core-services/01-03-SUMMARY.md
@.planning/phases/01-foundation-core-services/01-05-SUMMARY.md
@docs/architecture-dashboard.html

<interfaces>
<!-- The existing dashboard already has these section anchors (verified at plan time): -->
#overview (01 What SmartHealth Part A is)
#architecture (02 System architecture) — the DASH-01 diagram lives here
#decisions (03 Technology decisions) — the DASH-02 decision cards live here
#patterns (04 responsibility split & consistency patterns)
#flows (05 Core flows)
#qa (06 Anticipated evaluation questions)
#roadmap (07 Roadmap & how this dashboard grows)
<!-- Reconcile #architecture and #decisions against the SUMMARYs; leave phase-2/3-forward sections as-is unless they contradict Phase-1 reality. -->
</interfaces>
</context>

<artifacts_this_phase_produces>
- Updated `docs/architecture-dashboard.html` (#architecture diagram + #decisions cards reconciled to Phase-1 build; Temporal-auto-setup tradeoff + users/patients separation noted)
</artifacts_this_phase_produces>

<tasks>

<task type="auto">
  <name>Task 1: Reconcile the architecture diagram and decision cards against the delivered build</name>
  <read_first>docs/architecture-dashboard.html (#architecture and #decisions sections — read the current content before editing), .planning/phases/01-foundation-core-services/01-02-SUMMARY.md + 01-03-SUMMARY.md + 01-05-SUMMARY.md (the delivered decisions), .planning/phases/01-foundation-core-services/01-RESEARCH.md (The Two Flagged Decisions + Open Question 2 verify-not-rebuild)</read_first>
  <precondition>docs/architecture-dashboard.html exists and Plans 02/03/05 SUMMARYs are present (their decisions are what the dashboard is reconciled against)</precondition>
  <files>docs/architecture-dashboard.html</files>
  <action>Read the existing #architecture and #decisions sections. Compare each claim against the delivered build recorded in the Plan 02/03/05 SUMMARYs and correct drift IN PLACE (do not rewrite the whole file). Ensure the #architecture diagram shows: PostgreSQL as system-of-record (users/patients/providers/clinics/departments/availability_templates + empty provider_slots), MongoDB as append-only audit trail, Redis as cache + jti denylist, and the Kafka/Schema-Registry/Temporal/RabbitMQ/Prometheus/Grafana/Jaeger stack marked scaffolding-only (running with health checks but NOT wired into app code in Phase 1). Ensure #decisions contains cards — each with alternatives-considered AND reasoning (DASH-02) — for at minimum: NoSQL/MongoDB-for-audit choice, Postgres system-of-record, Redis role, Kafka+RabbitMQ split, Temporal (including the auto-setup vs. production-server tradeoff labeled non-prod per CLAUDE.md), the auth model (access+refresh JWT + Redis jti-denylist, alternatives: access-only / no-revocation-list), and the availability/slot model (weekly templates now + provider_slots exclusion constraint designed now for Phase-2 double-booking, alternative: generate concrete slots in Phase 1). Add a brief note on the users-vs-patients separation assumption. Keep the file self-contained (no new external CDN/font/JS dependency).</action>
  <acceptance_criteria>
    - the file still opens as a single self-contained HTML document (no new external resource references introduced)
    - #decisions contains a card mentioning "denylist" (auth model reconciled) and a card mentioning the slot/exclusion decision
    - the Temporal decision card notes the auto-setup non-production tradeoff
    - the diagram distinguishes wired (Postgres/Mongo/Redis) from scaffolding-only (Kafka/Temporal/RabbitMQ/observability) services
  </acceptance_criteria>
  <verify>
    <automated>grep -c "denylist" docs/architecture-dashboard.html; grep -icE 'exclusion|availability template|provider_slots' docs/architecture-dashboard.html; grep -icE 'auto-setup|non-production|not for production' docs/architecture-dashboard.html</automated>
    <fails_when>the denylist count is 0 (auth model not reconciled), OR the exclusion/availability count is 0 (slot decision not documented), OR the auto-setup count is 0 (Temporal tradeoff not noted)</fails_when>
    <automated>grep -icE 'https?://[^"]*\.(css|js)"|cdn\.|googleapis|jsdelivr|unpkg' docs/architecture-dashboard.html</automated>
    <fails_when>the count is greater than 0 — a new external CSS/JS/CDN dependency was introduced, breaking DASH-01 self-containment</fails_when>
  </verify>
  <done>The #architecture diagram and #decisions cards match the delivered Phase-1 build (auth model, slot model, Temporal tradeoff, wired-vs-scaffolding distinction), the file remains self-contained, and only drift was corrected.</done>
</task>

<task type="auto">
  <name>Task 2: Serve the dashboard for standalone verification (DASH-01/02 human-check)</name>
  <read_first>docs/architecture-dashboard.html (Task 1 output)</read_first>
  <files>docs/architecture-dashboard.html</files>
  <action>Serve the reconciled dashboard for review: start a static file server for the docs directory in the background (`python -m http.server` rooted at docs/, capture the port) so the file can be opened at a URL, and confirm the served page returns HTTP 200 and contains the "Architecture &amp; Decision Dashboard" title. This is an automated serve-and-smoke step; the human visual confirmation is captured via the human-check block below (harvested at end-of-phase per human_verify_mode=end-of-phase — this plan does NOT halt mid-flight).</action>
  <acceptance_criteria>
    - the static server returns HTTP 200 for architecture-dashboard.html
    - the served response body contains the dashboard title string
  </acceptance_criteria>
  <verify>
    <automated>python -m http.server 8099 --directory docs >/tmp/dashsrv.log 2>&1 &  SRV=$!; sleep 1; node -e "fetch('http://localhost:8099/architecture-dashboard.html').then(r=>r.text().then(t=>{if(!r.ok||!t.includes('Architecture'))process.exit(1)})).catch(()=>process.exit(1))"; RC=$?; kill $SRV 2>/dev/null; exit $RC</automated>
    <fails_when>the served response is not HTTP 200, or the body does not contain "Architecture", or the node fetch throws (server did not start)</fails_when>
    <human-check>
      Open docs/architecture-dashboard.html in a browser (or visit the served URL). Confirm (DASH-01) the #architecture system diagram renders and reflects the Phase-1 build (Postgres system-of-record, MongoDB audit, Redis cache+denylist, Kafka/Temporal/RabbitMQ/observability marked scaffolding-only). Confirm (DASH-02) the #decisions section documents each major technology/design decision with alternatives and reasoning, including the access+refresh JWT + Redis-denylist auth model, the availability-templates + provider_slots-exclusion-constraint slot model, and the Temporal auto-setup non-production tradeoff. The page opens standalone with no broken/missing external resources.
    </human-check>
  </verify>
  <done>The reconciled dashboard serves at HTTP 200 with its title, is queued for the end-of-phase human visual confirmation of DASH-01 (diagram) and DASH-02 (decisions with alternatives/reasoning), and opens self-contained.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| static docs → developer/reviewer browser | The dashboard is a static, self-contained artifact; it contains no live system data, no PHI, and no secrets |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-06-01 | Information Disclosure | Dashboard accidentally embeds real credentials/PHI | low | mitigate | The dashboard is architecture/decision documentation only — no live data, no secrets, no PHI embedded; reconciliation adds decision text, not runtime data |
| T-06-02 | Tampering | External CDN/JS dependency injected into a "self-contained" file | low | mitigate | A verify grep asserts no new external CSS/JS/CDN reference is introduced (DASH-01 self-containment) |
</threat_model>

<verification>
- Dashboard is self-contained (no new external resource refs); #decisions reconciled (denylist + slot/exclusion + Temporal auto-setup tradeoff present); wired-vs-scaffolding distinction in the diagram.
- Served page returns HTTP 200 with the dashboard title; end-of-phase human-check confirms DASH-01 diagram and DASH-02 decisions-with-reasoning render correctly.
</verification>

<success_criteria>
DASH-01 and DASH-02 satisfied: the self-contained HTML dashboard shows an architecture diagram matching Phase 1's actual build and documents each major technology/design decision with alternatives considered and reasoning — reconciled to reality, including the two flagged decisions and the Temporal auto-setup tradeoff.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-06-SUMMARY.md` when done. List which decision cards were corrected for drift versus already accurate, and confirm the file remains self-contained.
</output>