---
phase: 01-foundation-core-services
plan: 05
type: execute
wave: 5
depends_on: ["01-04"]
files_modified:
  - services/identity/repository/people_repo.py
  - services/identity/api/patients.py
  - services/identity/api/providers.py
  - services/identity/api/availability.py
  - services/identity/app.py
  - tests/test_patients.py
  - tests/test_providers.py
  - tests/test_availability.py
autonomous: true
requirements: [PPL-01, PPL-02, PPL-03, PPL-04]

estimate:
  tokens: 72000
  raw_tokens: 46000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Front-desk or admin can register a patient (POST /api/patients) and manage the profile; a patient-role token cannot register a patient on someone else's behalf via this staff route (PPL-01)"
    - "Admin can register a provider (POST /api/providers) with one or more specialties (PPL-02)"
    - "Provider or admin can configure availability windows (POST /api/providers/{provider_id}/availability) creating availability_templates with day_of_week/start/end/slot_duration/effective dates (PPL-03)"
    - "Users can retrieve patient and provider records (GET) reflecting the latest committed state — read-after-write consistency, no stale cache (PPL-04)"
    - "A provider is created under a department, and its availability templates carry provider_id + department_id — providers/availability are clinic/department scoped (ORG-03 people half)"
    - "Every patient/provider/availability mutation writes a shared write_audit entry with NO PHI values in the audit changes field or logs (IAM-04 discipline extended to PHI-heavy patient data)"
    - { statement: "Registering a patient then immediately reading it back returns the just-committed record (PPL-04 read-after-write)", verification: explicit }
    - { statement: "Creating a provider with zero specialties vs one vs many all have defined outcomes — empty specialties list is handled deterministically (rejected or allowed per a stated rule)", verification: backstop }
    - { statement: "An availability template with end_time not after start_time, or day_of_week outside 0-6, is rejected (invalid-window edge)", verification: explicit }
    - { statement: "A patient auth account (users, IAM-01) and a patient operational profile (patients, PPL-01) are distinct — a front-desk-created patient profile need not have a linked user_id", verification: backstop }
  artifacts:
    - path: services/identity/api/patients.py
      provides: "Patient CRUD router (front-desk/admin), PHI-safe, audited"
      exports: ["router"]
    - path: services/identity/api/providers.py
      provides: "Provider CRUD router with specialties (admin), audited"
      exports: ["router"]
    - path: services/identity/api/availability.py
      provides: "Availability-template router (provider/admin), audited"
      exports: ["router"]
    - path: services/identity/repository/people_repo.py
      provides: "Async repository for patients/providers/specialties/availability — clinic/department-scope-aware"
  key_links:
    - from: services/identity/api/providers.py
      to: services/identity/repository/models.py
      via: "provider ↔ specialties many-to-many via provider_specialties; provider.department_id FK (ORG-03)"
      pattern: "specialt"
    - from: services/identity/api/availability.py
      to: services/identity/repository/models.py
      via: "AvailabilityTemplate rows reference provider_id + department_id and validate day_of_week/time window"
      pattern: "availability_templates|AvailabilityTemplate"
    - from: services/identity/api/patients.py
      to: libs/audit/writer.py
      via: "patient mutations call shared write_audit with field-name-only changes (no PHI values)"
      pattern: "write_audit"
  prohibitions:
    - "Patient PHI values (name, dob, contact) MUST NOT be written into audit change-values, log events, or OTel span attributes — record identifiers and field names only"
    - "Patient/provider repository reads MUST NOT expose an unscoped-by-clinic path for staff-scoped access (Pitfall 1 cross-clinic enumeration)"
    - "Availability template times MUST NOT be interpreted without the clinic timezone — do not silently assume UTC/server-local (Pitfall 3); Phase 2 slot generation inherits this"
    - "Provider registration MUST NOT be relaxed below admin-only (PPL-02); patient registration is front-desk/admin only (PPL-01) — a patient-role token must not self-register a staff-managed profile via the staff route"
---

<objective>
Build patient, provider (with specialties), and provider-availability CRUD on the proven auth/RBAC/audit/PHI-safe skeleton — the highest-PHI-density expansion of the phase.

Purpose: Wave-5 expansion (sequenced after Plan 04 because both mount routers on the shared app.py). Satisfies PPL-01 (front-desk/admin patient management), PPL-02 (admin provider registration with specialties), PPL-03 (availability windows as templates), and PPL-04 (consistent read-after-write). It reuses require_role, write_audit, get_current_user, get_db — no new architecture. Availability is modeled as recurring weekly TEMPLATES only (no concrete slot generation — that is Phase 2), per the most-consequential flagged decision.

Output: people_repo (scope-aware), patients + providers + availability routers (audited, PHI-safe), integration tests.

**Flagged assumptions surfaced here (no CONTEXT.md locked them — following RESEARCH):**
- **Availability = weekly templates only in Phase 1** (RESEARCH Decision 2, the most consequential decision). NO concrete bookable slot rows are generated this phase; provider_slots stays empty. Phase 2 generates slots from these templates against the already-constrained provider_slots table (Plan 02).
- **users vs patients separation** (RESEARCH Open Question 1): a patient profile (patients) is distinct from an auth identity (users); patients.user_id is nullable.
- **Availability times resolve against clinic.timezone** (RESEARCH Pitfall 3), not UTC/server-local.
</objective>

<execution_context>
@~/.claude/gsd-core/workflows/execute-plan.md
@~/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.claude/CLAUDE.md
@.planning/phases/01-foundation-core-services/01-RESEARCH.md
@.planning/phases/01-foundation-core-services/01-03-SUMMARY.md
@.planning/phases/01-foundation-core-services/01-04-SUMMARY.md

<interfaces>
<!-- Consumes from Plans 02+03+04: -->
smarthealth.db: get_db
services.identity.domain.enums: Role
services.identity.repository.models: Patient (id, user_id nullable FK, clinic_id, PHI cols), Provider (id, department_id FK), Specialty, provider_specialties, AvailabilityTemplate (provider_id, department_id, day_of_week, start_time, end_time, slot_duration_minutes, effective_from, effective_to)
services.identity.security.rbac: require_role(*roles)
services.identity.security.current_user: get_current_user -> AuthenticatedUser
libs.audit.writer: write_audit(...)  (changes must be field-names-only)
services.identity.app: app (FastAPI) — mount the new routers; import Plan 04's already-mounted routers, add these alongside them (this plan edits app.py after Plan 04)
</interfaces>
</context>

<artifacts_this_phase_produces>
- Endpoints: POST/GET /api/patients, GET /api/patients/{id}, POST/GET /api/providers, GET /api/providers/{id}, POST/GET /api/providers/{provider_id}/availability
- `services/identity/repository/people_repo.py` → create_patient, get_patient, list_patients, create_provider (with specialties), get_provider, list_providers, create_availability_template, list_availability
- `services/identity/api/patients.py`, `providers.py`, `availability.py` → routers
- Pydantic schemas: PatientCreate/Out, ProviderCreate (with specialties[]) /Out, AvailabilityTemplateCreate/Out (with day_of_week/time-window validators)
</artifacts_this_phase_produces>

<tasks>

<task type="auto">
  <name>Task 1: Patient + Provider repository and routers (specialties, RBAC, PHI-safe, audited)</name>
  <read_first>services/identity/security/rbac.py + libs/audit/writer.py + services/identity/security/current_user.py (Plan 03), services/identity/repository/models.py (Patient, Provider, Specialty, provider_specialties from Plan 02), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Data model section + PHI-Safe logging + Pitfall 1 clinic scope + Open Question 1 users/patients)</read_first>
  <precondition>Plan 04's app.py mounts its clinic/department routers (this plan appends to the same app.py after Plan 04) and Plan 03's shared libs import cleanly</precondition>
  <reversibility rating="costly">The patients-vs-users separation and provider↔specialties relation are surfaced through the API response shapes; changing them after clients/tests depend on the payloads is a coordinated multi-site change (mitigated by keeping it behind Pydantic response schemas).</reversibility>
  <files>services/identity/repository/people_repo.py, services/identity/api/patients.py, services/identity/api/providers.py</files>
  <action>Create services/identity/repository/people_repo.py with async, clinic-scope-aware methods: create_patient(db, clinic_id, profile, user_id=None), get_patient(db, patient_id, clinic_id), list_patients(db, clinic_id); create_provider(db, department_id, profile, specialty_ids), get_provider(db, provider_id), list_providers(db, department_id=None), and a helper to attach specialties via provider_specialties (PPL-02 one-or-more). Create services/identity/api/patients.py: POST/GET /api/patients guarded by Depends(require_role(Role.FRONT_DESK, Role.ADMIN)); PatientCreate carries operational profile fields (name/dob/contact = PHI); after each mutation write_audit(action patient.create, entity_type patient, changes=field-names-only — NEVER PHI values). Create services/identity/api/providers.py: POST/GET /api/providers guarded by Depends(require_role(Role.ADMIN)); ProviderCreate carries department_id + specialties[] (>=1); audit provider.create. Return 403 for wrong role, 404 for missing scope target. Do NOT log or audit PHI values.</action>
  <acceptance_criteria>
    - `grep -c "require_role" services/identity/api/patients.py services/identity/api/providers.py` shows the guard on both
    - patient routes allow FRONT_DESK and ADMIN; provider routes allow ADMIN only (grep the Role args)
    - people_repo patient methods take clinic_id (no id-only patient read)
    - provider creation attaches one-or-more specialties (grep confirms specialty handling)
  </acceptance_criteria>
  <verify>
    <automated>python -c "from services.identity.api import patients, providers; assert hasattr(patients,'router') and hasattr(providers,'router')"</automated>
    <fails_when>the import raises or a router attribute is missing — the routers were not defined</fails_when>
    <automated>grep -c "require_role" services/identity/api/patients.py; grep -c "write_audit" services/identity/api/patients.py</automated>
    <fails_when>either count is 0 — the patient router is missing its RBAC guard or its audit write</fails_when>
  </verify>
  <done>Patient and provider routers exist with correct RBAC scoping, specialties support, PHI-safe auditing, and clinic/department scoping in the repository.</done>
</task>

<task type="auto">
  <name>Task 2: Availability-template router (weekly templates, timezone-aware, validated) + app mount</name>
  <read_first>services/identity/repository/models.py (AvailabilityTemplate + Clinic.timezone from Plan 02), services/identity/repository/people_repo.py (Task 1), services/identity/app.py (Plan 04 mounted routers — append here), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Decision 2 availability templates + Pitfall 3 timezone)</read_first>
  <reversibility rating="costly">The availability_templates API surface (request/response shape) is exposed here; the underlying schema + timezone-resolution convention that Phase 2 slot-generation reads was the one-way decision already confirmed at Plan 02's checkpoint. Changing this API surface after clients/tests depend on it is a coordinated multi-site change, mitigated by keeping it behind Pydantic response schemas — hence costly, not one-way (the irreversible schema door is upstream in Plan 02).</reversibility>
  <files>services/identity/api/availability.py, services/identity/repository/people_repo.py, services/identity/app.py</files>
  <action>Create services/identity/api/availability.py: POST /api/providers/{provider_id}/availability and GET list, guarded by Depends(require_role(Role.PROVIDER, Role.ADMIN)). AvailabilityTemplateCreate (Pydantic v2) validates day_of_week in 0-6, end_time strictly after start_time, slot_duration_minutes > 0, effective_from <= effective_to when both present — reject invalid windows with 422. Persist availability_templates rows carrying provider_id + department_id (derived from the provider's department — ORG-03 scoping). Document in the response/schema that times resolve against the clinic's timezone (Pitfall 3) — do NOT assume UTC. Do NOT generate concrete provider_slots rows (Phase 2 concern). Add create_availability_template + list_availability to people_repo. Audit availability.create. Finally, edit services/identity/app.py to include the patients, providers, and availability routers alongside Plan 04's clinic/department routers (append include_router calls — preserve the existing mounts).</action>
  <acceptance_criteria>
    - the app mounts /api/providers/{provider_id}/availability plus /api/patients and /api/providers (route set includes all)
    - an availability template with end_time <= start_time or day_of_week=7 is rejected 422
    - no provider_slots INSERT occurs from this router (grep confirms availability.py does not write provider_slots)
    - the availability router is guarded to provider/admin
  </acceptance_criteria>
  <verify>
    <automated>python -c "from services.identity.app import app; p={r.path for r in app.routes}; assert any('availability' in x for x in p) and '/api/patients' in p, sorted(p)"</automated>
    <fails_when>the import raises, or the availability/patients routes are absent — routers not mounted or Plan 04's mounts were clobbered</fails_when>
    <automated>grep -cE 'provider_slots|ProviderSlot' services/identity/api/availability.py</automated>
    <fails_when>the count is greater than 0 — the availability router is generating/writing concrete slots, which is a Phase 2 concern (RESEARCH Decision 2)</fails_when>
  </verify>
  <done>Availability templates (weekly, timezone-aware, validated) can be configured for a provider; no concrete slots are generated; all people routers are mounted alongside Plan 04's.</done>
</task>

<task type="auto">
  <name>Task 3: Patient/Provider/Availability integration tests (PPL-01/02/03/04)</name>
  <read_first>services/identity/api/patients.py + providers.py + availability.py (Tasks 1-2), tests/conftest.py (client + role-scoped JWT helpers), tests/test_audit.py (PHI-safe assertion pattern from Plan 03)</read_first>
  <files>tests/test_patients.py, tests/test_providers.py, tests/test_availability.py</files>
  <action>Create tests/test_patients.py: test_create_patient (front-desk registers a patient); test_read_after_write_consistency (create then GET returns committed record — PPL-04); test_patient_role_forbidden (a patient-role token on the staff register route → 403); test_no_phi_in_patient_audit_or_logs (a patient mutation's audit changes + captured logs contain no synthetic PHI value). Create tests/test_providers.py: test_create_provider_with_specialties (admin creates provider with >=1 specialty — PPL-02); test_provider_zero_specialties_defined_outcome (asserted deterministic outcome for empty specialties); test_provider_scoped_to_department (ORG-03). Create tests/test_availability.py: test_create_availability_template (provider/admin creates a valid weekly template — PPL-03); test_invalid_window_rejected (end<=start and day_of_week=7 → 422); test_no_concrete_slots_generated (after creating a template, provider_slots remains empty — Decision 2). Use synthetic PHI in fixtures.</action>
  <acceptance_criteria>
    - `pytest tests/test_patients.py tests/test_providers.py tests/test_availability.py -x` passes every case
    - read-after-write returns the just-created patient
    - patient-role token is rejected 403 on the staff register route
    - no PHI value appears in patient audit/logs
    - creating an availability template leaves provider_slots empty
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_patients.py tests/test_providers.py tests/test_availability.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — patient/provider/availability CRUD, RBAC, read-after-write, PHI safety, or the no-slots-generated invariant failed</fails_when>
  </verify>
  <done>PPL-01/02/03/04 proven: patient management (front-desk/admin), provider registration with specialties (admin), availability templates (provider/admin, validated, no concrete slots), and read-after-write consistency — all PHI-safe and audited.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| client → API | Untrusted patient/provider/availability input; role-scoped mutations validated server-side |
| API → PostgreSQL | PHI-bearing patient writes; clinic scoping at the query layer |
| API → MongoDB audit + logs/spans | Must carry field names and identifiers, never PHI values |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-05-01 | Information Disclosure | Patient PHI leaking into audit change-values / logs / span attributes | high | mitigate | Audit changes stores field names only; PHI-safe structlog convention; body capture disabled; test_no_phi_in_patient_audit_or_logs asserts absence |
| T-05-02 | Elevation of Privilege | Non-staff registers a patient / non-admin registers a provider | high | mitigate | require_role(FRONT_DESK, ADMIN) on patient routes; require_role(ADMIN) on provider routes; tests assert 403 |
| T-05-03 | Elevation of Privilege | Cross-clinic patient/provider enumeration | high | mitigate | people_repo methods require clinic/department scope; no id-only staff read path (Pitfall 1) |
| T-05-04 | Tampering | Ambiguous availability times corrupt Phase-2 slot generation | medium | mitigate | Times resolve against clinic.timezone (Pitfall 3); window validators reject invalid ranges; no naive-UTC assumption |
| T-05-05 | Repudiation | Unaudited patient/provider/availability mutation | medium | mitigate | Shared write_audit on every mutation; tests assert audit presence |
</threat_model>

<verification>
- App mounts patient/provider/availability routers alongside Plan 04's; correct RBAC scoping; PHI-safe auditing.
- `pytest tests/test_patients.py tests/test_providers.py tests/test_availability.py -x` passes: CRUD, specialties, availability validation, read-after-write, PHI safety, no-slots-generated.
</verification>

<success_criteria>
PPL-01/02/03/04 satisfied: front-desk/admin patient management, admin provider registration with specialties, provider/admin availability-template configuration (weekly, timezone-aware, no concrete slots), and consistent read-after-write — all clinic/department-scoped, PHI-safe, and audited.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-05-SUMMARY.md` when done. Confirm availability is templates-only (no slots generated) and record the availability_templates API contract Phase 2 slot-generation will read.
</output>
