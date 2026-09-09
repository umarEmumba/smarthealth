---
phase: 01-foundation-core-services
plan: 04
type: execute
wave: 4
depends_on: ["01-03"]
files_modified:
  - services/identity/repository/org_repo.py
  - services/identity/api/clinics.py
  - services/identity/api/departments.py
  - services/identity/app.py
  - tests/test_clinics.py
  - tests/test_departments.py
autonomous: true
requirements: [ORG-01, ORG-02, ORG-03]

estimate:
  tokens: 58000
  raw_tokens: 38000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "An admin can create a clinic (POST /api/clinics) with a required timezone and retrieve it (GET /api/clinics/{id}, GET /api/clinics) reflecting the latest committed state (ORG-01)"
    - "An admin can create a department within a clinic (POST /api/clinics/{clinic_id}/departments) and list departments for a clinic (ORG-02)"
    - "A non-admin token (front-desk/patient/provider) creating a clinic or department returns 403 (RBAC via require_role(Role.ADMIN))"
    - "Every clinic/department mutation writes a shared write_audit entry (who/what/when, no PHI)"
    - "A department cannot be created under a non-existent clinic — the FK/lookup rejects it (ORG-02 referential integrity)"
    - "Departments and their providers are visibly scoped to the correct clinic via the FK chain (ORG-03)"
    - { statement: "Creating a clinic and immediately reading it back returns the just-committed row (read-after-write consistency — single Postgres system of record, no stale cache)", verification: explicit }
    - { statement: "Listing departments for a clinic that has none returns an empty list (not an error) — empty-input edge", verification: explicit }
    - { statement: "Two departments with the same name under the same clinic are handled deterministically (either rejected as a duplicate or allowed with distinct ids) — the adjacency/equal-name case has a defined outcome", verification: backstop }
  artifacts:
    - path: services/identity/api/clinics.py
      provides: "Clinic CRUD router (admin-only), audited"
      exports: ["router"]
    - path: services/identity/api/departments.py
      provides: "Department CRUD router nested under clinic (admin-only), audited"
      exports: ["router"]
    - path: services/identity/repository/org_repo.py
      provides: "Async repository for clinics/departments — every method takes clinic scope explicitly (Pitfall 1)"
  key_links:
    - from: services/identity/api/clinics.py
      to: services/identity/security/rbac.py
      via: "clinic write routes declare Depends(require_role(Role.ADMIN))"
      pattern: "require_role"
    - from: services/identity/api/departments.py
      to: libs/audit/writer.py
      via: "each department mutation calls the shared write_audit helper"
      pattern: "write_audit"
    - from: services/identity/repository/org_repo.py
      to: services/identity/repository/models.py
      via: "Department.clinic_id FK enforces ORG-03 scoping; repo methods require clinic_id"
      pattern: "clinic_id"
  prohibitions:
    - "Clinic/department repository methods MUST NOT expose an unscoped query path for scoped reads — a method that fetches by entity id alone (no clinic scope) for a clinic-scoped entity is the Pitfall-1 cross-clinic enumeration hole"
    - "Clinic/department mutations MUST NOT bypass the shared write_audit helper (no per-router copy-pasted Mongo insert)"
    - "RBAC MUST NOT be relaxed for convenience — clinic/department management is admin-only (ORG-01/02)"
---

<objective>
Build clinic and department CRUD on top of the proven auth/RBAC/audit skeleton from the tracer (Plan 03): admin-only clinic creation/retrieval and department creation/listing within a clinic, with clinic-scoped queries and audit on every mutation.

Purpose: Wave-4 expansion. This satisfies ORG-01 (clinics), ORG-02 (departments within a clinic), and the organizational half of ORG-03 (departments/providers scoped to a clinic). It expands horizontally from the tracer's skeleton — reusing require_role, write_audit, get_current_user, get_db — so no new architecture is introduced, only breadth. Runs in parallel with Plan 05 (disjoint files).

Output: org_repo (clinic-scoped repository), clinics + departments routers (admin-only, audited), integration tests.
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

<interfaces>
<!-- Consumes from Plans 02+03: -->
smarthealth.db: get_db (AsyncSession dependency)
services.identity.domain.enums: Role
services.identity.repository.models: Clinic (id, name, timezone), Department (id, clinic_id FK, name)
services.identity.security.rbac: require_role(*roles)
services.identity.security.current_user: get_current_user -> AuthenticatedUser(sub, role, clinic_id, jti)
libs.audit.writer: write_audit(mongo, *, actor_id, actor_role, action, entity_type, entity_id, clinic_id, trace_id, outcome, changes=None)
services.identity.app: app (FastAPI) — mount the new routers here
</interfaces>
</context>

<artifacts_this_phase_produces>
- Endpoints: POST /api/clinics, GET /api/clinics, GET /api/clinics/{id}, POST /api/clinics/{clinic_id}/departments, GET /api/clinics/{clinic_id}/departments
- `services/identity/repository/org_repo.py` → create_clinic, get_clinic, list_clinics, create_department, list_departments (all clinic-scope-aware)
- `services/identity/api/clinics.py` → router; `services/identity/api/departments.py` → router
- Pydantic schemas: ClinicCreate (name, timezone), ClinicOut, DepartmentCreate (name), DepartmentOut
</artifacts_this_phase_produces>

<tasks>

<task type="auto">
  <name>Task 1: Clinic + Department repository and routers (admin-only, audited)</name>
  <read_first>services/identity/security/rbac.py + libs/audit/writer.py + services/identity/security/current_user.py (from Plan 03), services/identity/repository/models.py (Clinic, Department from Plan 02), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Pattern 1 RBAC scope note + Pitfall 1 clinic-scoped queries)</read_first>
  <precondition>Plan 03's shared libs exist and import cleanly (require_role, write_audit, get_current_user) and the tracer's auth slice passed — this plan builds directly on that skeleton</precondition>
  <files>services/identity/repository/org_repo.py, services/identity/api/clinics.py, services/identity/api/departments.py, services/identity/app.py</files>
  <action>Create services/identity/repository/org_repo.py with async methods: create_clinic(db, name, timezone), get_clinic(db, clinic_id), list_clinics(db), create_department(db, clinic_id, name) — which first verifies the clinic exists and rejects otherwise — and list_departments(db, clinic_id). Every department read/write takes clinic_id explicitly (Pitfall 1 — no unscoped path). Create services/identity/api/clinics.py: a router with POST /api/clinics (ClinicCreate: name + required timezone IANA string) and GET list/detail, all write routes guarded by Depends(require_role(Role.ADMIN)); after each mutation call write_audit (action clinic.create, entity_type clinic, trace_id from active span, outcome success). Create services/identity/api/departments.py: router with POST /api/clinics/{clinic_id}/departments and GET list, admin-guarded, audited (department.create). Mount both routers on services/identity/app.py under /api. Use Pydantic v2 request/response models; return 404 for a missing clinic, 403 for non-admin.</action>
  <acceptance_criteria>
    - `grep -c "require_role" services/identity/api/clinics.py services/identity/api/departments.py` shows the guard present on both routers
    - `grep -c "write_audit" services/identity/api/clinics.py services/identity/api/departments.py` shows audit on both
    - org_repo department methods accept clinic_id (grep confirms no id-only department fetch)
    - the app imports and starts with the new routers mounted (app factory import succeeds)
  </acceptance_criteria>
  <verify>
    <automated>python -c "from services.identity.app import app; paths={r.path for r in app.routes}; assert '/api/clinics' in paths, sorted(paths)"</automated>
    <fails_when>the import raises, or /api/clinics is not among the mounted routes — the routers were not wired into the app</fails_when>
    <automated>grep -c "require_role" services/identity/api/clinics.py; grep -c "write_audit" services/identity/api/departments.py</automated>
    <fails_when>either count is 0 — a router is missing its admin RBAC guard or its audit write</fails_when>
  </verify>
  <done>Clinic and department routers exist, admin-guarded, audited via the shared helper, clinic-scoped, and mounted on the app.</done>
</task>

<task type="auto">
  <name>Task 2: Clinic + Department integration tests (ORG-01/02/03)</name>
  <read_first>services/identity/api/clinics.py + services/identity/api/departments.py (Task 1), tests/conftest.py (client fixture + admin/front-desk JWT helpers), tests/test_rbac.py (RBAC test pattern from Plan 03)</read_first>
  <files>tests/test_clinics.py, tests/test_departments.py</files>
  <action>Create tests/test_clinics.py: test_create_clinic (admin creates a clinic with timezone, 201/200); test_read_after_write (create then GET returns the committed row — ORG-01 consistency); test_non_admin_forbidden (front-desk creating a clinic → 403); test_create_clinic_audited (a clinic.create audit entry exists). Create tests/test_departments.py: test_create_department (admin creates a department under an existing clinic); test_department_under_missing_clinic_404 (create under a non-existent clinic id → 404/422, no orphan row); test_list_departments_empty (a clinic with no departments returns []); test_department_scoped_to_clinic (a department lists only under its own clinic — ORG-03); test_duplicate_department_name_defined_outcome (two departments same name/same clinic have a deterministic, asserted outcome). Use conftest JWT helpers to mint admin vs front-desk tokens.</action>
  <acceptance_criteria>
    - `pytest tests/test_clinics.py tests/test_departments.py -x` passes every case
    - read-after-write returns the just-created clinic
    - non-admin clinic creation returns 403
    - department under a missing clinic does not create an orphan row
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_clinics.py tests/test_departments.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — clinic/department CRUD, RBAC 403, read-after-write, or referential integrity failed</fails_when>
  </verify>
  <done>ORG-01/02/03 proven: admin clinic+department CRUD with read-after-write consistency, RBAC 403 for non-admins, clinic scoping, and referential integrity, all audited.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| client → API | Untrusted clinic/department input; admin-only mutations validated server-side |
| API → PostgreSQL | Clinic/department writes; clinic scoping enforced at the query layer |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-04-01 | Elevation of Privilege | Cross-clinic data access via ID enumeration | high | mitigate | org_repo methods require clinic_id; no id-only department read path (Pitfall 1); ORG-03 FK scope chain |
| T-04-02 | Elevation of Privilege | Non-admin performs admin-only clinic/department op | high | mitigate | Depends(require_role(Role.ADMIN)) on all mutation routes; test asserts 403 for front-desk |
| T-04-03 | Repudiation | Unaudited clinic/department mutation | medium | mitigate | Shared write_audit on every mutation; test asserts audit entry presence |
| T-04-04 | Tampering | Orphan department under non-existent clinic | low | mitigate | create_department verifies clinic existence and rejects; FK constraint backstops |
</threat_model>

<verification>
- App imports with /api/clinics and department routes mounted; both routers carry require_role + write_audit.
- `pytest tests/test_clinics.py tests/test_departments.py -x` passes: CRUD, read-after-write, RBAC 403, referential integrity, empty-list edge.
</verification>

<success_criteria>
ORG-01/02/03 satisfied: admin can create/retrieve clinics (with timezone) and departments within them, reads reflect latest state, non-admins are rejected 403, departments/providers are clinic-scoped, and every mutation is audited.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-04-SUMMARY.md` when done.
</output>
