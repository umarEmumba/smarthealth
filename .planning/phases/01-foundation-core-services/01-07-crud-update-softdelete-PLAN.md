---
plan: 01-07-crud-update-softdelete
phase: 01-foundation-core-services
wave: 7
depends_on: ["01-04", "01-05"]
autonomous: true
gap_closure: true
requirements: ["PPL-01", "PPL-04", "ORG-01", "ORG-02"]
files_modified:
  - services/identity/repository/models.py
  - infra/migrations/versions/0002_soft_delete_columns.py
  - services/identity/domain/schemas.py
  - services/identity/repository/org_repo.py
  - services/identity/repository/people_repo.py
  - services/identity/api/clinics.py
  - services/identity/api/departments.py
  - services/identity/api/patients.py
  - services/identity/api/providers.py
  - services/identity/api/availability.py
  - tests/test_clinics.py
  - tests/test_departments.py
  - tests/test_patients.py
  - tests/test_providers.py
  - tests/test_availability.py
files_deleted: []
task_count: 4
---

<objective>
Close the requirement-vs-plan gap surfaced after Phase 1 verification: the requirements PPL-01/ORG-01/ORG-02 say "manage" (which includes editing and lifecycle), but Plans 04/05 shipped only create + read. Add partial-UPDATE (PATCH) and SOFT-DELETE (deactivate) for clinics, departments, patients, providers, and provider-availability templates — reusing the proven auth/RBAC/audit/PHI-safe skeleton from the tracer (Plan 03). No hard deletes: healthcare records must remain auditable and retained, so "delete" is a status flip, never a row removal.
</objective>

<must_haves>
- PATCH partial-update endpoint for each of: clinics, departments, patients, providers, availability templates. Only supplied fields change (Pydantic v2 update schemas with all-optional fields; reject empty bodies with 422).
- Deactivate (soft-delete) endpoint for each resource: sets is_active=false + deactivated_at=now(), never deletes the row. Idempotent (deactivating an already-inactive resource is a no-op success or 409 — pick one and be consistent).
- List endpoints exclude inactive resources by default; accept `?include_inactive=true` to include them. Get-by-id still returns an inactive resource (with its status visible).
- Every mutation (update, deactivate) writes an audit_log entry via the existing writer, and emits a PHI-safe structured log line carrying trace_id. No PHI (patient names, DOB, contact info) in logs.
- RBAC preserved and consistent with the create routes: clinics/departments/providers = admin-only; patients = front-desk or admin (clinic-scoped for front-desk); availability = provider-ownership (a provider may only edit/deactivate their OWN provider_id) or admin.
- Alembic migration 0002 adds `is_active BOOLEAN NOT NULL DEFAULT true` and `deactivated_at TIMESTAMPTZ NULL` to clinics, departments, patients, providers, availability_templates. Symmetric downgrade drops both columns. Migration verified with `alembic upgrade head` then `alembic downgrade -1` round-trip against the live Postgres container.
- Full test suite green, including new tests: successful PATCH, partial-field PATCH, 404 on unknown id, RBAC denials (wrong role / non-owner provider), soft-delete hides from default list but not from include_inactive, audit entries written for update + deactivate.
</must_haves>

<tasks>
<task id="1">
<action>Add soft-delete columns to the model + migration. In services/identity/repository/models.py add `is_active: Mapped[bool]` (default True, nullable=False) and `deactivated_at: Mapped[datetime | None]` (timezone-aware, nullable) to Clinic, Department, Patient, Provider, AvailabilityTemplate. Create infra/migrations/versions/0002_soft_delete_columns.py (down_revision = the 0001 revision id) adding both columns to all five tables with server_default text('true') for is_active (drop the server_default after backfill if desired, but keeping it is fine), and a symmetric downgrade dropping both columns from all five tables. Verify: `alembic upgrade head` then `alembic downgrade -1` then `alembic upgrade head` against the live Postgres container — schema identical after round-trip.</action>
<commit>feat(01-07): add is_active/deactivated_at soft-delete columns + migration 0002</commit>
</task>
<task id="2">
<action>Org update + deactivate. In services/identity/domain/schemas.py add ClinicUpdate and DepartmentUpdate (all fields Optional; reject a fully-empty body with 422 via a model validator). In services/identity/repository/org_repo.py add update_clinic, deactivate_clinic, update_department, deactivate_department (clinic-scoped), and make list_clinics/list_departments exclude is_active=false unless include_inactive=true. In services/identity/api/clinics.py add `PATCH /api/clinics/{clinic_id}` and `POST /api/clinics/{clinic_id}/deactivate`; in services/identity/api/departments.py add `PATCH /api/clinics/{clinic_id}/departments/{department_id}` and `POST /api/clinics/{clinic_id}/departments/{department_id}/deactivate`. All admin-only (reuse require_role(Role.ADMIN)). Audit clinic.update/clinic.deactivate/department.update/department.deactivate on every mutation. 404 when the id does not exist or is out of clinic scope.</action>
<commit>feat(01-07): clinic + department PATCH and deactivate (admin-only, audited)</commit>
</task>
<task id="3">
<action>People + availability update + deactivate. In schemas.py add PatientUpdate, ProviderUpdate, AvailabilityTemplateUpdate (all-optional; reject empty body). In services/identity/repository/people_repo.py add update_patient, deactivate_patient, update_provider, deactivate_provider, update_availability_template, deactivate_availability_template; keep provider specialties eager-loaded (selectinload) to avoid the async greenlet trap; list methods exclude inactive by default. In services/identity/api/patients.py add `PATCH /api/patients/{patient_id}` + `POST /api/patients/{patient_id}/deactivate` (front-desk or admin; front-desk clinic-scoped). In services/identity/api/providers.py add `PATCH /api/providers/{provider_id}` + `POST /api/providers/{provider_id}/deactivate` (admin-only). In services/identity/api/availability.py add `PATCH /api/providers/{provider_id}/availability/{template_id}` + `POST /api/providers/{provider_id}/availability/{template_id}/deactivate` — enforce provider-ownership (current user's provider.user_id must match) OR admin, mirroring the CR-02 fix. Audit every mutation. NEVER log PHI. Validate availability windows on PATCH the same way create does (end after start, day_of_week 0-6, positive slot duration) — reject invalid with 422.</action>
<commit>feat(01-07): patient/provider/availability PATCH and deactivate (RBAC, PHI-safe, audited)</commit>
</task>
<task id="4">
<action>Tests. Extend tests/test_clinics.py, test_departments.py, test_patients.py, test_providers.py, test_availability.py with: successful PATCH (full + partial fields), empty-body 422, unknown-id 404, RBAC denial (wrong role; and for availability a non-owner provider gets 403), deactivate sets is_active=false, deactivated resource is hidden from the default list but present with ?include_inactive=true, get-by-id still returns it, and an audit_log entry exists for both update and deactivate. Assert no PHI value appears in emitted logs for the patient update path. Run the full suite until green.</action>
<commit>test(01-07): update + soft-delete coverage (RBAC, audit, list-filtering, PHI-safety)</commit>
</task>
</tasks>

<verify>
- `alembic upgrade head && alembic downgrade -1 && alembic upgrade head` round-trips cleanly against the live Postgres container.
- `curl -X PATCH` on each resource with a valid admin/owner token returns 200 and the changed field; with a wrong-role token returns 403; on an unknown id returns 404.
- After `POST .../{id}/deactivate`, the resource is absent from `GET` list, present in `GET list?include_inactive=true`, and still returned by `GET .../{id}`.
- `pytest tests/` is fully green.
- `GET /openapi.json` shows PATCH + deactivate POST routes for all five resources.
</verify>
