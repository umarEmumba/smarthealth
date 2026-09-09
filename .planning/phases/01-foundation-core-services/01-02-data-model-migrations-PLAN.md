---
phase: 01-foundation-core-services
plan: 02
type: execute
wave: 2
depends_on: ["01-01"]
files_modified:
  - smarthealth/db.py
  - services/identity/domain/enums.py
  - services/identity/repository/models.py
  - infra/migrations/env.py
  - infra/migrations/script.py.mako
  - infra/migrations/versions/0001_initial_schema.py
  - alembic.ini
  - libs/audit/mongo_indexes.py
  - tests/test_models.py
autonomous: false
requirements: [PPL-01, PPL-02, PPL-03, PPL-04, ORG-01, ORG-02, ORG-03]

estimate:
  tokens: 68000
  raw_tokens: 45000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Alembic upgrade head creates users, patients, providers, specialties, provider_specialties, clinics, departments, availability_templates, and (empty) provider_slots tables in Postgres with no error"
    - "The provider_slots table carries a PostgreSQL EXCLUDE USING gist (provider_id WITH =, tstzrange(start_time,end_time) WITH &&) WHERE (status <> 'cancelled') constraint and the btree_gist extension is created by the migration"
    - "clinics has a required timezone column (IANA tz name) so availability template times are unambiguous (RESEARCH Pitfall 3)"
    - "users (auth identity) and patients (operational profile) are SEPARATE tables, with patients optionally linked to a user via nullable user_id (resolves the IAM-01 vs PPL-01 open question)"
    - "providers reference a department, departments reference a clinic, and availability_templates reference both provider_id and department_id — the ORG-03 scoping FK chain provider_slots.provider_id → providers.department_id → departments.clinic_id exists"
    - "a provider can carry one or more specialties via a provider_specialties join table (PPL-02)"
    - "availability_templates stores day_of_week (0-6 CHECK), start_time/end_time (TIME), slot_duration_minutes, effective_from/effective_to (DATE), clinic/department scope (PPL-03)"
    - "the MongoDB audit_log collection has indexes on (entity_type,entity_id,timestamp desc), (actor_id,timestamp desc), and (clinic_id,timestamp desc)"
    - { statement: "Inserting two overlapping non-cancelled provider_slots rows for the same provider raises a database integrity error (the exclusion constraint is live, not merely declared) — proves the Phase-2 double-booking backstop before Phase 2 depends on it", verification: explicit }
    - { statement: "The migration is re-runnable: alembic downgrade base then upgrade head returns to the same schema without error (idempotency of the schema definition)", verification: backstop }
  artifacts:
    - path: services/identity/repository/models.py
      provides: "SQLAlchemy 2.0 declarative models for all Phase-1 entities + the ProviderSlot model with ExcludeConstraint"
      contains: "ExcludeConstraint"
    - path: infra/migrations/versions/0001_initial_schema.py
      provides: "Initial Alembic migration creating every table + btree_gist extension + exclusion constraint + indexes"
      contains: "btree_gist"
    - path: services/identity/domain/enums.py
      provides: "Role enum (patient/provider/front_desk/admin) and slot status enum"
      contains: "class Role"
    - path: smarthealth/db.py
      provides: "Async engine + async_sessionmaker + get_db dependency (create_async_engine on settings.database_url)"
      contains: "create_async_engine"
    - path: libs/audit/mongo_indexes.py
      provides: "Idempotent audit_log index creation callable run at startup"
  key_links:
    - from: services/identity/repository/models.py
      to: smarthealth/db.py
      via: "models share the declarative Base bound to the async engine in db.py"
      pattern: "Base"
    - from: infra/migrations/env.py
      to: services/identity/repository/models.py
      via: "Alembic autogenerate target_metadata = Base.metadata imports the models module"
      pattern: "target_metadata"
    - from: services/identity/repository/models.py
      to: services/identity/repository/models.py
      via: "provider_slots.provider_id FK → providers.id, providers.department_id FK → departments.id, departments.clinic_id FK → clinics.id (ORG-03 scope chain)"
      pattern: "ForeignKey"
  prohibitions:
    - "Double-booking prevention MUST NOT be implemented only as an application-level check-then-insert or a Redis-lock-only scheme — the PostgreSQL EXCLUDE USING gist constraint is the load-bearing guarantee (RESEARCH Don't-Hand-Roll); this migration must create it even though provider_slots stays empty in Phase 1"
    - "Availability template start_time/end_time MUST NOT be stored without a clinic timezone reference — a naive TIME with no clinic.timezone column silently corrupts multi-region slot generation in Phase 2"
    - "The models MUST NOT use a sync engine or psycopg2 — SQLAlchemy 2.0 async (create_async_engine, postgresql+asyncpg) only"
---

<objective>
Define the complete PostgreSQL data model for identity, patient/provider/clinic/department management, and provider availability — plus the empty-but-constrained provider_slots table whose GiST exclusion constraint Phase 2's double-booking guarantee sits on — and the MongoDB audit_log index setup. All as SQLAlchemy 2.0 async declarative models and an Alembic initial migration.

Purpose: This is the shared data foundation (Wave 2). Plans 03/04/05 build CRUD and auth on top of these tables. Getting the availability/slot model right NOW is the single most consequential Phase 1 decision (per RESEARCH and the planning context) — Phase 2 inherits it and must not re-plumb it under saga time-pressure. This plan therefore designs the future provider_slots exclusion constraint now and proves it is live, even though no slot rows are generated until Phase 2.

Output: models.py (all entities), db.py (async engine/session), enums.py (Role + slot status), Alembic env + initial migration, Mongo audit index setup.

**Flagged assumptions (no CONTEXT.md locked these — following RESEARCH recommendations):**
- **users vs patients separation** (RESEARCH Open Question 1 + planning context `<open_question_from_research>`): auth identity (`users`) is a distinct entity from the operational patient profile (`patients`); `patients.user_id` is nullable so a front-desk-created profile need not have a login. This satisfies both IAM-01 and PPL-01.
- **provider_slots created empty in Phase 1** (RESEARCH assumption A3): the table + constraint ship now to remove it from Phase 2's critical path; no rows are generated this phase.
- **No audit-log TTL/retention** (RESEARCH assumption A2): no TTL index until a retention policy is decided.
- **Single-clinic-scoped staff** (RESEARCH assumption A1): `clinic_id` is a single scalar on users, not a multi-clinic array.
</objective>

<execution_context>
@~/.claude/gsd-core/workflows/execute-plan.md
@~/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.claude/CLAUDE.md
@.planning/phases/01-foundation-core-services/01-RESEARCH.md
@.planning/phases/01-foundation-core-services/01-01-SUMMARY.md

<interfaces>
<!-- Consumes from Plan 01: -->
smarthealth.config.settings.database_url (postgresql+asyncpg://...), settings.mongo_url, settings.mongo_db_name

<!-- This plan CREATES the shared ORM contract Plans 03/04/05 consume: -->
smarthealth/db.py exports: `Base` (DeclarativeBase), `engine` (AsyncEngine), `async_session` (async_sessionmaker), `get_db()` (FastAPI dependency yielding AsyncSession)
services/identity/domain/enums.py exports: `Role` (str Enum: PATIENT, PROVIDER, FRONT_DESK, ADMIN), `SlotStatus` (available/held/booked/cancelled)
services/identity/repository/models.py exports: `User, Patient, Provider, Specialty, Clinic, Department, AvailabilityTemplate, ProviderSlot` ORM classes
</interfaces>
</context>

<artifacts_this_phase_produces>
- Tables (Postgres): users, patients, providers, specialties, provider_specialties, clinics, departments, availability_templates, provider_slots
- `smarthealth/db.py` → Base, engine, async_session, get_db
- `services/identity/domain/enums.py` → Role, SlotStatus
- `services/identity/repository/models.py` → User, Patient, Provider, Specialty, Clinic, Department, AvailabilityTemplate, ProviderSlot
- `infra/migrations/versions/0001_initial_schema.py` → Alembic revision (btree_gist extension, all tables, exclusion constraint, FK chain, indexes)
- `alembic.ini`, `infra/migrations/env.py` → async Alembic config (run_sync bridge)
- `libs/audit/mongo_indexes.py` → `ensure_audit_indexes(mongo_client)` idempotent index creator
- MongoDB collection: `audit_log` with three indexes
</artifacts_this_phase_produces>

<tasks>

<task type="checkpoint:decision" gate="blocking-human">
  <decision>Confirm the provider availability & slot data model shape before it is written into the schema (the most consequential Phase-1 decision; no CONTEXT.md locked it)</decision>
  <context>This is a one-way door: the provider_slots table shape and its GiST exclusion constraint become the on-disk schema Phase 2's booking saga and its data-layer double-booking guarantee (Phase 2 success criterion 2) are built directly on. Changing it after Phase 2 generates and books slots requires migrating live scheduling state. The users-vs-patients separation is similarly load-bearing across both phases. RESEARCH recommends the options below; this checkpoint confirms the door before the agent walks through it.</context>
  <options>
    <option id="recommended">
      <name>Weekly availability templates now + empty provider_slots with GiST exclusion constraint designed now; separate users and patients tables</name>
      <pros>Phase 1 stays config-only (no slot-generation horizon/DST/timezone logic without a booking consumer); Phase 2 inherits a slot table whose double-booking invariant already exists and is tested; users/patients separation satisfies both IAM-01 self-registration and PPL-01 staff-managed profiles without conflict (RESEARCH Decision 2 + Open Question 1)</pros>
      <cons>One migration file carries an empty table Phase 1 never writes to; a second entity (patients vs users) to reason about</cons>
    </option>
    <option id="slots-now">
      <name>Generate concrete slot rows in Phase 1 (e.g. next 90 days materialized)</name>
      <pros>provider_slots is populated immediately</pros>
      <cons>Couples Phase 1 to a slot-generation horizon + regeneration-on-template-change with no booking consumer to validate against; violates the roadmap's Phase-1 boundary; risks discovering timezone/DST schema problems late (RESEARCH rejects this)</cons>
    </option>
    <option id="single-user-entity">
      <name>Single combined user/patient entity (no separate patients table)</name>
      <pros>One fewer table</pros>
      <cons>Forces patient auth-account creation and operational-profile creation to be the same action, breaking PPL-01 (front-desk-created profile with no login) — RESEARCH Open Question 1 rejects this</cons>
    </option>
  </options>
  <resume-signal>Select: recommended, slots-now, or single-user-entity (recommended is the RESEARCH-backed default)</resume-signal>
</task>

<task type="auto">
  <name>Task 1: SQLAlchemy 2.0 async engine, enums, and all entity models</name>
  <read_first>.planning/phases/01-foundation-core-services/01-RESEARCH.md (Decision 2 + Pattern 3 exclusion-constraint SQLAlchemy example + Pitfall 3 timezone + Recommended Project Structure), smarthealth/config.py (settings.database_url from Plan 01), .claude/CLAUDE.md (SQLAlchemy 2.0 async / asyncpg mandate)</read_first>
  <reversibility rating="one-way">The provider_slots table shape and its exclusion constraint are the on-disk schema Phase 2's booking saga and its data-layer double-booking guarantee are built directly on; changing this after Phase 2 generates and books slots requires a data migration of live scheduling state. The users/patients separation is similarly load-bearing for both phases.</reversibility>
  <files>smarthealth/db.py, services/identity/domain/enums.py, services/identity/repository/models.py</files>
  <action>Create smarthealth/db.py: create_async_engine(settings.database_url) with postgresql+asyncpg, an async_sessionmaker named async_session, a DeclarativeBase subclass `Base`, and an async get_db() dependency yielding an AsyncSession. Create services/identity/domain/enums.py with Role(str, Enum) = patient/provider/front_desk/admin and SlotStatus = available/held/booked/cancelled. Create services/identity/repository/models.py with SQLAlchemy 2.0 typed declarative models: User (id UUID pk, email unique, hashed_password, role, clinic_id nullable FK, created_at); Patient (id UUID, user_id nullable FK→users, clinic_id FK→clinics, plus operational profile fields — treat name/dob/contact as PHI columns, they live in Postgres but must never be logged); Clinic (id UUID, name, timezone NOT NULL — IANA tz string per Pitfall 3); Department (id UUID, clinic_id FK→clinics, name); Provider (id UUID, department_id FK→departments, plus profile fields); Specialty (id UUID, name) + provider_specialties association table (provider_id, specialty_id) for the many-to-many (PPL-02); AvailabilityTemplate (id UUID, provider_id FK→providers, department_id FK→departments, day_of_week SMALLINT CHECK 0-6, start_time TIME, end_time TIME, slot_duration_minutes default 30, effective_from DATE, effective_to DATE nullable, created_at); ProviderSlot (id UUID, provider_id FK→providers, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ, status default 'available') with __table_args__ carrying an ExcludeConstraint((provider_id,'='),(text('tstzrange(start_time,end_time)'),'&&'), where=text("status <> 'cancelled'"), using='gist'). Use server_default gen_random_uuid() for UUID pks. Do NOT import or use any sync engine/driver.</action>
  <acceptance_criteria>
    - `python -c "from services.identity.repository import models; from smarthealth.db import Base; print(len(Base.metadata.tables))"` prints at least 9 (all tables registered)
    - `grep -c "ExcludeConstraint" services/identity/repository/models.py` is at least 1
    - `grep -c "timezone" services/identity/repository/models.py` is at least 1 (clinic timezone column present)
    - `grep -vE '^\s*#' smarthealth/db.py | grep -c "create_async_engine"` is at least 1 and `grep -c "psycopg2" smarthealth/db.py` is 0
    - the models import cleanly (no circular-import or metadata error)
  </acceptance_criteria>
  <verify>
    <automated>python -c "from services.identity.repository import models; from smarthealth.db import Base; assert len(Base.metadata.tables) >= 9, len(Base.metadata.tables); assert 'provider_slots' in Base.metadata.tables and 'availability_templates' in Base.metadata.tables"</automated>
    <fails_when>the import raises, or the assertion fails because fewer than 9 tables registered or provider_slots/availability_templates are missing from the metadata</fails_when>
    <automated>grep -c "ExcludeConstraint" services/identity/repository/models.py; grep -vE '^\s*#' smarthealth/db.py | grep -c "psycopg2"</automated>
    <fails_when>the ExcludeConstraint count is 0 (double-booking backstop absent), OR the psycopg2 count is greater than 0 (forbidden sync driver imported)</fails_when>
  </verify>
  <done>All entity models exist as async SQLAlchemy 2.0 declaratives with the users/patients separation, ORG-03 FK scope chain, clinic timezone, and the provider_slots exclusion constraint; no sync driver.</done>
</task>

<task type="auto">
  <name>Task 2: Alembic async config + initial migration (extension, tables, constraint, indexes)</name>
  <read_first>services/identity/repository/models.py (Base.metadata — created in Task 1), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Pattern 3 raw-SQL exclusion DDL + btree_gist note), .claude/CLAUDE.md (Alembic 1.19.2 async via run_sync bridging)</read_first>
  <precondition>The Postgres container from Plan 01 is reachable at settings.database_url (read-only connection check) — the migration and its verification run against a live Postgres because the btree_gist exclusion constraint is a server-side feature that cannot be validated by SQLite/mock</precondition>
  <files>alembic.ini, infra/migrations/env.py, infra/migrations/script.py.mako, infra/migrations/versions/0001_initial_schema.py</files>
  <action>Initialize Alembic with the migrations directory at infra/migrations. Configure alembic.ini and env.py for the SQLAlchemy 2.0 async engine using the run_sync bridge (connect via async engine on settings.database_url, run migrations inside connection.run_sync). Set target_metadata = Base.metadata by importing services.identity.repository.models. Author 0001_initial_schema.py: first op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist") and rely on pgcrypto/gen_random_uuid availability (add CREATE EXTENSION IF NOT EXISTS pgcrypto if gen_random_uuid needs it); then create every table with its FK chain; add the provider_slots ExcludeConstraint via the migration (op.create_table with the constraint, or op.execute the raw EXCLUDE DDL from RESEARCH Pattern 3); create indexes on availability_templates(provider_id) and the ORG-03-relevant FKs. Provide a working downgrade that drops tables and the extension. Ensure upgrade/downgrade are symmetric.</action>
  <acceptance_criteria>
    - `alembic -c alembic.ini upgrade head` exits 0 against the live Postgres and creates all 9 tables (verify via a catalog query counting expected tables)
    - after upgrade, `\d provider_slots` (or a pg_constraint query) shows a gist exclusion constraint
    - `alembic -c alembic.ini downgrade base` then `upgrade head` completes with exit 0 (symmetric migration)
    - the migration file contains "btree_gist"
  </acceptance_criteria>
  <verify>
    <automated>alembic -c alembic.ini upgrade head</automated>
    <fails_when>alembic exits non-zero — the migration failed (missing btree_gist extension surfaces as "operator class not found" / "data type ... has no default operator class")</fails_when>
    <automated>python -m tests.check_exclusion_live 2>/dev/null || pytest tests/test_models.py::test_exclusion_constraint_live -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — inserting two overlapping non-cancelled provider_slots for one provider did NOT raise an integrity error, meaning the exclusion constraint is not live</fails_when>
  </verify>
  <done>Alembic upgrade head builds the full schema on live Postgres with the btree_gist exclusion constraint present and enforcing; downgrade/upgrade is symmetric.</done>
</task>

<task type="auto">
  <name>Task 3: MongoDB audit index setup + exclusion-constraint integration test</name>
  <read_first>.planning/phases/01-foundation-core-services/01-RESEARCH.md (MongoDB Audit-Trail Schema Design — Indexes), tests/conftest.py (db_session, mongo_client fixtures from Plan 01), services/identity/repository/models.py (ProviderSlot from Task 1)</read_first>
  <precondition>The MongoDB container from Plan 01 is reachable at settings.mongo_url (read-only ping) — index creation runs against a live Mongo</precondition>
  <files>libs/audit/mongo_indexes.py, tests/test_models.py</files>
  <action>Create libs/audit/mongo_indexes.py with an idempotent async `ensure_audit_indexes(mongo_client)` that creates three indexes on the smarthealth.audit_log collection: (entity_type ASC, entity_id ASC, timestamp DESC), (actor_id ASC, timestamp DESC), (clinic_id ASC, timestamp DESC). Use create_index (idempotent by name). Do NOT create a TTL index (RESEARCH A2 — no retention policy decided). Create tests/test_models.py with: (a) test_exclusion_constraint_live — using the db_session fixture, insert one provider + one non-cancelled slot, then attempt a second overlapping non-cancelled slot for the same provider and assert an IntegrityError is raised (mark integration); (b) test_audit_indexes_created — call ensure_audit_indexes against the mongo_client fixture and assert the three indexes exist via list_indexes (mark integration); (c) a non-integration unit test asserting the AvailabilityTemplate model rejects day_of_week outside 0-6 at the Python/Pydantic boundary if a schema exists, else asserting the CHECK is declared in the model.</action>
  <acceptance_criteria>
    - `pytest tests/test_models.py -x` (integration, against live containers) passes all three tests
    - ensure_audit_indexes is idempotent: calling it twice does not raise
    - `grep -c "TTL\|expireAfterSeconds" libs/audit/mongo_indexes.py` is 0 (no retention index)
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_models.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — the exclusion constraint did not fire on overlap, or the three audit indexes were not created</fails_when>
    <automated>grep -cE 'expireAfterSeconds' libs/audit/mongo_indexes.py</automated>
    <fails_when>the count is greater than 0 — a TTL/retention index was added contrary to the recorded assumption A2</fails_when>
  </verify>
  <done>The audit_log collection has its three query indexes (idempotently), and integration tests prove the exclusion constraint fires on overlap and the indexes are created.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| application → PostgreSQL | All entity writes cross here; PHI columns (patient name/dob/contact) live in Postgres and must never leak to logs/traces |
| application → MongoDB audit store | Audit documents are application-adjacent data subject to the same PHI discipline as logs |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-02-01 | Tampering | provider_slots double-booking under future concurrent load | high | mitigate | PostgreSQL EXCLUDE USING gist constraint created now and proven live in test — the load-bearing data-layer guarantee Phase 2 requires (not application logic alone) |
| T-02-02 | Elevation of Privilege | Cross-clinic data access via ID enumeration | high | mitigate | ORG-03 FK scope chain (provider_slots→providers→departments→clinics) makes clinic scoping structural; query-layer clinic_id filtering enforced in Plans 03-05 repositories (RESEARCH Pitfall 1) |
| T-02-03 | Information Disclosure | PHI columns in Postgres surfaced to logs/audit values | high | mitigate | PHI-bearing columns identified in the model; the audit schema (Plan 03) records field names not PHI values; logging discipline enforced in Plan 03 |
| T-02-04 | Tampering | SQL injection | high | mitigate | SQLAlchemy 2.0 ORM/Core parameterized queries by default; no raw string-interpolated SQL in repositories |
</threat_model>

<verification>
- Models import; ≥9 tables in Base.metadata including provider_slots + availability_templates; ExcludeConstraint present; clinic timezone present; no sync driver.
- `alembic upgrade head` builds the full schema on live Postgres; btree_gist exclusion constraint present and enforcing; downgrade/upgrade symmetric.
- Mongo audit_log has three indexes idempotently; no TTL index.
- Integration test proves overlapping non-cancelled slots raise IntegrityError.
</verification>

<success_criteria>
The complete Phase-1 PostgreSQL schema and Mongo audit indexing exist. The users/patients separation, ORG-03 scope chain, clinic timezone, and provider_slots exclusion constraint (proven live) are in place — Phase 2 inherits a slot model whose double-booking invariant already exists and is tested.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-02-SUMMARY.md` when done. Record the final table list and the exclusion-constraint DDL as delivered (dashboard Plan 06 references it), and confirm the users/patients separation decision as implemented.
</output>
