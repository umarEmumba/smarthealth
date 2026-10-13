# SmartHealth — Entity-Relationship Diagrams (one schema per service, Phase 2)

Since Phase 1.1 there is no single system-of-record schema. Each service owns one PostgreSQL database
and one SQLAlchemy `DeclarativeBase`, and each has its own Alembic baseline. Phase 2 extends
`scheduling_db` (appointments, idempotency keys, history, waitlist) and adds `visit_db` and `billing_db`:

| Service | Database | Models | Baseline migration |
|---|---|---|---|
| Identity | `identity_db` | `services/identity/repository/models.py` | `identity_0001_accounts` |
| Organization | `organization_db` | `services/organization/repository/models.py` | `organization_0001_org_hierarchy` |
| Provider | `provider_db` | `services/provider/repository/models.py` | `provider_0001_providers` |
| Patient | `patient_db` | `services/patient/repository/models.py` | `patient_0001_patients` |
| Scheduling | `scheduling_db` | `services/scheduling/repository/models.py` | `scheduling_0001_provider_slots` → `scheduling_0002_appointments` → `scheduling_0003_waitlist` (Phase 2) |
| Visit (Phase 2) | `visit_db` | `services/visit/repository/models.py` | `visit_0001_visits` |
| Billing (Phase 2) | `billing_db` | `services/billing/repository/models.py` | `billing_0001_billing_records` |

**Legend:** `PK` primary key · `FK` foreign key (always *within* one service) · `UK` unique · 🔒 PHI-bearing
(never logged, audited or sent to another service) · soft-deletable tables carry `is_active` + `deactivated_at`.
**No foreign key crosses a service.** A column that points at another service's row is a plain UUID, drawn as a
**dashed id-only link** in the last diagram and validated at write time through the owner's API (D-05), never at
read time and never with a cascade (D-06). This is asserted by `tests/libs/test_service_isolation.py`.

---

## identity_db

```mermaid
erDiagram
    USERS {
        uuid id PK "gen_random_uuid()"
        varchar(320) email UK
        varchar(255) hashed_password
        varchar(32) role "CHECK: patient|provider|front_desk|admin"
        uuid clinic_id "nullable, plain UUID (Organization), no FK"
        timestamptz created_at
    }
```

Accounts only: credentials, role and clinic scope. No patient or provider profile data lives here.

## organization_db

```mermaid
erDiagram
    CLINICS ||--o{ DEPARTMENTS : "has (FK, same database)"

    CLINICS {
        uuid id PK
        varchar(255) name
        varchar(64) timezone "IANA tz — resolves naive TIME ambiguity"
        timestamptz created_at
        boolean is_active
        timestamptz deactivated_at "nullable"
    }
    DEPARTMENTS {
        uuid id PK
        uuid clinic_id FK
        varchar(255) name
        timestamptz created_at
        boolean is_active
        timestamptz deactivated_at "nullable"
    }
```

## provider_db

```mermaid
erDiagram
    PROVIDERS ||--o{ AVAILABILITY_TEMPLATES : "defines"
    PROVIDERS ||--o{ PROVIDER_SPECIALTIES : ""
    SPECIALTIES ||--o{ PROVIDER_SPECIALTIES : ""

    PROVIDERS {
        uuid id PK
        uuid user_id UK "nullable, plain UUID (Identity), no FK"
        uuid department_id "plain UUID (Organization), no FK"
        varchar(255) full_name
        timestamptz created_at
        boolean is_active
        timestamptz deactivated_at "nullable"
    }
    SPECIALTIES {
        uuid id PK
        varchar(255) name UK
    }
    PROVIDER_SPECIALTIES {
        uuid provider_id PK "FK"
        uuid specialty_id PK "FK"
    }
    AVAILABILITY_TEMPLATES {
        uuid id PK
        uuid provider_id FK
        uuid department_id "plain UUID (Organization), no FK"
        smallint day_of_week "CHECK: 0..6 (Mon..Sun)"
        time start_time "clinic-tz naive"
        time end_time
        int slot_duration_minutes "default 30"
        date effective_from
        date effective_to "nullable"
        timestamptz created_at
        boolean is_active
        timestamptz deactivated_at "nullable"
    }
```

Provider holds no clinic data; the clinic (and its timezone) is reachable through the department's
Organization reference.

## patient_db

```mermaid
erDiagram
    PATIENTS {
        uuid id PK
        uuid user_id UK "nullable, plain UUID (Identity), no FK — no login required (PPL-01)"
        uuid clinic_id "plain UUID (Organization), no FK"
        varchar(255) full_name "PHI"
        date date_of_birth "PHI"
        varchar(32) phone "PHI, nullable"
        text address "PHI, nullable"
        timestamptz created_at
        boolean is_active
        timestamptz deactivated_at "nullable"
    }
```

🔒 `full_name`, `date_of_birth`, `phone` and `address` are the only PHI-bearing columns in the platform. They live
in this database only and are never written to logs, traces, internal-call URLs, headers or bodies, another
service's database, or the Mongo audit trail (field names only).

## scheduling_db

```mermaid
erDiagram
    PROVIDER_SLOTS ||--o{ APPOINTMENTS : "slot_id (FK, same db)"
    APPOINTMENTS ||--o{ APPOINTMENT_STATUS_HISTORY : "appointment_id (FK)"
    APPOINTMENTS ||--o{ IDEMPOTENCY_KEYS : "appointment_id"
    PROVIDER_SLOTS {
        uuid id PK
        uuid provider_id "plain UUID (Provider), no FK"
        uuid clinic_id "plain UUID (Organization) - Phase 2"
        uuid department_id "plain UUID (Organization) - Phase 2"
        timestamptz start_time "UK with provider_id"
        timestamptz end_time "CHECK end_time > start_time"
        varchar(16) status "CHECK: available|held|booked|cancelled"
        timestamptz held_until "Phase 2: hold expiry (reaper)"
        uuid held_by_appointment_id "Phase 2"
    }
    APPOINTMENTS {
        uuid id PK
        uuid clinic_id "plain UUID"
        uuid patient_id "plain UUID (Patient)"
        uuid provider_id "plain UUID (Provider)"
        uuid slot_id FK
        timestamptz slot_start "copied from slot"
        timestamptz slot_end "copied from slot"
        varchar(16) status "CHECK: pending|confirmed|cancelled|failed"
        varchar(64) failure_reason "machine-readable (BillingRejected, ...)"
        varchar(64) coverage_ref "opaque; never logged or in workflow I/O"
        int version "bumped by cancel/reschedule attempts"
        timestamptz workflow_started_at "NULL = start lost (starter Schedule)"
        uuid created_by
    }
    IDEMPOTENCY_KEYS {
        uuid caller_user_id PK
        varchar(255) key PK
        char(64) request_hash "SHA-256 of the canonical body"
        uuid appointment_id
        timestamptz expires_at "lazy expiry, overwritten not deleted"
    }
    APPOINTMENT_STATUS_HISTORY {
        uuid id PK
        uuid appointment_id FK
        varchar(16) from_status
        varchar(16) to_status
        varchar(64) reason
        varchar(64) actor "user id or workflow:<id>"
    }
    WAITLIST_ENTRIES {
        uuid id PK
        uuid clinic_id
        uuid provider_id "plain UUID"
        uuid patient_id "plain UUID"
        timestamptz window_start "optional"
        timestamptz window_end "optional, CHECK after start"
        varchar(64) coverage_ref "never returned"
        varchar(16) status "CHECK: waiting|matched|booked|failed|cancelled"
        varchar(64) failure_reason
        uuid matched_appointment_id
        uuid booked_appointment_id
        int attempts
    }
```

`ex_provider_slots_no_overlap` (GiST, `btree_gist`) still stops overlapping slots for one provider.
Phase 2 adds the appointment-level arbiters: `uq_appointments_active_slot` (partial unique index on
`slot_id` where status is `pending` or `confirmed`) and `ex_appointments_patient_no_overlap` (one patient
cannot hold two overlapping active appointments). Partial indexes `ix_provider_slots_held_until` (reaper),
`ix_appointments_pending_unstarted` (starter) and `ix_waitlist_fifo` (FIFO claim) serve the background
workflows.

## visit_db (Phase 2)

```mermaid
erDiagram
    VISITS ||--o{ VISIT_STATE_HISTORY : "visit_id (FK)"
    VISITS {
        uuid id PK
        uuid appointment_id "UK uq_visits_appointment; plain UUID (Scheduling)"
        uuid clinic_id
        uuid patient_id
        uuid provider_id
        varchar(16) status "CHECK: checked_in|in_progress|completed"
        timestamptz checked_in_at
        timestamptz started_at
        timestamptz completed_at
        timestamptz downstream_started_at "NULL after completion = sweeper picks it up"
        uuid created_by
    }
    VISIT_STATE_HISTORY {
        uuid id PK
        uuid visit_id FK
        varchar(16) from_status
        varchar(16) to_status "UK with visit_id"
        varchar(64) actor
    }
```

## billing_db (Phase 2)

```mermaid
erDiagram
    BILLING_RECORDS ||--o{ BILLING_STATUS_HISTORY : "record_id (FK)"
    BILLING_RECORDS {
        uuid id PK
        uuid appointment_id "UK; plain UUID (Scheduling)"
        uuid clinic_id
        varchar(16) status "CHECK: pending|verified|failed|voided|refund_pending|refunded|no_refund|finalized"
        varchar(16) coverage_kind "coarse kind only, never the reference"
        varchar(64) reason_code
    }
    BILLING_STATUS_HISTORY {
        uuid id PK
        uuid record_id FK
        varchar(16) from_status
        varchar(16) to_status
        varchar(64) reason_code
        varchar(200) idempotency_key "UK with record_id"
        varchar(64) actor
    }
```

Billing never stores the coverage reference itself, only the outcome and a coarse `coverage_kind`.

---

## Cross-service references (id-only, no foreign keys)

Dashed lines are plain UUID columns, **validated via owner API (D-05)** at write time. Only the referencing
column is shown for each table.

```mermaid
erDiagram
    ORG_CLINICS ||..o{ ID_USERS : "users.clinic_id — validated via owner API (D-05)"
    ORG_CLINICS ||..o{ PAT_PATIENTS : "patients.clinic_id — validated via owner API (D-05)"
    ID_USERS ||..o| PAT_PATIENTS : "patients.user_id — validated via owner API (D-05)"
    ID_USERS ||..o| PRV_PROVIDERS : "providers.user_id — validated via owner API (D-05)"
    ORG_DEPARTMENTS ||..o{ PRV_PROVIDERS : "providers.department_id — validated via owner API (D-05)"
    ORG_DEPARTMENTS ||..o{ PRV_AVAILABILITY_TEMPLATES : "availability_templates.department_id — validated via owner API (D-05)"
    PRV_PROVIDERS ||..o{ SCH_PROVIDER_SLOTS : "provider_slots.provider_id — validated via owner API (D-05)"
    PAT_PATIENTS ||..o{ SCH_APPOINTMENTS : "appointments.patient_id — validated by the booking saga"
    SCH_APPOINTMENTS ||..o| VIS_VISITS : "visits.appointment_id — checked confirmed via Scheduling /internal"
    SCH_APPOINTMENTS ||..o| BIL_BILLING_RECORDS : "billing_records.appointment_id — created by the pre-check"

    ORG_CLINICS {
        uuid id "organization_db.clinics"
    }
    ORG_DEPARTMENTS {
        uuid id "organization_db.departments"
    }
    ID_USERS {
        uuid clinic_id "identity_db.users"
    }
    PAT_PATIENTS {
        uuid clinic_id "patient_db.patients (PHI)"
    }
    PRV_PROVIDERS {
        uuid department_id "provider_db.providers"
    }
    PRV_AVAILABILITY_TEMPLATES {
        uuid department_id "provider_db.availability_templates"
    }
    SCH_PROVIDER_SLOTS {
        uuid provider_id "scheduling_db.provider_slots"
    }
    SCH_APPOINTMENTS {
        uuid patient_id "scheduling_db.appointments"
    }
    VIS_VISITS {
        uuid appointment_id "visit_db.visits"
    }
    BIL_BILLING_RECORDS {
        uuid appointment_id "billing_db.billing_records"
    }
```

| Reference | Owner called | Checked when | If missing or inactive | If owner cannot answer |
|---|---|---|---|---|
| `users.clinic_id` | Organization | account create | 422 | 503 |
| `patients.clinic_id` | Organization | profile create | 422 | 503 |
| `patients.user_id` | Identity | profile create | 422 | 503 |
| `providers.user_id` | Identity | provider create | 422 | 503 |
| `providers.department_id` | Organization | provider create | 422 | 503 |
| `availability_templates.department_id` | Organization | template create only | 422 | 503 |
| `provider_slots.provider_id`, `clinic_id`, `department_id` | Provider, Organization | slot generation | 422 | 503 |
| `appointments.patient_id`, `provider_id`, clinic scope | Patient, Provider, Organization | booking saga `validate` step (and the API for patient self-booking) | appointment `failed` (`ReferenceInvalid`) | activity retried, then `failed` |
| `visits.appointment_id` | Scheduling | check-in | 409 "appointment not confirmed" / 404 | 503 |
| `billing_records.appointment_id` | — (Billing trusts its callers, which are aud-scoped service tokens) | pre-check | — | — |

All Phase 2 references are ids only; reads never enrich across services.

## Constraints that carry weight

| Table | Constraint | What it guarantees |
|---|---|---|
| `scheduling_db.provider_slots` | `EXCLUDE USING gist (provider_id WITH =, tstzrange(start_time,end_time) WITH &&) WHERE status <> 'cancelled'` (`ex_provider_slots_no_overlap`) | A provider's slots never overlap (requires `btree_gist`). |
| `scheduling_db.appointments` | `uq_appointments_active_slot`: `UNIQUE (slot_id) WHERE status IN ('pending','confirmed')` | **Double-booking is impossible at the storage layer**: of N concurrent bookings for one slot exactly one row commits; the rest get 409 (proven by the concurrency suite and the live demo). |
| `scheduling_db.appointments` | `ex_appointments_patient_no_overlap`: GiST exclusion on `(patient_id, tstzrange(slot_start, slot_end))` for active appointments | One patient cannot be in two places at once. |
| `scheduling_db.provider_slots` | `ck_provider_slots_end_after_start`; `uq_provider_slots_provider_start` | No empty/negative slots; regeneration is idempotent (`ON CONFLICT DO NOTHING`). |
| `scheduling_db.idempotency_keys` | `PRIMARY KEY (caller_user_id, key)` + `request_hash` | A retried booking returns the original appointment; the same key with a different body is 422. |
| `scheduling_db.waitlist_entries` | `uq_waitlist_active_patient_provider`: `UNIQUE (patient_id, provider_id) WHERE status IN ('waiting','matched')` | A patient waits at most once per provider. |
| `visit_db.visits` | `uq_visits_appointment` | One visit per appointment; a repeated check-in returns the existing visit. |
| `visit_db.visit_state_history` | `uq_visit_history_visit_to` (`visit_id`, `to_status`) | Each visit state is recorded once, however often a transition is retried. |
| `billing_db.billing_records` | `uq_billing_records_appointment_id` | One billing record per appointment. |
| `billing_db.billing_status_history` | `uq_billing_history_record_key` (`record_id`, `idempotency_key`) | A retried billing call is a no-op instead of a second transition. |
| `identity_db.users` | `CHECK role IN ('patient','provider','front_desk','admin')`; `UNIQUE(email)` | The 4-role RBAC vocabulary is enforced in the DB; no duplicate accounts. |
| `provider_db.availability_templates` | `CHECK day_of_week BETWEEN 0 AND 6` | Weekly-template day is always a valid weekday. |
| `patient_db.patients.user_id`, `provider_db.providers.user_id` | `UNIQUE`, `NULLABLE` | A profile may exist with no login (PPL-01), but an account maps to at most one profile (a second link is a 409). |
| `provider_db.specialties.name` | `UNIQUE` | No duplicate specialties; concurrent creations of the same new specialty share one row. |
| `organization_db.departments.clinic_id` | `FOREIGN KEY` to `clinics.id` | The only hierarchy FK that survives, because both tables are in the same database. |

## Design notes

- **Accounts ⟂ profiles (D-04)** — `users` (Identity) and `patients`/`providers` (their own services) are linked
  only by a nullable plain `user_id`, so a front-desk-created patient needs no login (PPL-01) while
  self-registration still works (IAM-01). Creating a profile never creates the account.
- **PHI containment** — PHI columns exist only in `patient_db`; an invariant test fails if one appears elsewhere.
- **Scope chain** — `provider → department → clinic` is no longer a structural FK chain. It holds through
  write-time validation and stays sound only because a department's `clinic_id` cannot change (flagged
  assumption ORG-03, awaiting mentor confirmation).
- **Status, not deletion (Phase 2)** — appointments, visits, billing records and waitlist entries never
  leave their tables: failed bookings, cancellations and compensations are forward status transitions with an
  append-only history row, so every outcome is reconstructable.
- **Soft-delete** — every operational table except `provider_slots` (which uses `status`) and the reference
  tables `users`, `specialties`, `provider_specialties` carries `is_active` + `deactivated_at`; deletion is a
  status flip, never a row removal, and it never cascades across services (D-06).
- **Availability vs slots** — `availability_templates` (Provider) are recurring weekly *rules*; `provider_slots`
  (Scheduling) are concrete bookable rows generated in Phase 2. Template times are clinic-timezone-naive and are
  resolved against a calendar/DST at slot-generation time. Overlapping templates for one provider are still
  allowed (PPL-03 flagged assumption); Phase 2 slot generation must handle them.
- **Legacy database** — the Phase 1 single `smarthealth` database is untouched and not migrated; no service uses it.

See [`erd.html`](./erd.html) for a standalone visual, and [`architecture.md`](./architecture.md) for the
service breakdown and flow diagrams.
