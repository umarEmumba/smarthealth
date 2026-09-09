# SmartHealth — Intelligent Healthcare Operations Platform (Part A)

MediNova's foundational healthcare-operations backend: a production-grade, distributed
platform for patient/provider management, appointment scheduling, visit workflows,
billing pre-checks, notifications, and operational analytics.

This is Part A — the core platform. See `.planning/PROJECT.md` for full context, and
`docs/architecture-dashboard.html` for the living architecture & decision record.

> This README is a placeholder scaffolded in Phase 1, Plan 1. The full README (overview,
> architecture diagram, setup, API overview, stack) is a project deliverable completed
> incrementally as each phase lands — see `.planning/ROADMAP.md`.

## Local Development

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
cp infra/.env.example infra/.env

docker compose -f infra/docker-compose.yml up -d
scripts/compose_healthcheck.sh

pytest tests/ -x -k "not integration"
```

## First-Admin Bootstrap

`POST /api/auth/register` (public, unauthenticated) always creates a `patient`
account, and the only route that can create an `admin`/`provider`/`front_desk`
account — `POST /api/auth/admin/users` — is itself gated by
`Depends(require_role(Role.ADMIN))`. This closes the privilege-escalation gap
where an unauthenticated caller could self-assign an elevated role, but it also
means a fresh deployment has no way to create its *first* admin account through
the API (a bootstrapping deadlock).

`scripts/bootstrap_admin.py` is the operational escape hatch. It is a one-shot
CLI script, deliberately **not** an HTTP route, so it is never reachable by a
network caller:

```bash
BOOTSTRAP_ADMIN_EMAIL=admin@example.com \
BOOTSTRAP_ADMIN_PASSWORD='a-strong-generated-password' \
python scripts/bootstrap_admin.py
```

Behavior:

- **Idempotent / safe to re-run** — e.g. as a post-deploy hook or Kubernetes
  Job run on every release. If an `admin`-role user already exists anywhere
  in the `users` table, the script no-ops (`"already_exists"`) and makes no
  changes. It never creates a second admin and never overwrites an existing
  user's password or any other field.
- Hashes the password with the same bcrypt path (`libs.auth.passwords.hash_password`)
  the application's own `/api/auth/register` and `/api/auth/admin/users`
  routes use.
- Writes an `audit_log` entry (`action="user.bootstrap"`, `actor_id="system"`)
  so the first admin's creation is part of the audit trail, consistent with
  every other mutation in this codebase.
- Exits `1` with a clear stderr message if `BOOTSTRAP_ADMIN_EMAIL` /
  `BOOTSTRAP_ADMIN_PASSWORD` are not set, or if the requested email is already
  registered to a non-admin user.

Run this once against each real deployment's database after the schema
migration (`alembic upgrade head`) has been applied, then log in via
`POST /api/auth/login` and use `POST /api/auth/admin/users` for all
subsequent staff/provider/admin account creation.
