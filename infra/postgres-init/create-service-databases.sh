#!/usr/bin/env bash
# Idempotent database-per-service bootstrap (D-01).
#
# For each service this creates ONE role (<name>_svc) and ONE database
# (<name>_db) owned by it, then revokes PUBLIC access so no other role can even
# connect to it. Every statement is idempotent, so the script is safe to run
# on every `docker compose up` and against an EXISTING postgres volume — which
# is the whole point: the stock `/docker-entrypoint-initdb.d` mechanism only
# runs on an EMPTY data directory (RESEARCH Pitfall 3), so it would silently do
# nothing on a developer's machine that already has Phase 1 data. The legacy
# single database is left untouched (non-destructive; no `down -v` needed).
#
# Connection comes from the standard PG* environment (PGHOST, PGPORT, PGUSER,
# PGPASSWORD). Each service password comes from <NAME>_DB_PASSWORD and is
# passed to psql through the environment (\getenv), never on a command line.
#
# NEVER add `set -x` here: this script handles passwords.
set -euo pipefail

SERVICES=(identity organization provider patient scheduling)

for svc in "${SERVICES[@]}"; do
  upper=$(printf '%s' "$svc" | tr '[:lower:]' '[:upper:]')
  pw_var="${upper}_DB_PASSWORD"
  if [ -z "${!pw_var:-}" ]; then
    echo "ERROR: ${pw_var} must be set (password for role ${svc}_svc)" >&2
    exit 1
  fi

  SVC_PW="${!pw_var}" psql -v ON_ERROR_STOP=1 \
    -v "svc_role=${svc}_svc" -v "svc_db=${svc}_db" \
    --dbname postgres <<'EOSQL'
\getenv svc_pw SVC_PW

-- Role: create if absent, then always resync the password.
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'svc_role', :'svc_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'svc_role') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'svc_role', :'svc_pw') \gexec

-- Database: create if absent, owned by the service role.
SELECT format('CREATE DATABASE %I OWNER %I', :'svc_db', :'svc_role')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'svc_db') \gexec

-- Isolation: nobody but the owner may connect.
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'svc_db') \gexec
SELECT format('GRANT ALL ON DATABASE %I TO %I', :'svc_db', :'svc_role') \gexec
EOSQL

  echo "database ${svc}_db ready (owner ${svc}_svc)"
done

# Scheduling owns the GiST exclusion constraint on provider_slots, which needs
# btree_gist. Created here as the superuser so the service role never needs
# extension privileges.
psql -v ON_ERROR_STOP=1 --dbname scheduling_db -c 'CREATE EXTENSION IF NOT EXISTS btree_gist'
echo "btree_gist ready in scheduling_db"
