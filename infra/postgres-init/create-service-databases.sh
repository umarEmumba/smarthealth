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
# Plan 03-05 adds (all from Compose secrets, read from files, never echoed):
#   - role `debezium` (LOGIN REPLICATION): the CDC relay. CONNECT on the four outbox
#     databases only; its SELECT on each `outbox` table is granted by that
#     service's migration (libs.events.migration.create_outbox), which also
#     creates the publication as the table owner.
#   - role `grafana_ro` (LOGIN): read-only Grafana access to analytics_db.
#
# NEVER add `set -x` here: this script handles passwords.
set -euo pipefail

SERVICES=(identity organization provider patient scheduling visit billing analytics)

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

# --- Plan 03-05: CDC and read-only roles ------------------------------------
# Debezium's password is the `password=` line of a Compose secret file; Grafana's
# is the whole secret file. Both reach psql through the environment (\getenv),
# never a command line, and are never printed.
DEBEZIUM_SECRET_FILE=/run/secrets/debezium.properties
GRAFANA_RO_SECRET_FILE=/run/secrets/grafana_ro.secret
for secret_file in "$DEBEZIUM_SECRET_FILE" "$GRAFANA_RO_SECRET_FILE"; do
  if [ ! -s "$secret_file" ]; then
    echo "ERROR: secret file ${secret_file} is missing or empty (run scripts/generate_infra_secrets.py)" >&2
    exit 1
  fi
done
DEBEZIUM_PW=$(sed -n 's/^password=//p' "$DEBEZIUM_SECRET_FILE" | head -n 1)
GRAFANA_RO_PW=$(cat "$GRAFANA_RO_SECRET_FILE")
if [ -z "$DEBEZIUM_PW" ] || [ -z "$GRAFANA_RO_PW" ]; then
  echo "ERROR: a CDC/read-only role password is empty" >&2
  exit 1
fi

DEBEZIUM_PW="$DEBEZIUM_PW" GRAFANA_RO_PW="$GRAFANA_RO_PW" psql -v ON_ERROR_STOP=1 --dbname postgres <<'EOSQL'
\getenv dbz_pw DEBEZIUM_PW
\getenv gro_pw GRAFANA_RO_PW

-- debezium: replicate + log in, nothing else. Create if absent, always resync.
SELECT format('CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD %L', :'dbz_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') \gexec
SELECT format('ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD %L', :'dbz_pw') \gexec
-- Each outbox database revoked PUBLIC connect above, so CONNECT is required.
GRANT CONNECT ON DATABASE scheduling_db, visit_db, provider_db, patient_db TO debezium;

-- grafana_ro: read-only dashboards over analytics_db.
SELECT format('CREATE ROLE grafana_ro WITH LOGIN PASSWORD %L', :'gro_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') \gexec
SELECT format('ALTER ROLE grafana_ro WITH LOGIN PASSWORD %L', :'gro_pw') \gexec
GRANT CONNECT ON DATABASE analytics_db TO grafana_ro;
EOSQL
echo "roles debezium and grafana_ro ready"

# Inside analytics_db (superuser): schema usage plus default SELECT on every table
# the analytics service role creates later. Idempotent.
psql -v ON_ERROR_STOP=1 --dbname analytics_db <<'EOSQL'
GRANT USAGE ON SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE analytics_svc IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
EOSQL
echo "grafana_ro read access to analytics_db ready"
