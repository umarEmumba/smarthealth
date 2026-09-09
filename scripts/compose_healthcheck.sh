#!/usr/bin/env bash
# OBS-04 smoke check: assert every Compose service that declares a
# healthcheck reports "healthy". Services with no healthcheck (e.g. `api`,
# which has no application code wired in yet this phase) are not checked
# here — they are covered by later phases' own verification.
#
# Usage: scripts/compose_healthcheck.sh [compose-file]
set -euo pipefail

COMPOSE_FILE="${1:-infra/docker-compose.yml}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed or not on PATH" >&2
  exit 1
fi

STATUS_JSON="$(docker compose -f "$COMPOSE_FILE" ps --format json)"

FAILED=0
# `docker compose ps --format json` emits one JSON object per line.
while IFS= read -r line; do
  [ -z "$line" ] && continue
  name=$(printf '%s' "$line" | python3 -c "import json,sys; print(json.load(sys.stdin).get('Service',''))")
  health=$(printf '%s' "$line" | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))")

  if [ -z "$health" ]; then
    # No healthcheck declared for this service — not part of the OBS-04 gate.
    continue
  fi

  if [ "$health" != "healthy" ]; then
    echo "UNHEALTHY: $name (status: $health)" >&2
    FAILED=1
  else
    echo "healthy: $name"
  fi
done <<< "$STATUS_JSON"

if [ "$FAILED" -ne 0 ]; then
  echo "FAIL: one or more services with a healthcheck are not healthy" >&2
  exit 1
fi

echo "OK: every service with a healthcheck is healthy"
