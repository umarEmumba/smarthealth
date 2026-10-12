#!/usr/bin/env bash
# OBS-04 smoke check: assert Compose services report "healthy".
#
# Usage:
#   scripts/compose_healthcheck.sh [--wait SECONDS] [service ...]
#
#   --wait SECONDS   poll every 5s until everything checked is healthy, or fail
#                    once SECONDS have elapsed (default: check once, no waiting).
#   service ...      check ONLY these services (each must be present and healthy).
#                    With no names, the six application services
#                    (gateway identity organization provider patient scheduling)
#                    must be present AND healthy, and every other service that
#                    declares a healthcheck must be healthy too. A required
#                    service that is missing is a failure, not a skip.
#
# The compose file defaults to infra/docker-compose.yml; override with
# COMPOSE_FILE=/path/to/file.yml (and optionally COMPOSE_PROJECT_NAME).
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.yml}"
REQUIRED_DEFAULT=(gateway identity organization provider patient scheduling)
POLL_SECONDS=5

WAIT_SECONDS=0
SERVICES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --wait)
      if [ $# -lt 2 ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: --wait needs a number of seconds" >&2
        exit 2
      fi
      WAIT_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,17p' "$0"
      exit 0
      ;;
    -*)
      echo "ERROR: unknown option: $1" >&2
      exit 2
      ;;
    *)
      SERVICES+=("$1")
      shift
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed or not on PATH" >&2
  exit 1
fi

# Reads `docker compose ps --format json` on stdin; prints one line per finding
# and exits non-zero when anything checked is missing or not healthy.
# argv: mode ("named"|"all") followed by the services that must be present.
PY_EVALUATE="$(cat <<'PY'
import json
import sys

mode, required = sys.argv[1], sys.argv[2:]
raw = sys.stdin.read().strip()
rows = []
if raw:
    try:
        parsed = json.loads(raw)
        rows = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:  # older/newer compose: one JSON object per line
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]

by_service = {r.get("Service", ""): r for r in rows}
failed = False

for name in required:
    if name not in by_service:
        print("missing: " + name + " (not created by this compose project)", file=sys.stderr)
        failed = True

checked = list(required)
if mode == "all":
    checked += [n for n in by_service if n not in required]

for name in checked:
    row = by_service.get(name)
    if row is None:
        continue
    health = row.get("Health", "")
    state = row.get("State", "") or "unknown"
    if health == "healthy":
        print("healthy: " + name)
    elif health:
        print("UNHEALTHY: " + name + " (status: " + health + ")", file=sys.stderr)
        failed = True
    elif name in required:
        # Required but declares no healthcheck: it must at least be running.
        if state == "running":
            print("healthy: " + name + " (running, no healthcheck)")
        else:
            print("UNHEALTHY: " + name + " (state: " + state + ")", file=sys.stderr)
            failed = True
    # Not required and no healthcheck declared: not part of the OBS-04 gate.

sys.exit(1 if failed else 0)
PY
)"

evaluate() {
  docker compose -f "$COMPOSE_FILE" ps -a --format json | python3 -c "$PY_EVALUATE" "$@"
}

if [ "${#SERVICES[@]}" -gt 0 ]; then
  MODE=named
  REQUIRED=("${SERVICES[@]}")
else
  MODE=all
  REQUIRED=("${REQUIRED_DEFAULT[@]}")
fi

DEADLINE=$(( $(date +%s) + WAIT_SECONDS ))
while true; do
  if OUTPUT="$(evaluate "$MODE" "${REQUIRED[@]}" 2>&1)"; then
    printf '%s\n' "$OUTPUT"
    if [ "$MODE" = "all" ]; then
      echo "OK: every required service and every service with a healthcheck is healthy"
    else
      echo "OK: ${REQUIRED[*]} healthy"
    fi
    exit 0
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    printf '%s\n' "$OUTPUT" >&2
    echo "FAIL: not healthy within ${WAIT_SECONDS}s" >&2
    exit 1
  fi
  sleep "$POLL_SECONDS"
done
