#!/usr/bin/env bash
# Register Debezium outbox connectors and wait for their TASKS to run (plan 03-05).
#
#   register-connectors.sh                  # every /connect-init/connectors/*.json
#   register-connectors.sh scheduling-outbox patient-outbox   # only these names
#
# Registration is an idempotent `PUT /connectors/<name>/config`, so re-running it
# (every `docker compose up`) updates a connector in place. It then polls
# `GET /connectors/<name>/status` every 3 s for up to 120 s until the connector
# AND every one of its tasks report RUNNING. The connector state alone is not
# enough: a missing publication or table leaves the connector RUNNING while its
# task is FAILED (research Pitfall 2).
#
# On FAILED or timeout this prints the connector name and the states only, never
# the stack trace or the config (the config references a secret file, but a trace
# can echo connection details). Exit 1.
#
# The database password never appears here or in the JSON: the connector config
# carries a FileConfigProvider reference resolved inside Connect.
#
# NEVER add `set -x` here.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/connect-init/connectors}"
POLL_SECONDS=3
TIMEOUT_SECONDS=120

names=()
if [ "$#" -gt 0 ]; then
  names=("$@")
else
  shopt -s nullglob
  for file in "$CONNECTORS_DIR"/*.json; do
    base=$(basename "$file" .json)
    names+=("$base")
  done
  shopt -u nullglob
fi

if [ "${#names[@]}" -eq 0 ]; then
  echo "no connector files found in ${CONNECTORS_DIR}; nothing to register"
  exit 0
fi

# Prints "<connector-state> <task-state> ..." (one line) for $1. The status JSON
# lists the connector state first, then one state per task; grep keeps this free
# of a JSON tool (the Debezium image ships curl and bash, not python).
status_line() {
  local body states
  body=$(curl -fs "${CONNECT_URL}/connectors/$1/status") || return 1
  states=$(printf '%s' "$body" | grep -o '"state":"[A-Z_]*"' | sed 's/"state":"\(.*\)"/\1/' | tr '\n' ' ')
  # A connector with no task yet reports one state only: treat as not ready.
  if [ "$(printf '%s' "$states" | wc -w)" -lt 2 ]; then
    states="${states}NO_TASKS"
  fi
  printf '%s\n' "$states"
}

for name in "${names[@]}"; do
  file="${CONNECTORS_DIR}/${name}.json"
  if [ ! -f "$file" ]; then
    echo "ERROR: connector file for '${name}' not found" >&2
    exit 1
  fi
  if ! curl -fs -o /dev/null -X PUT -H 'Content-Type: application/json' \
      --data-binary "@${file}" "${CONNECT_URL}/connectors/${name}/config"; then
    echo "ERROR: could not register connector '${name}'" >&2
    exit 1
  fi
  echo "connector ${name} registered"
done

for name in "${names[@]}"; do
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while true; do
    states=$(status_line "$name" 2>/dev/null || echo "UNREACHABLE")
    all_running=1
    for state in $states; do
      [ "$state" = "RUNNING" ] || all_running=0
    done
    case "$states" in *NO_TASKS*) all_running=0 ;; esac
    if [ "$all_running" -eq 1 ]; then
      echo "connector ${name} RUNNING (connector and all tasks): ${states}"
      break
    fi
    case " $states " in
      *" FAILED "*)
        echo "ERROR: connector ${name} FAILED: ${states}" >&2
        exit 1
        ;;
    esac
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "ERROR: connector ${name} not RUNNING within ${TIMEOUT_SECONDS}s: ${states}" >&2
      exit 1
    fi
    sleep "$POLL_SECONDS"
  done
done
