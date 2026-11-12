#!/bin/sh
# Run one load-test profile with k6 from its official, pinned Docker image
# (plan 02.1-04). No k6 or Node install is needed on the host.
#
#   loadtest/run.sh <smoke|sustained|burst|contention>
#
# Environment:
#   GATEWAY_URL   gateway the k6 container calls
#                 (default http://host.docker.internal:8000)
#   PLAN_OFFSET   optional first booking-plan entry to use (see loadtest/README.md)
#
# The script exits with k6's exit code, so a crossed threshold (the NFR targets in
# loadtest/nfr.json) is a non-zero exit. Exit 2 means a usage or setup problem.
#
# LOCAL STACK ONLY. This sends thousands of requests; never point it at a shared or
# production environment. The gateway rate budget must be raised for the run first
# (loadtest/README.md).
set -eu

# Pinned, never `latest`. Verified with `docker manifest inspect` on 2026-10-25
# (recorded in loadtest/README.md).
K6_IMAGE="grafana/k6:1.8.2"

usage() {
  echo "usage: $0 <smoke|sustained|burst|contention>" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage
PROFILE="$1"
case "$PROFILE" in
  smoke | sustained | burst | contention) ;;
  *) usage ;;
esac

LOADTEST_DIR=$(cd "$(dirname "$0")" && pwd)
if [ ! -f "$LOADTEST_DIR/data/plan.json" ]; then
  echo "ERROR: $LOADTEST_DIR/data/plan.json is missing; run scripts/loadtest_seed.py first" >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || {
  echo "ERROR: docker is required (k6 runs from the $K6_IMAGE image)" >&2
  exit 2
}

GATEWAY_URL="${GATEWAY_URL:-http://host.docker.internal:8000}"

# --user: plan.json is mode 0600, so the container must read it as the host user.
# --add-host: makes host.docker.internal resolve on Linux (Docker Desktop has it).
if [ -n "${PLAN_OFFSET:-}" ]; then
  exec docker run --rm \
    --user "$(id -u):$(id -g)" \
    --add-host=host.docker.internal:host-gateway \
    -v "$LOADTEST_DIR:/loadtest:ro" \
    -e HOME=/tmp \
    -e K6_NO_USAGE_REPORT=true \
    -e PROFILE="$PROFILE" \
    -e GATEWAY_URL="$GATEWAY_URL" \
    -e PLAN_OFFSET="$PLAN_OFFSET" \
    "$K6_IMAGE" run /loadtest/booking.js
fi
exec docker run --rm \
  --user "$(id -u):$(id -g)" \
  --add-host=host.docker.internal:host-gateway \
  -v "$LOADTEST_DIR:/loadtest:ro" \
  -e HOME=/tmp \
  -e K6_NO_USAGE_REPORT=true \
  -e PROFILE="$PROFILE" \
  -e GATEWAY_URL="$GATEWAY_URL" \
  "$K6_IMAGE" run /loadtest/booking.js
