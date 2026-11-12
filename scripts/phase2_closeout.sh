#!/usr/bin/env bash
# Phase 2 close-out run: stack check, full regression, e2e, semgrep.
# Run from the repo root on the Mac with Docker running:  scripts/phase2_closeout.sh
# Results are written to .planning/phases/02-scheduling-service-workflow/02-CLOSEOUT-RUN.md
# (nothing is marked complete automatically; read the file, then tell Claude).
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=.planning/phases/02-scheduling-service-workflow/02-CLOSEOUT-RUN.md
LOGDIR=.planning/phases/02-scheduling-service-workflow/closeout-logs
mkdir -p "$LOGDIR"
[ -f .venv/bin/activate ] && . .venv/bin/activate

{
  echo "# Phase 2 close-out run"
  echo "Started: $(date -u +%FT%TZ)  Commit: $(git rev-parse --short HEAD)"
  echo
} > "$OUT"

step() { # name, command...
  local name="$1"; shift
  local log="$LOGDIR/$name.log"
  echo ">>> $name"
  local t0=$SECONDS
  "$@" > "$log" 2>&1; local rc=$?
  local tail_line; tail_line=$(grep -E "passed|failed|error|OK|Healthy|healthy" "$log" | tail -1)
  echo "- **$name**: exit $rc in $((SECONDS-t0))s — ${tail_line:-see log} (\`$log\`)" >> "$OUT"
  echo "    exit $rc"
}

step 01-stack-health   scripts/compose_healthcheck.sh --wait 120
step 02-regression     pytest tests -m "not e2e" -q -p no:cacheprovider
GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 SMARTHEALTH_E2E_REQUIRE_STACK=1 \
  step 03-e2e          pytest tests/e2e -m e2e -q -p no:cacheprovider
if command -v semgrep >/dev/null; then
  step 04-semgrep      semgrep scan --config auto --error services libs scripts
else
  echo "- **04-semgrep**: SKIPPED (not installed: pip install semgrep)" >> "$OUT"
fi
echo >> "$OUT"; echo "Finished: $(date -u +%FT%TZ)" >> "$OUT"
echo "Done. Results in $OUT"
