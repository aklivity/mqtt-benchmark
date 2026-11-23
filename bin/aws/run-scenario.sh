#!/usr/bin/env bash
# Runs bin/run-scenario.sh on the controller and brings results/<run-id> back into the local results/ directory.
#
# Usage: bin/aws/run-scenario.sh scenarios/<name>.env
#   DIAG, BENCH_IMAGE, RUN_ID  forwarded to bin/run-scenario.sh when set
#   SSM_TIMEOUT                seconds the run may take (default 14400)
# The scenario must be a file of this repo's scenarios/ directory: that is what push-bundle.sh sends. Output
# arrives when the run ends; the exit code is bin/run-scenario.sh's. Results come back even after a failed run.
set -euo pipefail
. "$(dirname "$0")/common.sh"
SCENARIO=${1:?usage: bin/aws/run-scenario.sh scenarios/<name>.env}
cd "$REPO"
[ -f "$SCENARIO" ] && [ "$(cd "$(dirname "$SCENARIO")" && pwd)" = "$REPO/scenarios" ] \
  || { echo "$SCENARIO is not a file in $REPO/scenarios" >&2; exit 2; }
NAME=$(. "$SCENARIO" && echo "${NAME:?$SCENARIO sets no NAME}")
# fixed here, not by bin/run-scenario.sh, to know which results directory to fetch
RUN_ID=${RUN_ID:-$NAME-$(date -u +%Y%m%dT%H%M%SZ)}
export RUN_ID

"$AWS_BIN/push-bundle.sh"
rc=0
ssm --timeout "${SSM_TIMEOUT:-14400}" "cd $SRC
INVENTORY=$INVENTORY_PRIVATE SSH_KEY=$KEY $(env_args RUN_ID DIAG BENCH_IMAGE)bin/run-scenario.sh scenarios/$(basename "$SCENARIO")
rc=\$?
if [ -d results/$RUN_ID ]; then
  tar czf /tmp/$RUN_ID.tgz -C results $RUN_ID && aws s3 cp /tmp/$RUN_ID.tgz s3://$BUCKET/results/$RUN_ID.tgz --only-show-errors
  rm -f /tmp/$RUN_ID.tgz
fi
exit \$rc" || rc=$?

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
if s3cp get "results/$RUN_ID.tgz" "$WORK/results.tgz"; then
  mkdir -p results && tar xzf "$WORK/results.tgz" -C results
  echo "results in results/$RUN_ID" >&2
else
  echo "no results for $RUN_ID could be fetched" >&2
fi
exit "$rc"
