#!/usr/bin/env bash
# Runs deploy/ansible/deploy.yaml from the controller, over private addresses. Arguments go to ansible-playbook,
# normally -e extra vars:
#   bin/aws/deploy.sh                                   Zilla
#   bin/aws/deploy.sh -e broker=emqx                    EMQX baseline
#   bin/aws/deploy.sh -e zilla_diagnostics=true -e mqtt_devices_partitions=32
# SSM_TIMEOUT (seconds, default 7200) bounds the run.
set -euo pipefail
. "$(dirname "$0")/common.sh"
"$AWS_BIN/push-bundle.sh"
# printf ' %q' with no arguments still prints an empty quoted argument, which ansible-playbook rejects
EXTRA=""
if [ "$#" -gt 0 ]; then
  EXTRA=$(printf ' %q' "$@")
fi
ssm --timeout "${SSM_TIMEOUT:-7200}" \
  "cd $SRC/deploy/ansible && ansible-playbook deploy.yaml -i ../inventory-private.ini --private-key $KEY$EXTRA"
