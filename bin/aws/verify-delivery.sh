#!/usr/bin/env bash
# Runs bin/verify-delivery.sh on the controller. It prints its report and ends with PASS or FAIL (exit code 1), and
# writes no results directory.
#
# Usage: bin/aws/verify-delivery.sh            QOS=2 bin/aws/verify-delivery.sh
#   QOS, KAFKA_TOPIC, QOS2_RECONNECT_N  forwarded to bin/verify-delivery.sh when set
#   SSM_TIMEOUT                         seconds the check may take (default 3600)
set -euo pipefail
. "$(dirname "$0")/common.sh"
"$AWS_BIN/push-bundle.sh"
ssm --timeout "${SSM_TIMEOUT:-3600}" \
  "cd $SRC && INVENTORY=$INVENTORY_PRIVATE SSH_KEY=$KEY $(env_args QOS KAFKA_TOPIC QOS2_RECONNECT_N)bin/verify-delivery.sh"
