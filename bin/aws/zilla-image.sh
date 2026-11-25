#!/usr/bin/env bash
# Puts a locally built Zilla image on the broker host without a registry.
#
# Usage: docker save <image> | gzip > zilla-image.tar.gz
#        bin/aws/zilla-image.sh zilla-image.tar.gz
# The archive goes to s3://$BUCKET/images/zilla-image.tar.gz, and the controller streams it from there into
# `docker load` on the broker host. The broker host needs Docker, which deploy.sh installs: deploy once, load the
# image, then deploy again with the image's tag and the pull skipped (the Loaded image line above says the tag):
#   bin/aws/deploy.sh -e zilla_image=<tag> -e zilla_image_loaded=true
set -euo pipefail
. "$(dirname "$0")/common.sh"
ARCHIVE=${1:?usage: bin/aws/zilla-image.sh <docker-save.tar.gz>}
[ -f "$ARCHIVE" ] || { echo "$ARCHIVE not found" >&2; exit 2; }
s3cp put "$ARCHIVE" images/zilla-image.tar.gz
ssm --timeout "${SSM_TIMEOUT:-3600}" "set -o pipefail
broker=\$(grep -A1 '^\[broker\]' $SRC/$INVENTORY_PRIVATE | sed -n 's/.*ansible_host=\([^ ]*\).*/\1/p')
[ -n \"\$broker\" ] || { echo 'no broker in the inventory: run bin/aws/push-bundle.sh' >&2; exit 1; }
aws s3 cp s3://$BUCKET/images/zilla-image.tar.gz - --only-show-errors | ssh -i $KEY ubuntu@\$broker 'sudo docker load'"
