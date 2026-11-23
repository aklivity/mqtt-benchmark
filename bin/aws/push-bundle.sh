#!/usr/bin/env bash
# Uploads what the controller runs (deploy/ansible, bin, scenarios and deploy/inventory-private.ini from Terraform)
# to s3://$BUCKET/bundle.tgz and unpacks it on the controller into /opt/mqtt-benchmark-src. Run it again after any
# local change; the other bin/aws scripts do it for you.
set -euo pipefail
. "$(dirname "$0")/common.sh"
cd "$REPO"
[ -f deploy/inventory-private.ini ] || { echo "deploy/inventory-private.ini is missing: terraform apply in deploy/terraform first" >&2; exit 1; }
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
COPYFILE_DISABLE=1 tar czf "$WORK/bundle.tgz" --exclude __pycache__ deploy/ansible bin scenarios deploy/inventory-private.ini
s3cp put "$WORK/bundle.tgz" bundle.tgz
ssm --timeout 600 "aws s3 cp s3://$BUCKET/bundle.tgz - --only-show-errors | tar xz -C $SRC && echo 'bundle unpacked in $SRC'"
