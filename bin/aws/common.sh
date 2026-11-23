# Sourced by the other bin/aws/*.sh scripts: where things are, and the settings every one of them needs.
#   CONTROLLER_ID, BUCKET   terraform output of deploy/terraform-controller
#   AWS_REGION              region of the benchmark
#   credentials             AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (boto3's usual chain)
AWS_BIN=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$AWS_BIN/../.." && pwd)
: "${CONTROLLER_ID:?set CONTROLLER_ID (terraform output controller_id in deploy/terraform-controller)}"
: "${BUCKET:?set BUCKET (terraform output bucket_name in deploy/terraform-controller)}"
: "${AWS_REGION:?set AWS_REGION}"
export CONTROLLER_ID BUCKET AWS_REGION

# On the controller: where push-bundle.sh unpacks the repo, the private key, and the inventory with private addresses
SRC=/opt/mqtt-benchmark-src
KEY=/home/ubuntu/.ssh/id_ed25519
INVENTORY_PRIVATE=deploy/inventory-private.ini

ssm() { "$AWS_BIN/ssm-run.py" "$@"; }
s3cp() { "$AWS_BIN/s3cp.py" "$@"; }
# env_args VAR...: "VAR=value " (shell-quoted) for each of the variables that is set, to forward them to the controller
env_args() {
  local v
  for v in "$@"; do if [ -n "${!v+x}" ]; then printf '%s=%q ' "$v" "${!v}"; fi; done
}
