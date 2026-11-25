#!/usr/bin/env bash
# Runs a list of scenarios in sequence ON THE CONTROLLER, so the sequence survives the operator host (see docs/aws-no-ssh.md).
# Per scenario: clean deploy -> client and broker health every 60 s -> bin/run-scenario.sh with DIAG=1 -> per-worker CPU
# (top -H) and, for the 100K scenarios, a thread-dump profile (bin/agg-dumps.py) during the window -> end offsets of the four
# topics -> results tarball to s3://$BUCKET/results/<run>.tgz, progress to s3://$BUCKET/status/driver.log.
#
# Usage, from the source directory on the controller (default /opt/mqtt-benchmark-src, where bin/aws/push-bundle.sh unpacks):
#   BUCKET=<bucket> DEPLOY_ARGS='-e zilla_cache_bootstrap=false' nohup bin/table-driver.sh <scenario-file-stem>... &
# DEPLOY_ARGS goes to ansible-playbook word-split, so a value with spaces must go in a vars file: DEPLOY_ARGS='-e @/tmp/vars.yml'.
# /tmp/health.stop ends the health loop; the sequence keeps running if the operator host disconnects.
set -u
: "${BUCKET:?set BUCKET (terraform output bucket_name in deploy/terraform-controller)}"
cd "${SRC:-/opt/mqtt-benchmark-src}"
KEY=/home/ubuntu/.ssh/id_ed25519; INV=deploy/inventory-private.ini
SSHO=(-i "$KEY" -o StrictHostKeyChecking=no -o LogLevel=ERROR -o ConnectTimeout=8)
grp() { awk -v g="[$1]" '/^\[/{f=($0==g); next} f&&NF{for(i=2;i<=NF;i++){split($i,kv,"=");if(kv[1]=="private_ip")print kv[2]}}' "$INV"; }
CLIENTS=$(grp clients); BROKER=$(grp broker | head -1); KAFKA=$(grp kafka | head -1)
st() { echo "$(date -u +%T) $*" | tee -a /tmp/driver.log; aws s3 cp /tmp/driver.log "s3://$BUCKET/status/driver.log" --only-show-errors || true; }

health() {  # health <run>: every 60 s docker state, memory and OOM kills of every client host and the broker
  while [ ! -e /tmp/health.stop ]; do
    for h in $CLIENTS $BROKER; do
      echo "$(date -u +%T) $h $(ssh "${SSHO[@]}" ubuntu@$h 'docker ps --format "{{.Names}}={{.Status}}" | tr "\n" ";"; free -m | awk "NR==2{printf \" mem_used_mb=%s avail_mb=%s\", \$3, \$7}"; echo -n " oom_lines="; sudo dmesg 2>/dev/null | grep -ciE "out of memory|oom-kill|killed process"' 2>&1 | tr '\n' ' ')" >> "/tmp/health-$1.log"
    done
    sleep 60
  done
}

windowprobes() {  # windowprobes <run> <profile:0|1>: starts when the run logs 'measuring for'
  until grep -q "measuring for" "results/$1/run.log" 2>/dev/null || [ -e /tmp/health.stop ]; do sleep 2; done
  [ -e /tmp/health.stop ] && return
  sleep 30
  ssh "${SSHO[@]}" ubuntu@$BROKER 'top -b -H -n 3 -d 20 -w 160' > "/tmp/top-$1.txt" 2>&1 || true
  if [ "$2" = 1 ]; then
    ssh "${SSHO[@]}" ubuntu@$BROKER 'PID=$(pgrep -x java | head -1); N0=$(docker logs zilla 2>&1 | wc -l); for i in $(seq 1 60); do sudo kill -QUIT $PID; sleep 1; done; docker logs zilla 2>&1 | tail -n +$((N0+1)) > /tmp/dumps.txt' >/dev/null 2>&1 || true
    scp "${SSHO[@]}" bin/agg-dumps.py ubuntu@$BROKER:/tmp/agg-dumps.py >/dev/null 2>&1 || true
    ssh "${SSHO[@]}" ubuntu@$BROKER 'python3 -I /tmp/agg-dumps.py /tmp/dumps.txt 10' > "/tmp/profile-$1.txt" 2>&1 || true
  fi
}

for s in "$@"; do
  RUN="$s-$(date -u +%Y%m%dT%H%M%SZ)"; rm -f /tmp/health.stop
  st "deploy for $s ($RUN)"
  (cd deploy/ansible && ansible-playbook deploy.yaml -i ../inventory-private.ini --private-key "$KEY" $DEPLOY_ARGS) > "/tmp/deploy-$RUN.log" 2>&1 || st "DEPLOY FAILED for $s"
  mkdir -p "results/$RUN"
  health "$RUN" & HP=$!
  PROF=0; case "$s" in *mqtt-kafka-100k*) PROF=1;; esac   # thread-dump profile only for the 100K scenarios
  windowprobes "$RUN" "$PROF" & WP=$!
  st "run $s"
  INVENTORY=$INV SSH_KEY=$KEY RUN_ID=$RUN DIAG=1 bin/run-scenario.sh "scenarios/$s.env" > "/tmp/run-$RUN.log" 2>&1; rc=$?
  touch /tmp/health.stop; wait $HP $WP 2>/dev/null
  mkdir -p "results/$RUN"
  cp "/tmp/health-$RUN.log" "/tmp/top-$RUN.txt" "/tmp/profile-$RUN.txt" "/tmp/deploy-$RUN.log" "/tmp/run-$RUN.log" "results/$RUN/" 2>/dev/null
  # per-worker CPU table: average of the last 2 top intervals, worker#index=cpu% (columns assume top's default layout)
  awk '/^top -/{n++} n>=2 && /engine\/worker#/{s[$12]+=$9; c[$12]++} END{for(w in s) printf "%s %.0f\n", w, s[w]/c[w]}' "/tmp/top-$RUN.txt" 2>/dev/null | sort -t'#' -k2 -n | awk '{split($1,a,"#"); printf "#%s=%s ", a[2], $2} END{print ""}' > "results/$RUN/worker-cpu.txt"
  for t in mqtt-devices mqtt-sessions mqtt-retained mqtt-messages; do echo -n "$t=" >> "results/$RUN/offsets-all.txt"; ssh "${SSHO[@]}" ubuntu@$KAFKA "docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $t" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}' >> "results/$RUN/offsets-all.txt"; done
  tar czf "/tmp/$RUN.tgz" -C results "$RUN" && aws s3 cp "/tmp/$RUN.tgz" "s3://$BUCKET/results/$RUN.tgz" --only-show-errors
  st "done $s rc=$rc ($RUN)"
done
st "ALL DONE"
