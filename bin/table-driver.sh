#!/usr/bin/env bash
# Runs a list of scenarios in sequence ON THE CONTROLLER, so the sequence survives the operator host (see docs/aws-no-ssh.md).
# Per scenario: clean deploy -> client and broker health every 60 s -> bin/run-scenario.sh with DIAG=1 (which also runs the
# host recorder, see bin/run-scenario.sh) and, for the 100K scenarios, 10 thread dumps 20 s into the connection ramp and 60 dumps
# from t0+30 s inside the window (bin/agg-dumps.py profile, dumps-*.txt.gz) -> per-worker CPU from the recorder joined to the
# dumps (bin/worker-cpu.py) -> end offsets of the four topics -> results tarball to s3://$BUCKET/results/<run>.tgz, progress to
# s3://$BUCKET/status/driver.log.
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

take_dumps() {  # take_dumps <count> <name>: <count> thread dumps of the broker JVM, one a second, with timestamps, to /tmp/dumps-<name>.txt there
  ssh "${SSHO[@]}" ubuntu@$BROKER 'PID=$(pgrep -x java | head -1); N0=$(docker logs zilla 2>&1 | wc -l); for i in $(seq 1 '"$1"'); do sudo kill -QUIT $PID; sleep 1; done; sleep 1; docker logs --timestamps zilla 2>&1 | tail -n +$((N0+1)) > /tmp/dumps-'"$2"'.txt' >/dev/null 2>&1 || true
}

windowprobes() {  # windowprobes <run> <profile:0|1> <duration_s>: thread dumps at fixed offsets taken from the run's own events.txt
  local E="results/$1/events.txt" off n wait_s
  evt() { awk -v k="$1" '$2==k {print $1}' "$E" 2>/dev/null | tail -1; }
  until [ -n "$(evt ramp_start)" ] || [ -e /tmp/health.stop ]; do sleep 1; done
  [ "$2" = 1 ] || return 0
  # a burst 20 s into the connection ramp, to see the divergence window and not only the middle of the run
  sleep 20; [ -e /tmp/health.stop ] && return
  take_dumps 10 ramp
  until [ -n "$(evt t0)" ] || [ -e /tmp/health.stop ]; do sleep 1; done
  [ -e /tmp/health.stop ] && return
  off=$(( $3 / 4 < 30 ? $3 / 4 : 30 )); n=$(( $3 - off - 15 > 60 ? 60 : $3 - off - 15 )); [ "$n" -ge 5 ] || n=5
  wait_s=$(( off - ($(date +%s%3N) - $(evt t0)) / 1000 )); [ "$wait_s" -gt 0 ] && sleep "$wait_s"   # the dumps start at t0 + off
  take_dumps "$n" window
  scp "${SSHO[@]}" bin/agg-dumps.py ubuntu@$BROKER:/tmp/agg-dumps.py >/dev/null 2>&1 || true
  ssh "${SSHO[@]}" ubuntu@$BROKER 'python3 -I /tmp/agg-dumps.py /tmp/dumps-window.txt 10' > "/tmp/profile-$1.txt" 2>&1 || true
  for k in ramp window; do ssh "${SSHO[@]}" ubuntu@$BROKER "gzip -c /tmp/dumps-$k.txt" > "/tmp/dumps-$k-$1.txt.gz" 2>/dev/null || true; done
}

for s in "$@"; do
  RUN="$s-$(date -u +%Y%m%dT%H%M%SZ)"; rm -f /tmp/health.stop
  st "deploy for $s ($RUN)"
  (cd deploy/ansible && ansible-playbook deploy.yaml -i ../inventory-private.ini --private-key "$KEY" $DEPLOY_ARGS) > "/tmp/deploy-$RUN.log" 2>&1 || st "DEPLOY FAILED for $s"
  mkdir -p "results/$RUN"
  health "$RUN" & HP=$!
  PROF=0; case "$s" in *mqtt-kafka-100k*) PROF=1;; esac   # thread-dump profile only for the 100K scenarios
  DUR=$( (source "scenarios/$s.env" >/dev/null 2>&1; echo "${DURATION_S:-1800}") )
  windowprobes "$RUN" "$PROF" "$DUR" & WP=$!
  st "run $s"
  INVENTORY=$INV SSH_KEY=$KEY RUN_ID=$RUN DIAG=1 bin/run-scenario.sh "scenarios/$s.env" > "/tmp/run-$RUN.log" 2>&1; rc=$?
  touch /tmp/health.stop; wait $HP $WP 2>/dev/null
  mkdir -p "results/$RUN"
  cp "/tmp/health-$RUN.log" "/tmp/profile-$RUN.txt" "/tmp/deploy-$RUN.log" "/tmp/run-$RUN.log" "results/$RUN/" 2>/dev/null
  for k in ramp window; do cp "/tmp/dumps-$k-$RUN.txt.gz" "results/$RUN/dumps-$k.txt.gz" 2>/dev/null || true; done
  # per-worker cpu from the recorder's per-thread samples joined to the dumps' thread ids (see bin/worker-cpu.py)
  python3 -I bin/worker-cpu.py "results/$RUN" --series "results/$RUN/worker-cpu-series.tsv" > "results/$RUN/worker-cpu.txt" 2>&1 || true
  python3 -I bin/tmpfs-summary.py "results/$RUN" > "results/$RUN/tmpfs-summary.txt" 2>&1 || true
  for t in mqtt-devices mqtt-sessions mqtt-retained mqtt-messages; do echo -n "$t=" >> "results/$RUN/offsets-all.txt"; ssh "${SSHO[@]}" ubuntu@$KAFKA "docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $t" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}' >> "results/$RUN/offsets-all.txt"; done
  tar czf "/tmp/$RUN.tgz" -C results "$RUN" && aws s3 cp "/tmp/$RUN.tgz" "s3://$BUCKET/results/$RUN.tgz" --only-show-errors
  st "done $s rc=$rc ($RUN)"
done
st "ALL DONE"
