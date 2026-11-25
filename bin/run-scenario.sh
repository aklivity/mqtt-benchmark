#!/usr/bin/env bash
# Runs one scenario against the environment provisioned by deploy/terraform and deploy/ansible.
#
# Usage: bin/run-scenario.sh scenarios/<name>.env
#   SSH_KEY=~/.ssh/mqtt-benchmark  INVENTORY=deploy/inventory.ini  (defaults)
#   RUN_ID=<id> names the run and its results directory (default <scenario NAME>-<UTC time>)
#   DIAG=1 also saves broker diagnostics under results/<run-id>/diagnostics/ (see "diagnostics" below)
#
# MODE=conn    emqtt-bench `conn` clients only: connect CONNECTIONS at CONN_RATE, hold them for DURATION_S.
# MODE=pubsub  1. SUBSCRIBERS emqtt-bench subscribers connect and subscribe (none if 0), and the runner waits for
#                 every SUBACK, as the Open MQTT Benchmark Suite requires connections before publishing.
#              2. PUBLISHERS emqtt-bench publishers connect; each publishes once every PUBLISH_INTERVAL_MS.
#              3. After WARMUP_S, it measures for DURATION_S: messages sent, records written to Kafka, messages
#                 received by subscribers, end-to-end latency, publish response time (bin/puback-probe.py on
#                 PROBE_CONNECTIONS extra connections, at the scenario's QOS), and broker/Kafka host CPU and memory.
#
# Clients are spread over the client hosts. PLACEMENT=split gives each host an equal share and its own range of
# client numbers (so subscriber N and publisher N, which use topic device/N, run on the same host);
# PLACEMENT=first puts every publisher on client host 0 (fan-out with one publisher).
# The broker (Zilla or EMQX) and its MQTT port are read from the broker host, where deploy/ansible records them.
# Writes results/<run-id>/: summary.json, metric snapshots, client logs, broker config and log, image versions.
#
# DIAG=1, best effort (a failed diagnostic is logged and skipped, never aborts the run), in diagnostics/:
#   broker-threads.txt        per-thread CPU of the broker process (top -H), sampled in the middle of the window
#   class-histogram-*.txt     top 40 lines of the Zilla JVM's class histogram just before the window opens and just
#                             after it closes (taking one forces a full GC, so never inside the window); needs jcmd
#                             in the Zilla image
#   ss-s.txt                  `ss -s` on the broker host at the end of the window
#   gc.log, zilla.jfr         the GC log and a Flight Recorder dump, when the broker was deployed with
#                             `-e zilla_diagnostics=true`
#
# DIAG=1 (or REC=1) also records the whole run, from the connection ramp to the end, for the broker and Kafka hosts:
#   events.txt                epoch ms of run_start, ramp_start, ramp_end, warmup_end, probe_start, t0, t1, probe_done
#   offsets-t0.txt, offsets-t1.txt   end offset of every partition of KAFKA_TOPIC at T0 and T1 (topic:partition:offset)
#   recorder-broker.tgz       bin/record-host.py output: per-thread cpu (tid, container tid, utime, stime, cpu id) every 5 s,
#                             meminfo Dirty/Writeback, /proc/<pid>/io, diskstats and vmstat every second, cpu topology
#   recorder-kafka.tgz        the same without threads, plus offsets.tsv from bin/record-offsets.sh: the end offset of every
#                             partition of every mqtt-* topic every OFFSET_INTERVAL_S (10) seconds
#   locations.txt, ebs-*.json where the Zilla cache and the Kafka log live, and the volume type, IOPS and throughput
#   puback-probe.json, puback-probe-progress.txt   the probe summary (waited for up to 60 s) and its counters every 5 s
# SESSION_EXPIRY_S=<seconds> (MQTT 5 with CLEAN_SESSION=false) passes emqtt-bench -x, so every client's session persists.
# WAIT_BEFORE_PUBLISHING=true (default) makes each emqtt-bench instance publish only once all its connections are up;
# false lets each publisher start as soon as it is connected.
set -euo pipefail
cd "$(dirname "$0")/.."
SCENARIO=${1:?usage: bin/run-scenario.sh scenarios/<name>.env}
MODE=pubsub; SUBSCRIBERS=0; SUB_CONN_RATE=500; SUB_TOPIC=''; SUB_QOS=1; PLACEMENT=split; DELIVERIES_PER_MSG=1
PUBLISHERS=0; PUB_CONN_RATE=500; PROBE_CONNECTIONS=0; INFLIGHT=1; KAFKA_TOPIC=mqtt-devices
MQTT_VERSION=4; KEEPALIVE_S=300; CLEAN_SESSION=true; WARMUP_S=60; DURATION_S=1800; WAIT_BEFORE_PUBLISHING=true
SESSION_EXPIRY_S=''
# shellcheck disable=SC1090
source "$SCENARIO"
INVENTORY=${INVENTORY:-deploy/inventory.ini}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/mqtt-benchmark}
BENCH_IMAGE=${BENCH_IMAGE:-emqx/emqtt-bench:0.6.3}
PUB_API=8080; SUB_API=8081
RUN_ID=${RUN_ID:-"$NAME-$(date -u +%Y%m%dT%H%M%SZ)"}
DIAG=${DIAG:-0}
REC=${REC:-$DIAG}
OFFSET_INTERVAL_S=${OFFSET_INTERVAL_S:-10}
OUT="results/$RUN_ID"; mkdir -p "$OUT/clients" "$OUT/metrics"

hosts() {   # hosts <group> <field: ansible_host|private_ip>
  awk -v g="[$1]" -v f="$2" '/^\[/{in_g=($0==g); next} in_g && NF {for (i=2;i<=NF;i++) {split($i,kv,"="); if (kv[1]==f) print kv[2]}}' "$INVENTORY"
}
BROKER_PUB=$(hosts broker ansible_host | head -1); BROKER_IP=$(hosts broker private_ip | head -1)
KAFKA_PUB=$(hosts kafka ansible_host | head -1)
MONITOR_PUB=$(hosts monitor ansible_host | head -1)
CLIENTS=(); while read -r h; do CLIENTS+=("$h"); done < <(hosts clients ansible_host)   # bash 3 (macOS) has no mapfile
N=${#CLIENTS[@]}; [ "$N" -gt 0 ] || { echo "no client hosts in $INVENTORY"; exit 1; }
SSH=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR)
on() { local h=$1; shift; "${SSH[@]}" "ubuntu@$h" "$@"; }
log() { echo "[$(date -u +%T)] $*" | tee -a "$OUT/run.log"; }
ZILLA_DIAGNOSTICS=false   # not in broker.env of hosts deployed before the option existed
eval "$(on "$BROKER_PUB" cat /opt/mqtt-benchmark/broker.env)"   # BROKER=zilla|emqx, MQTT_PORT, ZILLA_DIAGNOSTICS

# offsets [file]: the sum of the end offsets of KAFKA_TOPIC; with a file, also saves the topic:partition:offset lines
offsets() {
  local raw
  raw=$(on "$KAFKA_PUB" "docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $KAFKA_TOPIC")
  if [ -n "${1:-}" ]; then echo "$raw" > "$1"; fi
  echo "$raw" | awk -F: '{s+=$3} END {print s+0}'; }
ev() { echo "$(date +%s%3N) $1" >> "$OUT/events.txt"; }
# share <total> <k>: clients for host k when <total> is split evenly; first <total> <k>: sequence start for host k
share() { echo $(( $1 / N + ($2 < $1 % N ? 1 : 0) )); }
first() { local i s=1; for ((i = 0; i < $2; i++)); do s=$((s + $(share "$1" "$i"))); done; echo "$s"; }
# snapshot <label>: save every emqtt-bench /metrics page (role-host-label.txt) for the summary
snapshot() {
  local k
  for k in "${!CLIENTS[@]}"; do
    on "${CLIENTS[$k]}" "curl -s localhost:$PUB_API/metrics" > "$OUT/metrics/pub-$k-$1.txt" 2>/dev/null || true
    on "${CLIENTS[$k]}" "curl -s localhost:$SUB_API/metrics" > "$OUT/metrics/sub-$k-$1.txt" 2>/dev/null || true
  done
}
total() {   # total <role pub|sub> <counter>: sum of a counter over all client hosts, live
  local k s=0 v
  for k in "${!CLIENTS[@]}"; do
    v=$(on "${CLIENTS[$k]}" "curl -s localhost:$([ "$1" = pub ] && echo $PUB_API || echo $SUB_API)/metrics" 2>/dev/null | awk -v m="$2" '$1==m {printf "%d", $2}')
    s=$((s + ${v:-0}))
  done; echo "$s"; }
# emqx_action <label>: EMQX's own counters for the Kafka action (matched, success, failed, dropped, ...)
emqx_action() {
  [ "$BROKER" = emqx ] || return 0
  on "$BROKER_PUB" 'curl -s -u "$(cut -d: -f1,2 /etc/emqx/api-keys.txt)" localhost:18083/api/v5/actions/kafka_producer:devices/metrics' \
    > "$OUT/metrics/emqx-action-$1.json" 2>/dev/null || true; }
wait_for() {  # wait_for <role> <counter> <target> <timeout_s>
  local deadline=$(( $(date +%s) + $4 )) n
  while :; do
    n=$(total "$1" "$2"); [ "$n" -ge "$3" ] && { echo "$n"; return; }
    [ "$(date +%s)" -ge "$deadline" ] && { echo "$n"; return; }
    sleep 2
  done; }
promq() {  # promq <query> <unix time>: first value of an instant query on the monitor host
  { on "$MONITOR_PUB" "curl -s --get localhost:9090/api/v1/query --data-urlencode 'query=$1' --data-urlencode time=$2" || true; } \
    | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "null")
except Exception:
    print("null")   # monitoring host unreachable: report no value instead of aborting the run
'; }
COMMON="-h $BROKER_IP -p $MQTT_PORT -V $MQTT_VERSION -k $KEEPALIVE_S -C $CLEAN_SESSION${SESSION_EXPIRY_S:+ -x $SESSION_EXPIRY_S}"
bench() {  # bench <host index> <name> <api port> <args...>
  local k=$1 name=$2 port=$3; shift 3
  on "${CLIENTS[$k]}" "docker run -d --name $name --network host --ulimit nofile=1048576:1048576 $BENCH_IMAGE $* \
    $COMMON --prometheus --restapi $port" >/dev/null; }

DIAG_DIR="$OUT/diagnostics"
diag() { [ "$DIAG" = 1 ] || return 0; "$@" || log "diagnostic failed: $*"; }
# JAVA_TOOL_OPTIONS= keeps jcmd's own JVM from starting a second GC log and recording on the same files
has_jcmd() { [ "$BROKER" = zilla ] && on "$BROKER_PUB" "docker exec zilla sh -c 'command -v jcmd'" >/dev/null 2>&1; }
jcmd_zilla() { on "$BROKER_PUB" "docker exec -e JAVA_TOOL_OPTIONS= zilla jcmd 1 $*"; }
diag_threads() {
  on "$BROKER_PUB" 'pid=$(pgrep -x java | head -1); [ -n "$pid" ] || pid=$(docker inspect --format "{{.State.Pid}}" '"$BROKER"')
    top -H -b -n 3 -d 5 -p "$pid"' > "$DIAG_DIR/broker-threads.txt"; }
diag_histogram() {  # diag_histogram <label>
  if has_jcmd; then jcmd_zilla 'GC.class_histogram | head -n 40' > "$DIAG_DIR/class-histogram-$1.txt"
  else log "diagnostics: no jcmd in the $BROKER container, no class histogram"; fi; }
diag_ss() { on "$BROKER_PUB" "ss -s" > "$DIAG_DIR/ss-s.txt"; }
diag_zilla_logs() {
  if [ "$BROKER" = zilla ] && [ "$ZILLA_DIAGNOSTICS" = true ]; then
    on "$BROKER_PUB" "sudo cat /var/log/zilla/gc.log" > "$DIAG_DIR/gc.log"
    if has_jcmd; then
      jcmd_zilla 'JFR.dump filename=/var/log/zilla/zilla-window.jfr' > "$DIAG_DIR/jfr-dump.txt"
      on "$BROKER_PUB" "sudo cat /var/log/zilla/zilla-window.jfr" > "$DIAG_DIR/zilla.jfr"
    fi
  fi; }
if [ "$DIAG" = 1 ]; then mkdir -p "$DIAG_DIR"; fi

# The recorder (bin/record-host.py, bin/record-offsets.sh) is copied to the hosts and run in the background for the whole
# run; every step is best effort, a failure is logged and never aborts the run.
REC_DIR=/tmp/rec-$RUN_ID
rec() { [ "$REC" = 1 ] || return 0; "$@" || log "recorder: $* failed"; }
rec_start() {
  local h
  for h in "$BROKER_PUB" "$KAFKA_PUB"; do
    on "$h" "mkdir -p $REC_DIR && cat > $REC_DIR/record-host.py" < bin/record-host.py
  done
  on "$KAFKA_PUB" "cat > $REC_DIR/record-offsets.sh" < bin/record-offsets.sh
  on "$BROKER_PUB" "sudo nohup python3 $REC_DIR/record-host.py $REC_DIR/broker --interval 1 --threads > /dev/null 2>&1 < /dev/null &" < /dev/null
  on "$KAFKA_PUB" "sudo nohup python3 $REC_DIR/record-host.py $REC_DIR/kafka --interval 5 > /dev/null 2>&1 < /dev/null &" < /dev/null
  on "$KAFKA_PUB" "KAFKA_TOPIC=$KAFKA_TOPIC nohup bash $REC_DIR/record-offsets.sh $REC_DIR/offsets $OFFSET_INTERVAL_S > /dev/null 2>&1 < /dev/null &" < /dev/null
  local k   # every emqtt-bench metrics page of every client host, every 5 s, with the epoch ms of each read
  for k in "${!CLIENTS[@]}"; do
    on "${CLIENTS[$k]}" "mkdir -p $REC_DIR/metrics && nohup bash -c 'while [ ! -e $REC_DIR/metrics/stop ]; do for r in $PUB_API $SUB_API; do echo \"### \$(date +%s%3N) port \$r\"; curl -s localhost:\$r/metrics; done; sleep 5; done' > $REC_DIR/metrics/series.txt 2>&1 < /dev/null &" < /dev/null
  done
}
rec_meta() {   # what ran: image digest and revision label, the JVM options, host uptime and the run's index since the broker host started
  local env labels up idx
  env=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{json .Config.Env}}'") || env='"unknown"'
  labels=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{json .Config.Labels}}'") || labels='"unknown"'
  up=$(on "$BROKER_PUB" "cut -d' ' -f1 /proc/uptime; uptime -s" | tr '\n' ' ')
  idx=$(on "$BROKER_PUB" 'n=$(cat /var/tmp/mqtt-benchmark-run-count 2>/dev/null || echo 0); n=$((n + 1)); echo $n | sudo tee /var/tmp/mqtt-benchmark-run-count')
  python3 - "$OUT/run-meta.json" "$RUN_ID" "$env" "$labels" "$up" "$idx" <<'PY'
import json, sys
out, run, env, labels, up, idx = sys.argv[1:7]
j = lambda v: json.loads(v) if v.strip().startswith(('[', '{', '"')) else v
json.dump({'run_id': run, 'broker_env': j(env), 'broker_labels': j(labels), 'broker_uptime_s_and_boot_time': up.strip(),
           'run_index_since_broker_host_start': int(idx)}, open(out, 'w'), indent=1)
PY
}
rec_locations() {   # where the Zilla cache and the Kafka log live, and what the volumes are
  {
    echo "## $BROKER mounts"; on "$BROKER_PUB" "docker inspect $BROKER --format '{{json .Mounts}}'; docker exec $BROKER df -h /var/run/zilla 2>&1; df -h / /var/lib/docker"
    echo "## kafka mounts"; on "$KAFKA_PUB" "docker inspect kafka --format '{{json .Mounts}}'; df -h /data"
  } > "$OUT/locations.txt"
  local role h iid
  for role in broker kafka; do
    h=$([ "$role" = broker ] && echo "$BROKER_PUB" || echo "$KAFKA_PUB")
    iid=$(on "$h" 'T=$(curl -s -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 60" http://169.254.169.254/latest/api/token); curl -s -H "X-aws-ec2-metadata-token: $T" http://169.254.169.254/latest/meta-data/instance-id')
    echo "$iid" > "$OUT/instance-$role.txt"
    aws ec2 describe-volumes --filters "Name=attachment.instance-id,Values=$iid" \
      --query 'Volumes[].{id:VolumeId,type:VolumeType,iops:Iops,mbps:Throughput,gib:Size,device:Attachments[0].Device}' --output json \
      > "$OUT/ebs-$role.json" 2>/dev/null || echo "aws ec2 describe-volumes not available here, see lsblk in the recorder system.txt" > "$OUT/ebs-$role.json"
  done
}
rec_cloudwatch() {   # EBS volume and instance EBS metrics for the run, from the controller (needs cloudwatch:GetMetricStatistics)
  local role vols start_ms end_ms
  start_ms=$(head -1 "$OUT/events.txt" | cut -d' ' -f1); end_ms=$(date +%s%3N)
  for role in broker kafka; do
    vols=$(python3 -c 'import json,sys; print(" ".join(v["id"] for v in json.load(open(sys.argv[1]))))' "$OUT/ebs-$role.json" 2>/dev/null || true)
    python3 bin/ebs-cloudwatch.py "$OUT/ebs-cloudwatch-$role.json" "$start_ms" "$end_ms" "$(cat "$OUT/instance-$role.txt")" $vols
  done
}
rec_stop() {
  local h name i k
  on "$BROKER_PUB" "sudo touch $REC_DIR/broker/stop" < /dev/null || true
  on "$KAFKA_PUB" "sudo touch $REC_DIR/kafka/stop; touch $REC_DIR/offsets/stop" < /dev/null || true
  for k in "${!CLIENTS[@]}"; do
    on "${CLIENTS[$k]}" "touch $REC_DIR/metrics/stop; sleep 6; tar czf - -C $REC_DIR metrics" < /dev/null > "$OUT/recorder-client-$k.tgz" || true
  done
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do   # up to 30 s for the last Kafka offset read to finish
    on "$BROKER_PUB" "test -e $REC_DIR/broker/done" < /dev/null \
      && on "$KAFKA_PUB" "test -e $REC_DIR/kafka/done && test -e $REC_DIR/offsets/done" < /dev/null && break
    sleep 2
  done
  on "$BROKER_PUB" "tar czf - -C $REC_DIR broker" < /dev/null > "$OUT/recorder-broker.tgz"
  on "$KAFKA_PUB" "tar czf - -C $REC_DIR kafka offsets" < /dev/null > "$OUT/recorder-kafka.tgz"
}

log "run $RUN_ID ($MODE) on $N client hosts"
ev run_start
cp "$SCENARIO" "$OUT/scenario.env"
{
  echo "broker=$BROKER"
  echo "broker_image=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{.Config.Image}} {{.Image}}'")"
  echo "broker_args=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{.Args}}'")"
  echo "kafka_image=$(on "$KAFKA_PUB" "docker inspect kafka --format '{{.Config.Image}} {{.Image}}'")"
  echo "bench_image=$BENCH_IMAGE"
  echo "broker_host=$(on "$BROKER_PUB" 'echo $(nproc) vCPU, $(free -g | awk "/Mem:/ {print \$2}") GiB, $(uname -r)')"
} > "$OUT/versions.txt"
if [ "$BROKER" = zilla ]; then on "$BROKER_PUB" "cat /etc/zilla/zilla.yaml" > "$OUT/zilla.yaml"
else on "$BROKER_PUB" "cat /etc/emqx/base.hocon" > "$OUT/emqx.hocon"; fi
log "broker: $BROKER on $BROKER_IP:$MQTT_PORT"
for h in "${CLIENTS[@]}"; do on "$h" "docker rm -f bench-pub bench-sub bench-conn >/dev/null 2>&1 || true"; done

CONNECTED=0; SUBSCRIBED=0
rec rec_start; rec rec_locations; rec rec_meta
if [ "$MODE" = conn ]; then
  ev ramp_start
  for k in "${!CLIENTS[@]}"; do
    c=$(share "$CONNECTIONS" "$k"); [ "$c" -gt 0 ] || continue
    bench "$k" bench-conn $PUB_API conn -c "$c" -R $(( (CONN_RATE + N - 1) / N )) -n "$(first "$CONNECTIONS" "$k")" --prefix "'$RUN_ID-c-'" -Q true
    log "client $k: $c connections"
  done
  T_START=$(date +%s)
  CONNECTED=$(wait_for pub connect_succ "$CONNECTIONS" $(( CONNECTIONS / CONN_RATE * 3 + 120 )))
  T_CONNECTED=$(date +%s); ev ramp_end
  log "connected $CONNECTED/$CONNECTIONS in $((T_CONNECTED - T_START)) s"
else
  if [ "$SUBSCRIBERS" -gt 0 ]; then
    for k in "${!CLIENTS[@]}"; do
      c=$(share "$SUBSCRIBERS" "$k"); [ "$c" -gt 0 ] || continue
      bench "$k" bench-sub $SUB_API sub -c "$c" -R $(( (SUB_CONN_RATE + N - 1) / N )) -n "$(first "$SUBSCRIBERS" "$k")" \
        --prefix "'$RUN_ID-s-'" -t "'$SUB_TOPIC'" -q "$SUB_QOS" --payload-hdrs ts
      log "client $k: $c subscribers"
    done
    SUBSCRIBED=$(wait_for sub sub "$SUBSCRIBERS" $(( SUBSCRIBERS / SUB_CONN_RATE * 3 + 120 )))
    log "subscribed: $SUBSCRIBED/$SUBSCRIBERS (sub_fail=$(total sub sub_fail), connect_fail=$(total sub connect_fail))"
  fi
  # With subscribers, publishers add emqtt-bench's 8-byte send timestamp for end-to-end latency; shrink the body
  # so the payload on the wire is still PAYLOAD_BYTES. Without subscribers there is no timestamp.
  if [ "$SUBSCRIBERS" -gt 0 ]; then HDRS="--payload-hdrs ts"; BODY=$((PAYLOAD_BYTES - 8)); else HDRS=""; BODY=$PAYLOAD_BYTES; fi
  ev ramp_start   # publishers (and, with WAIT_BEFORE_PUBLISHING=true, their start) follow the connection ramp
  for k in "${!CLIENTS[@]}"; do
    if [ "$PLACEMENT" = first ]; then [ "$k" -eq 0 ] && c=$PUBLISHERS || c=0; start=1
    else c=$(share "$PUBLISHERS" "$k"); start=$(first "$PUBLISHERS" "$k"); fi
    [ "$c" -gt 0 ] || continue
    bench "$k" bench-pub $PUB_API pub -c "$c" -R $(( (PUB_CONN_RATE + N - 1) / N )) -n "$start" --prefix "'$RUN_ID-p-'" \
      -t "'$PUB_TOPIC'" -q "$QOS" -s "$BODY" -I "$PUBLISH_INTERVAL_MS" -F "$INFLIGHT" -w "$WAIT_BEFORE_PUBLISHING" $HDRS
    log "client $k: $c publishers from #$start"
  done
  CONNECTED=$(wait_for pub connect_succ "$PUBLISHERS" $(( PUBLISHERS / PUB_CONN_RATE * 3 + 120 )))
  ev ramp_end
  log "publishers connected: $CONNECTED/$PUBLISHERS (connect_fail=$(total pub connect_fail))"
fi

log "warm-up ${WARMUP_S}s"; sleep "$WARMUP_S"; ev warmup_end
diag diag_histogram start

if [ "$MODE" = pubsub ] && [ "$PROBE_CONNECTIONS" -gt 0 ]; then
  on "${CLIENTS[0]}" "PROBE_PROGRESS=/tmp/probe-$RUN_ID.progress nohup python3 /opt/mqtt-benchmark/puback-probe.py $BROKER_IP $MQTT_PORT $PROBE_CONNECTIONS $DURATION_S $RUN_ID $PAYLOAD_BYTES $QOS \
    < /dev/null > /tmp/probe-$RUN_ID.json 2>/tmp/probe-$RUN_ID.err &"
  ev probe_start
fi
ev t0; T0=$(date +%s); K0=$(offsets "$OUT/offsets-t0.txt"); snapshot t0; emqx_action t0
log "measuring for ${DURATION_S}s"
if [ "$DIAG" = 1 ]; then
  sleep $((DURATION_S / 2)); diag diag_threads
  rest=$((T0 + DURATION_S - $(date +%s))); if [ "$rest" -gt 0 ]; then sleep "$rest"; fi
else
  sleep "$DURATION_S"
fi
ev t1; T1=$(date +%s); K1=$(offsets "$OUT/offsets-t1.txt"); snapshot t1; emqx_action t1
if [ "$MODE" = pubsub ] && [ "$PROBE_CONNECTIONS" -gt 0 ]; then
  # The probe writes its summary only when its last publish has been acked or has timed out (10 s), so under load it
  # can still be running a while after T1: wait for a complete JSON, up to 60 s, before the clients are torn down.
  for _ in $(seq 1 30); do
    on "${CLIENTS[0]}" "python3 -c 'import json,sys; json.load(open(sys.argv[1]))' /tmp/probe-$RUN_ID.json" 2>/dev/null && break
    sleep 2
  done
  on "${CLIENTS[0]}" "cat /tmp/probe-$RUN_ID.json" > "$OUT/puback-probe.json" 2>/dev/null || true
  on "${CLIENTS[0]}" "cat /tmp/probe-$RUN_ID.progress /tmp/probe-$RUN_ID.err" > "$OUT/puback-probe-progress.txt" 2>/dev/null || true
  ev probe_done
fi
rec rec_stop; rec rec_cloudwatch

W="$((T1 - T0))s"
cpu() { promq "avg_over_time((100 * (1 - avg(rate(node_cpu_seconds_total{role=\"$1\",mode=\"idle\"}[1m]))))[$W:15s])" "$T1"; }
mem() { promq "avg_over_time(((node_memory_MemTotal_bytes{role=\"$1\"} - node_memory_MemAvailable_bytes{role=\"$1\"}) / 2^30)[$W:15s])" "$T1"; }
BCPU=$(cpu broker); BMEM=$(mem broker); KCPU=$(cpu kafka)
# connections the broker holds, counted by the kernel (no broker metrics needed)
BCONN=$(on "$BROKER_PUB" "ss -Htn state established '( sport = :$MQTT_PORT )' | wc -l")
diag diag_ss; diag diag_histogram end; diag diag_zilla_logs

for k in "${!CLIENTS[@]}"; do
  for c in bench-pub bench-sub bench-conn; do
    on "${CLIENTS[$k]}" "docker logs $c" > "$OUT/clients/$c-$k.log" 2>&1 || rm -f "$OUT/clients/$c-$k.log"
  done
  on "${CLIENTS[$k]}" "docker rm -f bench-pub bench-sub bench-conn >/dev/null 2>&1 || true"
done
on "$BROKER_PUB" "docker logs --timestamps $BROKER" > "$OUT/$BROKER.log" 2>&1 || true
BRESTARTS=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{.RestartCount}}'")
BSTATE=$(on "$BROKER_PUB" "docker inspect $BROKER --format '{{.State.Status}}'")   # "exited" = the broker stopped
# Zilla: engine exceptions; EMQX: log lines at error level or above
if [ "$BROKER" = zilla ]; then BERR=$(grep -c 'AgentTerminationException' "$OUT/$BROKER.log" || true)
else BERR=$(grep -c -E '\[(error|critical|alert|emergency)\]' "$OUT/$BROKER.log" || true); fi

CONNECT_S=null; [ -n "${T_CONNECTED:-}" ] && CONNECT_S=$((T_CONNECTED - T_START))
cat > "$OUT/run-values.json" <<JSON
{"run_id": "$RUN_ID", "mode": "$MODE", "window_s": $((T1 - T0)), "k0": $K0, "k1": $K1,
 "broker": "$BROKER", "connected": $CONNECTED, "subscribed": $SUBSCRIBED, "broker_cpu": "$BCPU", "broker_mem": "$BMEM",
 "kafka_cpu": "$KCPU", "broker_tcp_connections": "$BCONN", "broker_state": "$BSTATE", "broker_restarts": $BRESTARTS,
 "broker_errors": $BERR,
 "connect_s": $CONNECT_S}
JSON
python3 bin/summarize.py "$OUT" "$SCENARIO" | tee "$OUT/summary.json"
log "results in $OUT"
