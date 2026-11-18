#!/usr/bin/env bash
# Runs one scenario against the environment provisioned by deploy/terraform and deploy/ansible.
#
# Usage: bin/run-scenario.sh scenarios/<name>.env
#   SSH_KEY=~/.ssh/mqtt-benchmark  INVENTORY=deploy/inventory.ini  (defaults)
#
# MODE=conn    emqtt-bench `conn` clients only: connect CONNECTIONS at CONN_RATE, hold them for DURATION_S.
# MODE=pubsub  1. SUBSCRIBERS emqtt-bench subscribers connect and subscribe (none if 0), and the runner waits for
#                 every SUBACK, as the Open MQTT Benchmark Suite requires connections before publishing.
#              2. PUBLISHERS emqtt-bench publishers connect; each publishes once every PUBLISH_INTERVAL_MS.
#              3. After WARMUP_S, it measures for DURATION_S: messages sent, records written to Kafka, messages
#                 received by subscribers, end-to-end latency, publish response time (bin/puback-probe.py on
#                 PROBE_CONNECTIONS extra connections), and broker/Kafka host CPU and memory.
#
# Clients are spread over the client hosts. PLACEMENT=split gives each host an equal share and its own range of
# client numbers (so subscriber N and publisher N, which use topic device/N, run on the same host);
# PLACEMENT=first puts every publisher on client host 0 (fan-out with one publisher).
# The broker (Zilla or EMQX) and its MQTT port are read from the broker host, where deploy/ansible records them.
# Writes results/<run-id>/: summary.json, metric snapshots, client logs, broker config and log, image versions.
set -euo pipefail
cd "$(dirname "$0")/.."
SCENARIO=${1:?usage: bin/run-scenario.sh scenarios/<name>.env}
# defaults, overridden by the scenario file
MODE=pubsub; SUBSCRIBERS=0; SUB_CONN_RATE=500; SUB_TOPIC=''; SUB_QOS=1; PLACEMENT=split; DELIVERIES_PER_MSG=1
PUBLISHERS=0; PUB_CONN_RATE=500; PROBE_CONNECTIONS=0; INFLIGHT=1; KAFKA_TOPIC=mqtt-devices
MQTT_VERSION=4; KEEPALIVE_S=300; CLEAN_SESSION=true; WARMUP_S=60; DURATION_S=1800
# shellcheck disable=SC1090
source "$SCENARIO"
INVENTORY=${INVENTORY:-deploy/inventory.ini}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/mqtt-benchmark}
BENCH_IMAGE=${BENCH_IMAGE:-emqx/emqtt-bench:0.6.3}
PUB_API=8080; SUB_API=8081
RUN_ID="$NAME-$(date -u +%Y%m%dT%H%M%SZ)"
OUT="results/$RUN_ID"; mkdir -p "$OUT/clients" "$OUT/metrics"

# --- inventory and helpers -------------------------------------------------------------------------------
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
eval "$(on "$BROKER_PUB" cat /opt/mqtt-benchmark/broker.env)"   # BROKER=zilla|emqx, MQTT_PORT

offsets() { on "$KAFKA_PUB" "docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $KAFKA_TOPIC" \
  | awk -F: '{s+=$3} END {print s+0}'; }
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
COMMON="-h $BROKER_IP -p $MQTT_PORT -V $MQTT_VERSION -k $KEEPALIVE_S -C $CLEAN_SESSION"
bench() {  # bench <host index> <name> <api port> <args...>
  local k=$1 name=$2 port=$3; shift 3
  on "${CLIENTS[$k]}" "docker run -d --name $name --network host --ulimit nofile=1048576:1048576 $BENCH_IMAGE $* \
    $COMMON --prometheus --restapi $port" >/dev/null; }

# --- metadata --------------------------------------------------------------------------------------------
log "run $RUN_ID ($MODE) on $N client hosts"
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

# --- start clients ---------------------------------------------------------------------------------------
CONNECTED=0; SUBSCRIBED=0
if [ "$MODE" = conn ]; then
  for k in "${!CLIENTS[@]}"; do
    c=$(share "$CONNECTIONS" "$k"); [ "$c" -gt 0 ] || continue
    bench "$k" bench-conn $PUB_API conn -c "$c" -R $(( (CONN_RATE + N - 1) / N )) -n "$(first "$CONNECTIONS" "$k")" --prefix "'$RUN_ID-c-'" -Q true
    log "client $k: $c connections"
  done
  T_START=$(date +%s)
  CONNECTED=$(wait_for pub connect_succ "$CONNECTIONS" $(( CONNECTIONS / CONN_RATE * 3 + 120 )))
  T_CONNECTED=$(date +%s)
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
  for k in "${!CLIENTS[@]}"; do
    if [ "$PLACEMENT" = first ]; then [ "$k" -eq 0 ] && c=$PUBLISHERS || c=0; start=1
    else c=$(share "$PUBLISHERS" "$k"); start=$(first "$PUBLISHERS" "$k"); fi
    [ "$c" -gt 0 ] || continue
    bench "$k" bench-pub $PUB_API pub -c "$c" -R $(( (PUB_CONN_RATE + N - 1) / N )) -n "$start" --prefix "'$RUN_ID-p-'" \
      -t "'$PUB_TOPIC'" -q "$QOS" -s "$BODY" -I "$PUBLISH_INTERVAL_MS" -F "$INFLIGHT" -w true $HDRS
    log "client $k: $c publishers from #$start"
  done
  CONNECTED=$(wait_for pub connect_succ "$PUBLISHERS" $(( PUBLISHERS / PUB_CONN_RATE * 3 + 120 )))
  log "publishers connected: $CONNECTED/$PUBLISHERS (connect_fail=$(total pub connect_fail))"
fi

log "warm-up ${WARMUP_S}s"; sleep "$WARMUP_S"

# --- measure ---------------------------------------------------------------------------------------------
if [ "$MODE" = pubsub ] && [ "$PROBE_CONNECTIONS" -gt 0 ]; then
  on "${CLIENTS[0]}" "nohup python3 /opt/mqtt-benchmark/puback-probe.py $BROKER_IP $MQTT_PORT $PROBE_CONNECTIONS $DURATION_S $RUN_ID $PAYLOAD_BYTES \
    < /dev/null > /tmp/probe-$RUN_ID.json 2>/tmp/probe-$RUN_ID.err &"
fi
T0=$(date +%s); K0=$(offsets); snapshot t0; emqx_action t0
log "measuring for ${DURATION_S}s"
sleep "$DURATION_S"
T1=$(date +%s); K1=$(offsets); snapshot t1; emqx_action t1
if [ "$MODE" = pubsub ] && [ "$PROBE_CONNECTIONS" -gt 0 ]; then
  sleep 15; on "${CLIENTS[0]}" "cat /tmp/probe-$RUN_ID.json" > "$OUT/puback-probe.json" 2>/dev/null || true
fi

W="$((T1 - T0))s"
cpu() { promq "avg_over_time((100 * (1 - avg(rate(node_cpu_seconds_total{role=\"$1\",mode=\"idle\"}[1m]))))[$W:15s])" "$T1"; }
mem() { promq "avg_over_time(((node_memory_MemTotal_bytes{role=\"$1\"} - node_memory_MemAvailable_bytes{role=\"$1\"}) / 2^30)[$W:15s])" "$T1"; }
BCPU=$(cpu broker); BMEM=$(mem broker); KCPU=$(cpu kafka)
# connections the broker holds, counted by the kernel (no broker metrics needed)
BCONN=$(on "$BROKER_PUB" "ss -Htn state established '( sport = :$MQTT_PORT )' | wc -l")

# --- stop and collect ------------------------------------------------------------------------------------
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
