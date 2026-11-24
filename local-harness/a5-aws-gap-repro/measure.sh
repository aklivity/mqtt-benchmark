#!/bin/bash
# usage: measure.sh <label> <pubs> <qos> <window_s> [idle_conns] [connect_rate]
# assumes zilla already running (pgrep ZillaMain) and kafka up.
S=${S:-$(cd "$(dirname "$0")" && pwd)}
LABEL=$1; PUBS=$2; QOS=$3; WIN=$4; IDLE=${5:-0}; RATE=${6:-300}
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done | head -1)
KB="docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages"
recs(){ $KB 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
cpu(){ awk '{print $14+$15}' /proc/$PID/task/$TID/stat; }
docker rm -f lg >/dev/null 2>&1
T0=$(date +%s.%N)
if [ "$IDLE" -gt 0 ]; then
 docker run -d --name lgi --network host --cpuset-cpus 2,3 emqx/emqtt-bench conn -h 127.0.0.1 -p 7183 -c $IDLE -R $RATE -k 60 -V 5 -C true -i 1 -t 'x' >/dev/null
 # wait for connections
 until [ "$(docker logs lgi 2>&1 | grep -c 'connect_succ')" -gt 0 ] && docker logs lgi 2>&1 | grep 'connect_succ' | tail -1 | grep -q "total=$IDLE"; do sleep 2; [ $(echo "$(date +%s.%N)-$T0>600"|bc) = 1 ] && break; done
 echo "idle conns connected after $(echo "$(date +%s.%N)-$T0"|bc) s"
fi
IT0=$(date +%s.%N)
docker run -d --name lg --network host --cpuset-cpus 2,3 emqx/emqtt-bench pub -h 127.0.0.2 -p 7183 -c $PUBS -I 1000 -s 1024 -t 'place/%i/data' -q $QOS -V 5 -C true -R $RATE >/dev/null
CT=NA
for i in $(seq 1 300); do
  sleep 1
  L=$(docker logs lg 2>&1 | grep -a 'connect_succ' | tail -1)
  if echo "$L" | grep -q "total=$PUBS"; then CT=$(echo "$(date +%s.%N)-$IT0"|bc); break; fi
done
echo "pub CONNACK complete: $CT s  last: $L"
sleep 10
C0=$(cpu); R0=$(recs); W0=$(date +%s.%N)
sleep $WIN
C1=$(cpu); R1=$(recs); W1=$(date +%s.%N)
RSS=$(awk '/VmRSS/{print $2}' /proc/$PID/status)
HEAP=$(jcmd $PID GC.heap_info 2>/dev/null | grep -v Picked | head -3 | tr '\n' ' ')
LAST=$(docker logs lg 2>&1 | grep -a 'connect_succ\|publish_ok\|pub_succ\|pub_fail\|connect_fail' | tail -4 | tr '\n' '|')
python3 - <<PY
c=($C1-$C0)*10000.0; r=$R1-$R0; w=$W1-$W0
print("RESULT label=$LABEL pubs=$PUBS idle=$IDLE qos=$QOS conack_s=$CT worker_cpu_pct=%.1f records=%d rate=%.0f us_per_msg=%.0f rss_mb=%.0f" % (c/(w*1e6)*100, r, r/w, c/max(r,1), $RSS/1024.0))
PY
echo "HEAP $HEAP"; echo "LG $LAST"
docker logs lg 2>&1 | tail -30 > $S/lg-$LABEL.log
docker rm -f lg >/dev/null 2>&1
