#!/bin/bash
# usage: replay.sh <label>  restart zilla w/o load; report time to port, replay cpu-seconds and time until worker idle
S=${S:-$(cd "$(dirname "$0")" && pwd)}
L=$1
for t in mqtt-sessions mqtt-messages; do echo -n "$t records: "; docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $t | awk -F: '{s+=$3} END{print s}'; done
pkill -f ZillaMain; sleep 2
nohup $S/zilla.sh 0 1 $S/zilla.yaml $S/zilla-$L.log -Xlog:gc*:file=$S/gc-$L.log >/dev/null 2>&1 &
T0=$(date +%s.%N)
until (exec 3<>/dev/tcp/127.0.0.1/7183) 2>/dev/null; do sleep 0.3; done
echo "port ready: $(echo "$(date +%s.%N)-$T0"|bc) s"
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done | head -1)
P=$(awk '{print $14+$15}' /proc/$PID/task/$TID/stat); IDLE=0; LASTBUSY=0
for i in $(seq 1 600); do sleep 1; C=$(awk '{print $14+$15}' /proc/$PID/task/$TID/stat); D=$((C-P)); P=$C
  if [ $D -gt 10 ]; then LASTBUSY=$i; fi; [ $((i-LASTBUSY)) -ge 15 ] && break; done
echo "worker busy (>10% cpu) until t=${LASTBUSY}s after port-ready; worker total cpu-s=$(awk '{print ($14+$15)/100}' /proc/$PID/task/$TID/stat) rss_mb=$(awk '/VmRSS/{print int($2/1024)}' /proc/$PID/status)"
