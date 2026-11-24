#!/bin/bash
# usage: measure.sh <label> <qos> <publishers> [window=60] [jfr=0] ; restarts zilla (extra JVM opts via ZOPTS)
S=/tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad
L=$1; Q=$2; N=$3; W=${4:-60}; JFR=${5:-0}
pkill -f ZillaMain; sleep 2; bash $S/reset-topics.sh
JOPTS="$ZOPTS"
[ "$JFR" = 1 ] && JOPTS="$JOPTS -XX:StartFlightRecording=filename=$S/$L.jfr,settings=profile,jdk.CPUTimeSample#enabled=true,jdk.CPUTimeSample#throttle=5ms,delay=60s,duration=$((W+2))s"
(nohup $S/run-zilla.sh $JOPTS > $S/zilla-$L.log 2>&1 &)
sleep 12
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done)
offs(){ a=$(date +%s.%N); docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages 2>/dev/null | awk -F: '{s+=$3} END{printf "%d ",s}'; docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-sessions 2>/dev/null | awk -F: '{s+=$3} END{printf "%d ",s}'; b=$(date +%s.%N); echo "$a $b"; }
cpu(){ awk '{print $14+$15}' /proc/$PID/task/$TID/stat; }
tot(){ awk '{print $14+$15}' /proc/$PID/stat; }
docker run --rm -d --name bench --network host --cpuset-cpus 2,3 emqx/emqtt-bench pub -h 127.0.0.1 -p 7183 -V 5 -c $N -R 300 -I 1000 -q $Q -s 1024 -t 'place/%i/data' -C true --prefix "b$L" -L 100000000 >/dev/null
for i in $(seq 1 200); do docker logs bench 2>&1 | grep -q "connect_succ total=$N " && break; sleep 1; done; sleep 20
[ "$JFR" = 1 ] && sleep 5
(sleep 25; docker stats --no-stream --format "{{.Name}} cpu={{.CPUPerc}}" > $S/stats-$L.txt) &
read M0 S0 a0 b0 < <(offs); C0=$(cpu); T0=$(tot); c0=$(date +%s.%N); sleep $W; C1=$(cpu); T1=$(tot); c1=$(date +%s.%N); read M1 S1 a1 b1 < <(offs)
docker logs bench 2>&1 > $S/bench-$L.log; docker kill bench >/dev/null
HZ=$(getconf CLK_TCK); DT=$(echo "$c1 - $c0" | bc); OT=$(echo "($a1+$b1)/2 - ($a0+$b0)/2" | bc -l)
echo "$L qos=$Q pubs=$N cpu_window=${DT}s offsets_window=$(echo $OT | cut -c1-6)s msgs_kafka=$((M1-M0)) sessions_recs=$((S1-S0)) kafka_rate=$(echo "($M1-$M0)/$OT" | bc -l | cut -c1-8)/s worker_cpu=$(echo "($C1-$C0)/$HZ/$DT*100" | bc -l | cut -c1-6)% proc_cpu=$(echo "($T1-$T0)/$HZ/$DT*100" | bc -l | cut -c1-6)% us_per_msg=$(echo "($C1-$C0)/$HZ*1000000/(($M1-$M0)/$OT*$DT)" | bc -l | cut -c1-8)"
grep -c "connect_succ" $S/bench-$L.log >/dev/null; tail -4 $S/bench-$L.log | tr '\n' ' '; echo
grep -i "pub_fail\|connect_fail\|error" $S/bench-$L.log | tail -2
