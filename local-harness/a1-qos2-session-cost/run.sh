#!/bin/bash
# usage: run.sh <label> <storm|pub> <count> <qos> <secs_total> [zilla args...]
S="$(cd "$(dirname "$0")" && pwd)"
label=$1; mode=$2; count=$3; qos=$4; secs=$5; shift 5
out=$S/res/$label; mkdir -p $out
t0=$(date +%s)
$S/kafka.sh >/dev/null; $S/zilla.sh "$@" > $out/zilla.start
base=$($S/offsets.sh); echo "base $base" > $out/summary
if [ $mode = storm ]; then
  cmd="conn -c $count -R 100 -C true -V 5"
else
  cmd="pub -c $count -R 100 -I 1000 -s 1024 -q $qos -t device/%i -C true -V 5"
fi
docker run -d --name bench --network host emqx/emqtt-bench $cmd -h localhost -p 1883 >/dev/null
( for i in $(seq 1 $((secs/10))); do sleep 10; echo "$(date +%s) $(ps -eo pcpu,rss,args --sort=-pcpu | grep -a '[j]ava' | head -2 | cut -c1-40 | tr '\n' ' ')" >> $out/stats; done ) &
if [ $mode = pub ]; then
  sleep $((count/100 + 15)); a=$($S/offsets.sh); ta=$(date +%s); echo "winstart $a" >> $out/summary
  sleep 120; b=$($S/offsets.sh); tb=$(date +%s); echo "winend $b window=$((tb-ta))s" >> $out/summary
  docker exec zilla jcmd 1 GC.heap_info 2>&1 | head -4 >> $out/summary
  sleep $((secs - count/100 - 15 - 120 - 5)) 
else
  sleep $((count/100 + 20)); echo "hold-mid $($S/offsets.sh)" >> $out/summary; sleep $((secs - count/100 - 20))
fi
docker logs bench > $out/bench.log 2>&1
docker rm -f bench >/dev/null
sleep 8
echo "end $($S/offsets.sh)" >> $out/summary
echo "elapsed $(( $(date +%s)-t0 ))s" >> $out/summary
docker logs zilla 2>&1 | grep -ciE "exception|error" > $out/zilla.errcount
docker logs zilla > $out/zilla.log 2>&1
wait
