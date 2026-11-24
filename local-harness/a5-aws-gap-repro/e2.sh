#!/bin/bash
# usage: e2.sh <tag> <startz-opts...>  ; stages of idle conns, each: idle cpu, then 3125 QoS1 pubs
S=${S:-$(cd "$(dirname "$0")" && pwd)}
TAG=$1; shift
docker rm -f lg $(docker ps -aq --filter name=lgi) >/dev/null 2>&1
$S/kafka.sh reset >/dev/null
$S/startz.sh $TAG "$@"
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done | head -1)
cpu(){ awk '{print $14+$15}' /proc/$PID/task/$TID/stat; }
TOT=0
for N in ${N_LIST:-0 5000 5000 5000}; do
  if [ $N -gt 0 ]; then
    TOT=$((TOT+N)); T0=$(date +%s.%N)
    docker run -d --name lgi$TOT --network host --cpuset-cpus 2,3 emqx/emqtt-bench conn -h 127.0.0.1 -p 7183 -c $N -R 300 -k 60 -V 5 -C true -i 1 --prefix idle$TOT >/dev/null
    until docker logs lgi$TOT 2>&1 | grep -a connect_succ | tail -1 | grep -q "total=$N"; do sleep 3; [ $(echo "$(date +%s.%N)-$T0>500"|bc) = 1 ] && break; done
    echo "idle total=$TOT: ramp took $(echo "$(date +%s.%N)-$T0"|bc) s ; $(docker logs lgi$TOT 2>&1 | grep -a connect_succ | tail -1)"
  fi
  sleep 15
  C0=$(cpu); W0=$(date +%s.%N); sleep 30; C1=$(cpu); W1=$(date +%s.%N)
  GC0=$(grep -c Pause $S/gc-$TAG.log)
  echo "IDLE total=$TOT worker_cpu_pct=$(python3 -c "print('%.1f'%(($C1-$C0)*10000.0/(($W1-$W0)*1e6)*100))") rss_mb=$(awk '/VmRSS/{print int($2/1024)}' /proc/$PID/status) $(jcmd $PID GC.heap_info 2>/dev/null | grep -i 'total.*used' | head -2 | tr -s ' ' | tr '\n' ';')"
  [ -n "$JFR" ] && [ $TOT -ge 15000 ] && jcmd $PID JFR.start name=p duration=50s filename=$S/$TAG.jfr jdk.CPUTimeSample#enabled=true jdk.CPUTimeSample#throttle=5ms >/dev/null 2>&1
  $S/measure.sh "$TAG-idle$TOT" 3125 1 30 2>&1 | grep -v "^HEAP\|^LG"
done
echo "GC pauses:"; grep "Pause" $S/gc-$TAG.log | awk '{print $NF}' | sort -n | awk '{a[NR]=$1} END{print "count",NR,"max",a[NR],"p50",a[int(NR/2)],"p99",a[int(NR*0.99)]}'
