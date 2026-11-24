#!/bin/bash
# usage: measure.sh <label> <mode pub|pubsub> <flag> [qos=1] [clients=3125] [window=60]
LABEL=$1; MODE=$2; FLAG=$3; QOS=${4:-1}; N=${5:-3125}; WIN=${6:-60}; NSUB=${7:-$N}
S="$(cd "$(dirname "$0")" && pwd)"
cd $S
./kafka.sh >/dev/null 2>&1
./zilla.sh $FLAG $S/$LABEL.jfr; sleep 20
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done | head -1)
B="docker run -d --network host --cpuset-cpus 2-3"
IMG=emqx/emqtt-bench:latest
[ "$MODE" != pub ] && docker logs bsub 2>&1 | grep -E "sub total|recv total" > $LABEL.sublog; docker rm -f bpub bsub >/dev/null 2>&1
if [ "$MODE" != pub ]; then
  $B --name bsub $IMG sub -h 127.0.0.1 -p 7183 -c $NSUB -i 2 -t bench/%i -q $QOS >/dev/null
  sleep $((NSUB*2/1000+15)); docker logs bsub 2>&1 | grep -E "sub total" | tail -1 > $LABEL.subtail
fi
[ "$MODE" != sub ] && $B --name bpub $IMG pub -h 127.0.0.1 -p 7183 -c $N -i 2 -I 1000 -t bench/%i -s 128 -q $QOS >/dev/null
sleep $((N*2/1000+25))
off() { docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages 2>/dev/null | tail -1 | cut -d: -f3; }
stat() { awk '{print $14, $15}' /proc/$PID/task/$TID/stat; }
pstat() { awk '{print $14+$15}' /proc/$PID/stat; }
HZ=$(getconf CLK_TCK)
o0=$(off); s0=($(stat)); p0=$(pstat); t0=$(date +%s.%N)
sleep $WIN
o1=$(off); s1=($(stat)); p1=$(pstat); t1=$(date +%s.%N)
docker logs --tail 12 bpub 2>&1 | grep -E "pub total|pub_overrun|pub_fail" | tail -3 > $LABEL.benchtail
[ "$MODE" = pubsub ] && docker logs --tail 12 bsub 2>&1 | grep -E "recv total" | tail -2 >> $LABEL.benchtail
[ "$MODE" != pub ] && docker logs bsub 2>&1 | grep -E "sub total|recv total" > $LABEL.sublog; docker rm -f bpub bsub >/dev/null 2>&1
python3 - <<P > $LABEL.result
t=$t1-$t0; n=$o1-$o0
u=($((s1[0]-s0[0])))/$HZ; s=($((s1[1]-s0[1])))/$HZ; p=($((p1-p0)))/$HZ
print("label=$LABEL mode=$MODE flag=$FLAG qos=$QOS clients=$N window=%.1fs"%t)
print("kafka_records=%d rate=%.0f/s"%(n,n/t))
print("worker_cpu: user=%.2fs sys=%.2fs total=%.2fs (%.1f%% of cpu)"%(u,s,u+s,100*(u+s)/t))
print("process_cpu=%.2fs (%.1f%%)"%(p,100*p/t))
print("worker_us_per_record=%.1f"%(1e6*(u+s)/max(n,1)))
P
cat $LABEL.result $LABEL.benchtail
pkill -TERM -f ZillaMain; sleep 5
