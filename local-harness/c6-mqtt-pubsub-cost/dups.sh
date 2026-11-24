#!/bin/bash
# usage: dups.sh <label> <flag> <qos> [clients=3125] [msgs per client=40]
LABEL=$1; FLAG=$2; QOS=$3; N=${4:-3125}; L=${5:-40}
S="$(cd "$(dirname "$0")" && pwd)"; cd $S
./kafka.sh >/dev/null 2>&1; ./zilla.sh $FLAG -; sleep 20
docker rm -f bpub >/dev/null 2>&1
docker run -d --name bpub --network host --cpuset-cpus 2-3 emqx/emqtt-bench:latest pub -h 127.0.0.1 -p 7183 -c $N -i 2 -I 1000 -L $((N*L)) -t bench/%i -s 64 -q $QOS --payload-hdrs cnt64 --prometheus --restapi 18080 >/dev/null
sleep $((N*2/1000+L+25))
curl -s localhost:18080/metrics 2>/dev/null | grep -E "^(pub|pub_succ|pub_overrun|puback|pubrec|pubcomp|publish)" | head -12 > $LABEL.metrics
docker logs bpub 2>&1 | grep -E "pub total|pub_succ total|publish complete" | tail -4 > $LABEL.benchlog
sleep 5
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages 2>/dev/null | tail -1 > $LABEL.offsets
rm -rf $LABEL.seg; mkdir $LABEL.seg; docker cp kafka:/tmp/kraft-combined-logs/mqtt-messages-0/. $LABEL.seg/ >/dev/null 2>&1
echo "offered=$((N*L))"; cat $LABEL.offsets $LABEL.metrics $LABEL.benchlog; python3 -I segparse.py $LABEL.seg
docker rm -f bpub >/dev/null 2>&1; pkill -TERM -f ZillaMain; sleep 4
