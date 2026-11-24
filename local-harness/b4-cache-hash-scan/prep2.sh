#!/bin/bash
# usage: prep2.sh <prefill-records>  recreates topics; prefills mqtt-messages with keyed records (unique keys)
S=$(dirname "$0"); N=$1
K="docker exec -i kafka /opt/kafka/bin"
for t in mqtt-sessions mqtt-messages mqtt-retained; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t >/dev/null 2>&1; done
sleep 3
$K/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic mqtt-messages --partitions 1 --replication-factor 1 --config segment.bytes=1073741824 >/dev/null
for t in mqtt-sessions mqtt-retained; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $t --partitions 1 --replication-factor 1 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=1.0 >/dev/null; done
if [ "$N" -gt 0 ]; then
python3 -c "
import sys
v='z'*96
for i in range($N): sys.stdout.write('prefill/%d:%s\n'%(i,v))" | $K/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic mqtt-messages --property parse.key=true --property key.separator=: --producer-property linger.ms=20 --producer-property batch.size=262144 --producer-property compression.type=lz4
fi
$K/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages
