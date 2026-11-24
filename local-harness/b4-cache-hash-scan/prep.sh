#!/bin/bash
# usage: prep.sh <ids>   recreates topics and produces 5 records per id into mqtt-sessions
S=$(dirname "$0"); N=$1
K="docker exec -i kafka /opt/kafka/bin"
for t in mqtt-sessions mqtt-messages mqtt-retained; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t >/dev/null 2>&1; done
sleep 3
for t in mqtt-sessions mqtt-messages mqtt-retained; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $t --partitions 1 --replication-factor 1 --config segment.bytes=1073741824 >/dev/null; done
python3 $S/gen.py $N | $K/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic mqtt-sessions --property parse.key=true --property key.separator=: --property null.marker=NULL --producer-property linger.ms=20 --producer-property batch.size=262144 --producer-property compression.type=lz4
$K/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-sessions
