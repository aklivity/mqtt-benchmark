#!/bin/bash
docker rm -f kafka >/dev/null 2>&1
docker run -d --name kafka --network host --cpuset-cpus 1 -e KAFKA_HEAP_OPTS="-Xmx2g -Xms2g" apache/kafka:4.1.1 >/dev/null
for i in $(seq 60); do docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 2; done
K="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists"
$K --topic mqtt-messages --partitions 1
$K --topic mqtt-retained --partitions 1 --config cleanup.policy=compact
$K --topic mqtt-sessions --partitions 1 --config cleanup.policy=compact
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
