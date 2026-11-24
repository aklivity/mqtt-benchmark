#!/bin/bash
docker rm -f kafka >/dev/null 2>&1
docker run -d --name kafka --network host \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 \
 -e KAFKA_HEAP_OPTS="-Xmx1g" apache/kafka:4.1.1 >/dev/null
K=/opt/kafka/bin
until docker exec kafka $K/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 1; done
docker exec kafka $K/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1 >/dev/null 2>&1
T="docker exec kafka $K/kafka-topics.sh --bootstrap-server localhost:9092 --create"
$T --topic mqtt-messages --partitions 16 >/dev/null
$T --topic mqtt-retained --partitions 1 --config cleanup.policy=compact >/dev/null
$T --topic mqtt-devices --partitions 16 --config cleanup.policy=compact >/dev/null
$T --topic mqtt-sessions --partitions 1 --config cleanup.policy=compact >/dev/null
echo kafka ready
