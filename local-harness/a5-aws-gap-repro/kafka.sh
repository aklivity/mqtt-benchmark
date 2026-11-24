#!/bin/bash
# usage: kafka.sh start|reset|stop
case $1 in
start)
docker rm -f kafka >/dev/null 2>&1
docker run -d --name kafka --network host --cpuset-cpus 1 \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_LISTENERS=CLIENT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
 -e KAFKA_ADVERTISED_LISTENERS=CLIENT://localhost:9092 \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=CLIENT \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,CONTROLLER:PLAINTEXT \
 -e KAFKA_LOG_DIRS=/tmp/logs -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
 -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 \
 apache/kafka:4.1.1 >/dev/null
for i in $(seq 60); do docker exec kafka /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 1; done ;;
reset)
K="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
for t in mqtt-messages mqtt-retained mqtt-devices mqtt-sessions; do $K --delete --topic $t >/dev/null 2>&1; done
sleep 3
$K --create --topic mqtt-messages --partitions 16 >/dev/null
$K --create --topic mqtt-devices --partitions 16 --config cleanup.policy=compact >/dev/null
$K --create --topic mqtt-retained --config cleanup.policy=compact >/dev/null
$K --create --topic mqtt-sessions --partitions 1 --config cleanup.policy=compact >/dev/null
$K --list ;;
stop) docker rm -f kafka ;;
esac
