K="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
for t in mqtt-messages mqtt-retained mqtt-devices mqtt-sessions; do $K --delete --topic $t >/dev/null 2>&1; done; sleep 3
$K --create --topic mqtt-messages --partitions 16 >/dev/null 2>&1; for t in mqtt-retained mqtt-devices mqtt-sessions; do $K --create --topic $t --config cleanup.policy=compact >/dev/null 2>&1; done
