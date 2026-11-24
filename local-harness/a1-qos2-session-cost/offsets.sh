#!/bin/bash
# prints topic total
for t in mqtt-sessions mqtt-devices mqtt-messages mqtt-retained; do
 n=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $t 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
 echo -n "$t=$n "
done; echo
