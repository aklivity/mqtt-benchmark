#!/usr/bin/env bash
# Exact delivery check on the provisioned environment: every QoS 1 PUBACK must have exactly one record in Kafka.
# Runs from client host 0: 10 mosquitto_pub clients x 2,000 numbered 1 KB messages, counts successful PUBACKs
# (reason 0, or 16 "no matching subscribers", which EMQX returns to MQTT 5 publishers when nobody subscribes), then
# reads KAFKA_TOPIC and counts the records whose key is one of those topics (both Zilla and the EMQX rule use the
# MQTT topic as the Kafka key), plus duplicates. It reads the whole topic, so run it on a fresh deploy or after the
# smoke scenario, not after a full-size run (100K msg/s for 30 minutes leaves about 180 million records to scan).
#
# Usage: bin/verify-delivery.sh            SSH_KEY / INVENTORY / KAFKA_TOPIC as in bin/run-scenario.sh
set -euo pipefail
cd "$(dirname "$0")/.."
INVENTORY=${INVENTORY:-deploy/inventory.ini}; SSH_KEY=${SSH_KEY:-$HOME/.ssh/mqtt-benchmark}
KAFKA_TOPIC=${KAFKA_TOPIC:-mqtt-devices}
hosts() { awk -v g="[$1]" -v f="$2" '/^\[/{in_g=($0==g); next} in_g && NF {for (i=2;i<=NF;i++) {split($i,kv,"="); if (kv[1]==f) print kv[2]}}' "$INVENTORY"; }
CLIENT=$(hosts clients ansible_host | head -1); BROKER_IP=$(hosts broker private_ip | head -1); KAFKA_IP=$(hosts kafka private_ip | head -1)
on() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "ubuntu@$CLIENT" "$@"; }
R="verify-$(date +%s)"
# BROKER and MQTT_PORT, recorded on the broker host by deploy/ansible
eval "$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "ubuntu@$(hosts broker ansible_host | head -1)" cat /opt/mqtt-benchmark/broker.env)"

echo "== publishing 10 x 2,000 QoS 1 messages to $BROKER as $R"
acks=$(on "docker run --rm --network host -e R=$R -e H=$BROKER_IP -e P=$MQTT_PORT eclipse-mosquitto:2.0 sh -c '
  PL=\$(head -c 1017 /dev/zero | tr \"\\0\" x)
  for c in \$(seq 1 10); do
    seq -w 1 2000 | sed \"s/\$/-\$PL/\" | timeout 300 mosquitto_pub -d -h \$H -p \$P -V mqttv5 -i \$R-\$c -q 1 -t device/\$R/\$c -l > /tmp/o\$c 2>&1 &
  done; wait; cat /tmp/o* | grep -c -E \"received PUBACK .*RC:(0|16)\\)\" || true'")
sleep 30
echo "== reading $KAFKA_TOPIC"
on "docker run --rm --network host confluentinc/cp-kafkacat:7.1.9 kafkacat -b $KAFKA_IP:9092 -C -t $KAFKA_TOPIC -e -q -f '%k|%s\n' \
  | awk -F'|' -v p=device/$R/ 'index(\$1,p)==1 {print \$1 \"|\" substr(\$2,1,4)}' > /tmp/$R.txt"
recs=$(on "wc -l < /tmp/$R.txt"); uniq=$(on "sort -u /tmp/$R.txt | wc -l"); on "rm -f /tmp/$R.txt"
echo "successful PUBACKs = $acks, Kafka records = $recs, distinct = $uniq, duplicates = $((recs - uniq))"
[ "$acks" -eq 20000 ] && [ "$recs" -eq 20000 ] && [ "$uniq" -eq 20000 ] && echo "PASS" || { echo "FAIL"; exit 1; }
