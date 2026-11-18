#!/usr/bin/env bash
# Exact delivery check on the provisioned environment: every completed publish must have exactly one record in Kafka.
#
# QOS=1 (default): 10 mosquitto_pub clients x 2,000 numbered 1 KB messages, counting successful PUBACKs (reason 0,
#   or 16 "no matching subscribers", which EMQX returns to MQTT 5 publishers when nobody subscribes).
# QOS=2: bin/qos2-delivery.py from client host 0, MQTT 5.
#   - exact: 10 connections x 2,000 numbered 1 KB messages, one exchange at a time per connection, counting
#     PUBCOMPs with reason 0.
#   - reconnect: QOS2_RECONNECT_N flows (default 20) for each of three cases where the connection drops in the middle
#     of the exchange (after PUBREC, before PUBREC, after PUBREL) and the client resumes its session. Each message
#     must be in Kafka exactly once.
#
# Then it reads KAFKA_TOPIC and counts the records whose key is one of those topics (both Zilla and the EMQX rule use
# the MQTT topic as the Kafka key), plus duplicates. It reads the whole topic, so run it on a fresh deploy or after the
# smoke scenario, not after a full-size run (100K msg/s for 30 minutes leaves about 180 million records to scan).
#
# Usage: bin/verify-delivery.sh            QOS=2 bin/verify-delivery.sh
#        SSH_KEY / INVENTORY / KAFKA_TOPIC as in bin/run-scenario.sh
set -euo pipefail
cd "$(dirname "$0")/.."
INVENTORY=${INVENTORY:-deploy/inventory.ini}; SSH_KEY=${SSH_KEY:-$HOME/.ssh/mqtt-benchmark}
KAFKA_TOPIC=${KAFKA_TOPIC:-mqtt-devices}; QOS=${QOS:-1}; QOS2_RECONNECT_N=${QOS2_RECONNECT_N:-20}
case "$QOS" in 1|2) ;; *) echo "QOS must be 1 or 2"; exit 2 ;; esac
hosts() { awk -v g="[$1]" -v f="$2" '/^\[/{in_g=($0==g); next} in_g && NF {for (i=2;i<=NF;i++) {split($i,kv,"="); if (kv[1]==f) print kv[2]}}' "$INVENTORY"; }
CLIENT=$(hosts clients ansible_host | head -1); BROKER_IP=$(hosts broker private_ip | head -1); KAFKA_IP=$(hosts kafka private_ip | head -1)
on() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "ubuntu@$CLIENT" "$@"; }
R="verify-q$QOS-$(date +%s)"
# BROKER and MQTT_PORT, recorded on the broker host by deploy/ansible
eval "$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "ubuntu@$(hosts broker ansible_host | head -1)" cat /opt/mqtt-benchmark/broker.env)"

if [ "$QOS" = 1 ]; then
  echo "== publishing 10 x 2,000 QoS 1 messages to $BROKER as $R"
  acks=$(on "docker run --rm --network host -e R=$R -e H=$BROKER_IP -e P=$MQTT_PORT eclipse-mosquitto:2.0 sh -c '
    PL=\$(head -c 1017 /dev/zero | tr \"\\0\" x)
    for c in \$(seq 1 10); do
      seq -w 1 2000 | sed \"s/\$/-\$PL/\" | timeout 300 mosquitto_pub -d -h \$H -p \$P -V mqttv5 -i \$R-\$c -q 1 -t device/\$R/x/\$c -l > /tmp/o\$c 2>&1 &
    done; wait; cat /tmp/o* | grep -c -E \"received PUBACK .*RC:(0|16)\\)\" || true'")
else
  echo "== publishing 10 x 2,000 QoS 2 messages to $BROKER as $R, one exchange at a time per connection"
  acks=$(on "timeout 900 python3 /opt/mqtt-benchmark/qos2-delivery.py exact $BROKER_IP $MQTT_PORT $R 10 2000" | awk '$1=="completed" {print $2}')
  echo "== $((3 * QOS2_RECONNECT_N)) interrupted QoS 2 flows"
  flows=$(on "timeout 900 python3 /opt/mqtt-benchmark/qos2-delivery.py reconnect $BROKER_IP $MQTT_PORT $R $QOS2_RECONNECT_N")
fi
sleep 30
echo "== reading $KAFKA_TOPIC"
on "docker run --rm --network host confluentinc/cp-kafkacat:7.1.9 kafkacat -b $KAFKA_IP:9092 -C -t $KAFKA_TOPIC -e -q -f '%k|%s\n' \
  | awk -F'|' -v p=device/$R/ 'index(\$1,p)==1 {print \$1 \"|\" substr(\$2,1,4)}' > /tmp/$R.txt"
recs=$(on "grep -c '^device/$R/x/' /tmp/$R.txt || true"); uniq=$(on "grep '^device/$R/x/' /tmp/$R.txt | sort -u | wc -l")
name=$([ "$QOS" = 1 ] && echo PUBACKs || echo PUBCOMPs)
echo "successful $name = $acks, Kafka records = $recs, distinct = $uniq, duplicates = $((recs - uniq))"
ok=true
[ "$acks" -eq 20000 ] && [ "$recs" -eq 20000 ] && [ "$uniq" -eq 20000 ] || ok=false

if [ "$QOS" = 2 ]; then
  # records per reconnect topic (each flow publishes one message to its own topic)
  counts=$(on "grep '^device/$R/r/' /tmp/$R.txt | cut -d'|' -f1 | sort | uniq -c" | awk '{print $2, $1}')
  res=$(echo "$flows" | while read -r case topic result; do
    n=$(echo "$counts" | awk -v t="$topic" '$1==t {print $2}'); echo "$case $result ${n:-0}"; done)
  for c in after-pubrec before-pubrec after-pubrel; do
    echo "$res" | awk -v c=$c '$1==c {t++; if ($2 ~ /^ok/) ok++; if ($3==1) one++; else if ($3==0) zero++; else dup++}
      END {printf "%-14s flows completed %d/%d, in Kafka once %d, missing %d, duplicated %d\n", c, ok, t, one, zero, dup}'
  done
  echo "$res" | awk '$2 !~ /^ok/ {print "not completed: " $1 " " $2}' | sort | uniq -c | head -5
  echo "$res" | awk '$2 ~ /session_present=0/' | wc -l | xargs -I{} echo "reconnects without the session present: {}"
  good=$(echo "$res" | awk '$2 ~ /^ok/ && $3==1' | wc -l | tr -d ' ')
  [ "$good" -eq $((3 * QOS2_RECONNECT_N)) ] || ok=false
fi
on "rm -f /tmp/$R.txt"
$ok && echo "PASS" || { echo "FAIL"; exit 1; }
