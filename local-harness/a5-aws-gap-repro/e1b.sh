#!/bin/bash
S=${S:-$(cd "$(dirname "$0")" && pwd)}
for i in 1 2 3 4 5 6; do
  echo "=== run $i $(date)"
  $S/startz.sh e1b-r$i 0 1
  $S/measure.sh e1b-r$i 3125 1 30 2>&1 | grep -v HEAP
  for t in mqtt-sessions mqtt-messages; do
    echo -n "$t end offsets: "; docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $t | awk -F: '{s+=$3} END{print s}'
  done
  docker exec kafka du -sm /tmp/logs 2>/dev/null | tail -1
done
