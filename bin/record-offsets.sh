#!/usr/bin/env bash
# Records the end offsets of every partition of the mqtt-* topics over time, on the Kafka host (docker container "kafka").
# Usage: record-offsets.sh <outdir> [interval_s]   (default 10 s; a pass takes about 1.5 s per topic, so a pass that
#   overruns the interval starts the next one at once). Stops when <outdir>/stop exists, then writes <outdir>/done.
# offsets.tsv: <epoch ms before the read> <epoch ms after it> <topic> <partition> <end offset>. The main topic is read
#   first in each pass, so its timestamps are the most precise.
set -u
OUT=${1:?usage: record-offsets.sh <outdir> [interval_s]}; INTERVAL=${2:-10}; MAIN=${KAFKA_TOPIC:-mqtt-devices}
mkdir -p "$OUT"
GET=(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092)
TOPICS=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep '^mqtt-' | grep -vx "$MAIN" | tr '\n' ' ')
TOPICS="$MAIN $TOPICS"
echo "topics: $TOPICS" > "$OUT/topics.txt"
while [ ! -e "$OUT/stop" ]; do
  pass=$(date +%s)
  for t in $TOPICS; do
    s=$(date +%s%3N); raw=$("${GET[@]}" --topic "$t" 2>/dev/null); e=$(date +%s%3N)
    echo "$raw" | awk -F: -v s="$s" -v e="$e" 'NF==3 {printf "%s\t%s\t%s\t%s\t%s\n", s, e, $1, $2, $3}' >> "$OUT/offsets.tsv"
  done
  left=$((INTERVAL - ($(date +%s) - pass)))
  while [ "$left" -gt 0 ] && [ ! -e "$OUT/stop" ]; do sleep 1; left=$((left - 1)); done
done
date +%s%3N > "$OUT/done"
