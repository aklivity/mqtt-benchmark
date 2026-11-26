#!/usr/bin/env bash
# Records per record batch in the largest segment of one partition of a topic, read from the Kafka container's log.
# Usage on the Kafka host: kafka-batch-stats.sh <topic> <partition>
# Prints: segment <file> batches <n> records <n> producerIds <n>. A producer id per record means every batch holds one record.
set -u
dir=/var/lib/kafka/data/$1-$2
f=$(docker exec kafka sh -c "ls -S $dir/*.log 2>/dev/null | head -1")
docker exec kafka /opt/kafka/bin/kafka-dump-log.sh --files "$f" 2>/dev/null | awk -v f="$f" '
  /^baseOffset:/ { b++; for (i = 1; i <= NF; i++) { if ($i == "count:") r += $(i + 1); if ($i == "producerId:") p[$(i + 1)] = 1 } }
  END { n = 0; for (k in p) n++; print "segment", f, "batches", b + 0, "records", r + 0, "producerIds", n }'
