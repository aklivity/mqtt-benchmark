#!/bin/bash
# usage: zilla.sh [extra start args...]
S="$(cd "$(dirname "$0")" && pwd)"
docker rm -f zilla >/dev/null 2>&1
docker run -d --name zilla --network host -v $S/etc:/etc/zilla \
 -e KAFKA_BOOTSTRAP_SERVER=localhost:9092 -e ZILLA_INCUBATOR_ENABLED=true \
 ghcr.io/aklivity/zilla:develop-SNAPSHOT start -v -e -w 2 "$@" >/dev/null
for i in $(seq 60); do docker exec zilla test -f /var/run/zilla/ready 2>/dev/null && { echo zilla ready; exit 0; }; sleep 1; done
echo zilla NOT ready; docker logs zilla | tail -20
