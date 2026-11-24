#!/bin/bash
while true; do
  echo "== $(date +%T)"
  find /tmp/zl/.zilla/engine/cache/kafka -name '*.log' -path '*-0-0/*' -printf '%i %s %p\n' 2>/dev/null | sed 's#/tmp/zl/.zilla/engine/cache/kafka/[^/]*/##'
  sleep 5
done
