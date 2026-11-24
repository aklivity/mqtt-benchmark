#!/bin/bash
# usage: idle-watch.sh <seconds>  samples worker thread cpu% and rss every 5s
PID=$(pgrep -f ZillaMain | head -1)
TID=$(for t in /proc/$PID/task/*; do [ "$(cat $t/comm)" = "engine/worker#0" ] && basename $t; done | head -1)
P=$(awk '{print $14+$15}' /proc/$PID/task/$TID/stat)
for i in $(seq 1 $(($1/5))); do sleep 5; C=$(awk '{print $14+$15}' /proc/$PID/task/$TID/stat); echo "t=$((i*5))s worker_cpu=$(( (C-P)*2 ))% rss=$(awk '/VmRSS/{print int($2/1024)}' /proc/$PID/status)MB"; P=$C; done
