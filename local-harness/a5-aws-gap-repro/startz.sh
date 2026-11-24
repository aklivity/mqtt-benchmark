#!/bin/bash
# usage: startz.sh <label> [cpuset] [workers] [jvm opts...]
S=${S:-$(cd "$(dirname "$0")" && pwd)}
L=$1; CPU=${2:-0}; W=${3:-1}; shift 3
pkill -f ZillaMain; sleep 2
T0=$(date +%s.%N)
nohup $S/zilla.sh $CPU $W $S/zilla.yaml $S/zilla-$L.log -Xlog:gc*:file=$S/gc-$L.log "$@" >/dev/null 2>&1 &
until (exec 3<>/dev/tcp/127.0.0.1/7183) 2>/dev/null; do sleep 0.5; done
echo "zilla port ready after $(echo "$(date +%s.%N)-$T0"|bc) s"
sleep 5
