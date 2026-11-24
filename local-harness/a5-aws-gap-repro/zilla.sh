#!/bin/bash
S=${S:-$(cd "$(dirname "$0")" && pwd)}
ZILLA_REPO=${ZILLA_REPO:-/home/user/zilla}
# usage: zilla.sh <cpuset> <workers> <config> <logfile> [extra jvm opts...]
CPU=$1; W=$2; CFG=$3; LOG=$4; shift 4
MP=$(find $ZILLA_REPO/cloud/docker-image/target/zpm/repository -name "*.jar" -not -name "manager*" | tr '\n' ':')
mkdir -p ${S:-$(cd "$(dirname "$0")" && pwd)}/zdir; rm -rf $S/zdir/*
export ZILLA_INCUBATOR_ENABLED=true
exec taskset -c $CPU java -Xms1g -Xmx2g "$@" -Dzilla.engine.directory=${S:-$(cd "$(dirname "$0")" && pwd)}/zdir -Dzilla.engine.workers=$W \
 -XX:+UnlockDiagnosticVMOptions --add-exports java.base/jdk.internal.misc=org.agrona --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED -p "$MP" -m io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain start -v -c $CFG -w $W -P zilla.engine.directory=${S:-$(cd "$(dirname "$0")" && pwd)}/zdir -e > $LOG 2>&1
