#!/bin/bash
# usage: zilla.sh <flag true|false> <jfr-out or ->
S="${HARNESS_DIR:-$(cd "$(dirname "$0")" && pwd)}"
RT="${ZILLA_RT:?set ZILLA_RT to a zpmw-installed runtime dir containing .zpm}"
pkill -f ZillaMain; sleep 1
JFR=""
[ "$2" != "-" ] && JFR="-XX:StartFlightRecording=filename=$2,settings=profile,dumponexit=true,maxage=10m"
export JAVA_TOOL_OPTIONS=""; export _JAVA_OPTIONS=""
cd $S
nohup taskset -c ${JVMCPUS:-0} java -Xms1g -Xmx1g $JFR -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=org.agrona \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -Dzilla.directory=$S/zd \
  --module-path $RT/.zpm/modules:$RT/.zpm/generated/modules/java.inject.jar:$RT/.zpm/generated/modules/org.agrona.jar \
  -m io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain \
  start -c $S/zilla.yaml -w 1 -e -P zilla.binding.mqtt.session.state.store.enabled=$1 \
  > $S/zilla.log 2>&1 &
