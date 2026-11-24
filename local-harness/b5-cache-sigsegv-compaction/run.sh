#!/bin/bash
# usage: run.sh <yaml> <logfile>
cd /tmp/zl
export KAFKA_BOOTSTRAP_SERVER=localhost:9092
exec taskset -c 0 java -XX:ErrorFile=/tmp/zl/hs_err_pid%p.log -Dzilla.engine.directory=/tmp/zl/dir \
  -Dzilla.engine.config.url=file:$1 \
  --add-opens java.base/sun.nio.ch=org.agrona --add-opens java.base/java.nio=org.agrona --add-opens java.base/jdk.internal.misc=org.agrona --add-exports java.base/jdk.internal.misc=org.agrona --enable-native-access=ALL-UNNAMED,io.aklivity.zilla.runtime.common.agrona \
  -p /tmp/zl/mods -m io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain start -w 1 -c $1 -v > $2 2>&1
