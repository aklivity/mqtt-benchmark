#!/bin/bash
# usage: run-zilla.sh [extra JVM opts...]; pinned to vCPU 0, one worker
cd /tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad
rm -rf /tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/engine-dir; mkdir -p /tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/engine-dir
export KAFKA_BOOTSTRAP_SERVER=localhost:9092
exec taskset -c 0 java "$@" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=org.agrona \
 --enable-native-access=io.aklivity.zilla.runtime.common.agrona --sun-misc-unsafe-memory-access=deny \
 -Xms1g -Xmx2g \
 -Dzilla.directory=/tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/engine-dir -Dzilla.engine.config.url=file:/tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/zilla.yaml -Dzilla.engine.directory=/tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/engine-dir \
 -p /tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/zilla/.zpm/modules --add-modules ALL-MODULE-PATH \
 -m io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain start -v -w 1 -c /tmp/claude-0/-home-user-zilla/0f5558be-14fa-516b-9e3c-2118eebb58a6/scratchpad/zilla.yaml
