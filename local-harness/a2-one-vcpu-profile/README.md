# a2: one-vCPU MQTT publish profile (Zilla mqtt-kafka)

Measures how much of one vCPU a single Zilla engine worker spends per published
message (QoS 1) or per QoS 2 exchange, and where that time goes. Target being
checked: 100,000 msg/s on 32 vCPU = 3,125 msg/s and 3,125 publishers per vCPU
(about 320 us of one vCPU per message).

## Setup

- Zilla: aklivity/zilla `develop` at e0e504f86 (2.8.0-rc6 merged into develop),
  built with `./mvnw clean install -Dskip.checks=true -DskipTests -DskipITs`.
  Run on the host JDK 25 (jlink image has no JFR) via a module-path launcher
  built with `zpmw install` (see `run-zilla.sh`), `-w 1`, `-Xms1g -Xmx2g`.
- Config: `zilla.yaml` is the examples/mqtt.kafka.proxy config with telemetry
  removed and the Kafka bootstrap server hard-coded to localhost:9092.
- Host: shared 4 vCPU box, 15 GB RAM. Expect noise of about +-25% between runs.
- Pinning: Zilla `taskset -c 0`; Kafka 4.1.1 single node in Docker
  (`--network host --cpuset-cpus 1`); emqtt-bench in Docker
  (`--network host --cpuset-cpus 2,3`).
- Topics (reset before every run by `reset-topics.sh`): mqtt-messages 16
  partitions, mqtt-retained / mqtt-devices / mqtt-sessions compacted, 1 partition.
- Load: `emqtt-bench pub -V 5 -c N -R 300 -I 1000 -q {1,2} -s 1024 -C true
  -t 'place/%i/data'` (1 msg/s per publisher, 1 KB, clean start). The topic
  avoids the mqtt-devices route. The bench ramps connects at about 130/s.

## How to run

`measure.sh <label> <qos> <publishers> [window=60] [jfr=0|1]` restarts Zilla,
resets topics, waits for all connects plus 20 s, then measures a 60 s window:
Kafka offset deltas (single window timed at the midpoint of the offset reads),
and worker thread CPU from `/proc/<pid>/task/<tid>/stat` (thread
`engine/worker#0`). Paths inside the scripts point at the original scratchpad
and need editing. `ZOPTS=-Dzilla.binding.mqtt.kafka.publish.qos.max=1` selects
the QoS-max comparison. `agg.py file.jfr` aggregates a recording.

## Headline numbers

QoS 1 ladder (clean topics each run, `ladder-q1-clean.txt`; 3,125 from the
CPU-time JFR run and the earlier clean run):

| Publishers | msgs/s | Worker CPU | us/msg | Failures |
|---|---|---|---|---|
| 2,000 | 1,990 | 42.0% | 211 | 0 |
| 3,125 | 3,123 | 40.7% / 43.4% | 130 / 139 | 0 |
| 5,000 | 5,006 | 60.4% | 121 | 0 |
| 8,000 | 7,962 | 84.2% | 106 | 0 |

Worker never saturated; QoS 1 ceiling about 9,400 msg/s per worker
(106 us). Marginal cost about 70 us/msg plus a fixed cost of about 28% of the vCPU.

QoS 2 (`results-qos2-and-qos1max1-chain.txt`): 2,000 publishers 46.2% CPU, 230
us/exchange; 3,125 publishers 72-75% CPU, 229-240 us/exchange; ceiling about
4,200-4,400 exchanges/s. Records per exchange: 1.00 to mqtt-messages, 2.00 to
mqtt-sessions. `publish.qos.max=1` at QoS 1, 3,125: 49.1% CPU, 158 us/msg
(single run, no better than default; treat as noise).

No PUBACK latency: emqtt-bench does not report it.

## Profile (3,125 publishers, 60 s, jdk.CPUTimeSample, worker thread only)

Worker CPU by component (leaf-most Zilla/Agrona frame; JDK time is attributed
to the Zilla frame above it):

| Component | QoS 1 | QoS 2 |
|---|---|---|
| tcp binding (socket syscalls) | 27.3% | 19.6% |
| kafka binding client side and streams | 25.9% | 28.6% |
| engine worker loop and ring buffers | 20.4% | 13.1% |
| agrona | 9.0% | 7.4% |
| kafka cache server/client side | 8.8% | 9.0% |
| mqtt-kafka binding | 5.3% | 17.5% |
| mqtt binding | 3.3% | 4.9% |

Top self-time frames:

| Frame | QoS 1 | QoS 2 |
|---|---|---|
| SocketDispatcher.write0 | 20.0% | 12.5% |
| EPoll.wait | 5.8% | 1.6% |
| Int2ObjectHashMap.getMapped | 4.3% | 4.1% |
| AbstractMemorySegmentImpl.checkEnclosingLayout | 4.1% | 6.9% |
| SocketDispatcher.read0 | 3.9% | 3.8% |
| Thread.yield0 | 3.5% | - |
| AbstractMemorySegmentImpl.checkBounds | 2.8% | 3.3% |
| ManyToOneRingBuffer.claimCapacity | 2.7% | 1.5% |
| KafkaChecksum.gfTwoMatrixTimes | - | 3.2% |

Full top-25 lists and inclusive method tables are in `agg-q1.txt`, `agg-q2.txt`.
QoS 1 inclusive: tcp write to Kafka 15.2% plus write to MQTT client 6.2%,
Poller 13.9%, kafka cache produce 8.5%, MQTT publish handling 4.6%. QoS 2 adds
MqttPublishProxy.onMqttMessage 12.9%, session-offsets encode 9.0%,
doCommitOffsetIncomplete 7.6%.

Allocation and GC: the hot path allocates little (6 allocation samples in the
window). The one large site is a capturing lambda in
`KafkaCachePartition.writeProduceEntryStart` (line 893). Serial GC was selected
(single-CPU affinity); one young GC per recording (282 ms pause QoS 1, 70 ms
QoS 2). Rough allocation rate 3-5 MB/s (at most about 1.3 KB per message);
one GC only, so an estimate.

## How the JFR was taken

`-XX:StartFlightRecording=...,settings=profile,jdk.CPUTimeSample#enabled=true,
jdk.CPUTimeSample#throttle=5ms,delay=60s,duration=62s`. `jdk.ExecutionSample`
recorded zero events for the Zilla worker (cause unknown; the same flags work
on a trivial program), so the profile uses the timer-based
`jdk.CPUTimeSample`. The `.jfr` files are not included.

## Caveats

- `CONTAMINATED-ladder-q1-stale-kafka-state.txt` was taken before topics were
  reset between runs; stale Kafka state inflated CPU and caused connect failures.
  Do not use it. Its kafka rates are also inflated by an offset window that
  included slow `docker exec` calls.
- The bench and Kafka share a 4 vCPU box; the 8,000-publisher point may be
  bench-limited. Kafka and bench CPU were not captured reliably.
- Stale accumulated topics made Zilla stop answering CONNACK in two runs; the
  topic reset in `measure.sh` avoids that.
- Single worker with cache and Kafka client on the same thread; not a 32 vCPU result.
