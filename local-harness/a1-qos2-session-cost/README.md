# a1: cost of the default QoS 2 session path in mqtt-kafka

Measures what `publish.qos.max` (default 2) costs every MQTT connection in the
mqtt-kafka binding, compared with `publish.qos.max=1`, using local Docker runs.

## Setup used
- Zilla commit 177fef5f5 (detached HEAD), image `ghcr.io/aklivity/zilla:develop-SNAPSHOT` built from that checkout, `start -w 2`
- Kafka `apache/kafka:4.1.1`, single node, topics mqtt-messages (16p), mqtt-retained (1, compact), mqtt-devices (16, compact), mqtt-sessions (1, compact)
- Load generator `emqx/emqtt-bench`, MQTT v5, clean start, 100 connections/s
- Host: shared 4 vCPU / 15 GB box; Zilla, Kafka and the load generator share it. Treat CPU numbers as +-25% noise.
- Property names: `zilla.binding.mqtt.kafka.publish.qos.max=1` and `zilla.binding.mqtt.publish.qos.max=at_least_once`, passed with `zilla start -P name=value`

## Run
`run.sh <label> <storm|pub> <clients> <qos> <total_secs> [zilla args...]` starts a fresh Kafka and Zilla (`kafka.sh`, `zilla.sh`), runs the load, and writes `res/<label>/`
(summary, stats, bench.log, zilla.log, zilla.errcount). `offsets.sh` prints per-topic record totals.
Example: `./run.sh storm-q1-1 storm 5000 1 95 -P zilla.binding.mqtt.kafka.publish.qos.max=1 -P zilla.binding.mqtt.publish.qos.max=at_least_once`.
Needs a running Docker daemon. `etc/zilla.yaml` is the repo's mqtt.kafka.proxy example on port 1883.

## Headline numbers (records on mqtt-sessions per clean connection lifecycle)
| | default (2) | qos.max=1 |
|---|---|---|
| at CONNECT | 3 | 2 |
| at DISCONNECT | 4 | 4 |

- Connection storm, 5,000 clients at 100/s: all connected in about 52 s, no CONNACK failures, in both conditions, 3 runs each with identical record totals (35,000 vs 30,000 after disconnect). The CONNECT/DISCONNECT split comes from the third run of each condition only.
- The record removed at CONNECT is the non-null `#offsets` seed.
- QoS 1 publishing, 5,000 publishers at 1 msg/s, 1 KB: about 5,025 msgs/s reached mqtt-devices in both conditions; no measurable difference in Zilla CPU (about 107-114%, one run each). Nothing was written to mqtt-sessions during publishing.
- QoS 2 publishing (default only), 2,000 publishers: per exchange 1.00 record on mqtt-devices plus 2.006 `#offsets` records on mqtt-sessions, i.e. 3 records per exchange.

## Caveats
- One run each for the publish scenarios; storm runs were 3 each.
- No PUBACK latency (emqtt-bench did not report it), no heap figures (no jcmd in the image; RSS includes mmap'd cache), no Kafka request counts.
- Early runs (1 and 2) of the storm scenario read offsets only after disconnect.
