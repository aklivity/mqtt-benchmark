# mqtt-benchmark

MQTT benchmarks for [Zilla](https://github.com/aklivity/zilla)'s MQTT-to-Kafka proxy (`mqtt-kafka`).

It runs an MQTT-to-Kafka throughput scenario and the Basic Set of the
[Open MQTT Benchmark Suite](https://github.com/emqx/mqttbs) with Zilla on AWS. The same setup can also run EMQX with a
Kafka rule, as a baseline measured with the same clients, Kafka and hosts.

## Scenarios

| Scenario | Source | What it does |
|---|---|---|
| `mqtt-kafka-100k` | | 100,000 publishers connecting at 5,000/s, one topic each, 1 msg/s each (100,000 msg/s), QoS 1, 1 KB, all written to one Kafka topic with 16 partitions |
| `basic-conn-10k` | [Open MQTT Benchmark Suite](https://github.com/emqx/mqttbs), Basic Set | 10,000 connections within 100 s, held for 30 minutes |
| `basic-fanout-1-1k` | Basic Set | 1 publisher, 1,000 subscribers on one topic, 1 msg/s in, 1,000 msg/s out, QoS 1, 16 B |
| `basic-p2p-1k` | Basic Set | 1,000 publisher/subscriber pairs, one topic per pair, 1,000 msg/s in and out, QoS 1, 16 B |
| `basic-sharedsub-1k-5` | Basic Set | 1,000 publishers, 5 subscribers in one shared subscription group, MQTT 5, QoS 1, 16 B. Through Zilla, the group is a Kafka share group (Kafka has share groups enabled and the mqtt server binding has a store). |
| `smoke-1k` | | 1,000 publishers for 2 minutes, to check a new environment |
| `*-q2` | | QoS 2 versions of `smoke-1k`, `basic-p2p-1k`, `basic-fanout-1-1k` (QoS 2 publishers and subscribers) and `mqtt-kafka-100k` (see [QoS 2](#qos-2)) |

All scenarios use MQTT 3.1.1 (except shared subscriptions, which need MQTT 5), keep alive 300 s and clean session,
as the suite specifies. Subscribers connect and subscribe before any publisher connects. The Basic Set scenarios run
for 30 minutes; the suite doesn't fix connection rates for the fan-out, point-to-point and shared subscription
cases, so those use 500/s. Through Zilla, every message is written to Kafka and subscribers are served from Kafka.

### QoS 2

The `*-q2` scenarios run the same load with every publish a full QoS 2 exchange (PUBLISH, PUBREC, PUBREL, PUBCOMP),
and `QOS=2 bin/verify-delivery.sh` checks exactly-once delivery into Kafka. The Open MQTT Benchmark Suite's Basic Set
defines no QoS 2 case, so there are no published numbers to compare with.

Each client keeps one QoS 2 exchange in flight (`INFLIGHT=1`): it finishes PUBCOMP before its next PUBLISH. The
delivery check and the probe do the same.

## Infrastructure

This repo provisions the hosts on AWS with Terraform:

| Role | Count | Default instance |
|---|---|---|
| Broker (Zilla or EMQX) | 1 | c6i.8xlarge (32 vCPU, 64 GiB), 500 GB gp3 (Zilla's Kafka cache) |
| Kafka | 1 | c6i.8xlarge, 1 TB gp3 data volume; `mqtt-devices` keeps 15 minutes |
| Load generators | 2 | c6i.2xlarge, emqtt-bench |
| Monitoring | 1 | t3.medium, Prometheus and Grafana |

All hosts run Ubuntu 22.04 in one subnet and one cluster placement group, with the kernel tuning in
`deploy/ansible/files/99-mqtt-benchmark.conf`. One client IP can open about 64K connections to a single broker
address and port, so 100K connections need at least two load generator hosts (50K each). In total the default
setup uses 82 vCPUs: 64 for the broker and Kafka, 16 for the load generators, 2 for monitoring. Check the EC2 on-demand
vCPU quota of the account before provisioning. If the load generators' CPU goes above about 70% during a run
(Prometheus, `role="client"`), raise `client_count` so they don't skew the results.

## Running a benchmark

Prerequisites: an AWS account with credentials in your environment, Terraform 1.5+, Ansible, Python 3, and an SSH key
pair at `~/.ssh/mqtt-benchmark` (`ssh-keygen -t ed25519 -f ~/.ssh/mqtt-benchmark`).

1. **Provision.** Copy `deploy/terraform/terraform.tfvars.example` to `terraform.tfvars` and fill it in, then:
   ```bash
   cd deploy/terraform && terraform init && terraform apply
   ```
   This writes `deploy/inventory.ini` and prints the Grafana and Prometheus URLs.
2. **Deploy.** Installs Docker, tunes the kernel, and starts Kafka (with the topics), the broker, node_exporter,
   Prometheus and Grafana. The broker is Zilla unless you pass `-e broker=emqx`; the images are set in
   `deploy/ansible/group_vars/all.yaml`.
   ```bash
   cd deploy/ansible && ansible-playbook deploy.yaml                  # Zilla
   cd deploy/ansible && ansible-playbook deploy.yaml -e broker=emqx   # EMQX baseline
   ```
   Redeploying switches brokers on the same hosts: it removes both broker containers and starts the chosen one.
3. **Check the environment** with the small scenario and the exact delivery check:
   ```bash
   bin/run-scenario.sh scenarios/smoke-1k.env
   bin/verify-delivery.sh
   ```
   For QoS 2 runs, also check the QoS 2 path:
   ```bash
   bin/run-scenario.sh scenarios/smoke-1k-q2.env
   QOS=2 bin/verify-delivery.sh
   ```
4. **Run the benchmark.** Re-run the deploy step first so Kafka and the broker start empty. The runner reads which
   broker is deployed (and its MQTT port: Zilla 7183, EMQX 1883) from the broker host.
   ```bash
   (cd deploy/ansible && ansible-playbook deploy.yaml)
   bin/run-scenario.sh scenarios/mqtt-kafka-100k.env
   ```
   Do the same for each `scenarios/basic-*.env`, redeploying between runs. Results go to `results/<run-id>/`:
   `summary.json` (made by `bin/summarize.py`), the scenario, the broker config, image versions, emqtt-bench metric
   snapshots and logs, and the publish response time probe output.
5. **Tear down.** Do this as soon as you're done. The default setup costs several dollars per hour.
   ```bash
   cd deploy/terraform && terraform destroy
   ```

## How each number is measured

| Metric | How |
|---|---|
| Connections | emqtt-bench `connect_succ` / `connect_fail` summed over the load generators; connections the broker holds at the end, counted with `ss` on the broker host |
| Connect latency (`basic-conn-10k`) | emqtt-bench's `mqtt_client_handshake_duration` and `mqtt_client_connect_duration` histograms, reported under those names. Neither is a clean CONNECT-to-CONNACK time (the first read 0.07 ms on AWS; the second includes emqtt-bench's pacing), so treat them as indicative only. |
| Message rate into Kafka | Growth of the end offsets of `mqtt-devices` during the measurement window (probe messages excluded) |
| Messages sent | emqtt-bench `pub`. It counts messages sent, not PUBACKs, so it is not a delivery measure. |
| Messages received | emqtt-bench subscribers' `recv`; `delivery_pct` compares it with the messages the publishers sent in the same window, times the deliveries per message (1,000 for fan-out) |
| End-to-end latency | emqtt-bench's publisher timestamp in each message (`--payload-hdrs ts`) against the subscriber's clock. Every host syncs to the Amazon Time Sync Service; point-to-point pairs run on the same host. Percentiles are bucket bounds (1, 5, 10, 25, 50, 100 ms ...), the average is exact. The 8-byte timestamp is part of the payload: publishers send `PAYLOAD_BYTES - 8` bytes plus the timestamp, so the size on the wire matches the scenario. Scenarios without subscribers send no timestamp. |
| Publish response time | `bin/puback-probe.py`: extra connections that publish once per second at the scenario's QoS and time each publish until the broker completes it (avg, p50, p90, p99, p99.9, max): PUBLISH to PUBACK at QoS 1; at QoS 2, PUBLISH to PUBCOMP (`publish_response_ms`) and PUBLISH to PUBREC (`publish_to_pubrec_ms`). emqtt-bench doesn't report this. |
| CPU and memory | node_exporter on the broker and Kafka hosts, averaged over the window in Prometheus |
| Delivery | `bin/verify-delivery.sh`: 20,000 numbered QoS 1 messages; PUBACK count = Kafka records with those keys, no duplicates. `QOS=2`: 20,000 numbered QoS 2 messages (PUBCOMP count = Kafka records, no duplicates), plus 60 QoS 2 exchanges interrupted by a dropped connection after PUBREC, before PUBREC or after PUBREL and resumed on reconnect (`bin/qos2-delivery.py`), each of which must be in Kafka exactly once. It scans the whole topic, so run it after a fresh deploy or the smoke scenario, not after a full-size run. |
| Stability | Broker container restarts; Zilla `AgentTerminationException` lines, or EMQX log lines at error level or above |
| EMQX Kafka action (EMQX only) | EMQX's own `matched`, `success`, `failed` and `dropped` counters for the Kafka action over the window, read from its REST API, to compare with the Kafka offsets |

Zilla's config (`deploy/ansible/templates/zilla.yaml.j2`) has no metrics; its only telemetry is the stdout log
exporter, which reports engine start and stop, client connections and errors. Everything above is measured from the
clients, Kafka and the hosts. Zilla runs with its default number of engine workers, one per vCPU.

EMQX (`deploy/ansible/templates/emqx.hocon.j2`) has one rule, `SELECT * FROM "device/#"`, feeding a Kafka producer
action that writes to `mqtt-devices` with the MQTT topic as key and the payload as value, the same records Zilla
writes. Its partition strategy is `key_dispatch`, like Zilla's; every other Kafka setting is EMQX's default (async,
memory buffer, `max_inflight` 10, `required_acks` `all_isr`). EMQX runs 6.3.3 under its free single-node Community
License.

## What to state with published results

- **Load tool:** emqtt-bench 0.6.3.
- **Cloud:** AWS c6i instances.
- **Kafka:** 4.1.1.
- **EMQX:** 6.3.3 (see above), for the EMQX baseline.
- **Publish response time means different things for the two brokers.** Zilla sends the QoS 1 PUBACK only after
  Kafka has acknowledged the message, so the PUBACK means the message is in Kafka. EMQX's rule writes to Kafka
  asynchronously (the default `query_mode`), so its PUBACK doesn't wait for Kafka; compare EMQX's message rate into
  Kafka and its action's `dropped`/`failed` counters, not only its publish response time.
- **Choices made here:** test duration (30 minutes, as in the Open MQTT Benchmark
  Suite), Kafka producer settings, load generator sizing, disks.

Use [docs/disclosure-report-template.md](docs/disclosure-report-template.md) for every published result.

## Layout

```
scenarios/            scenario definitions (.env)
bin/run-scenario.sh   runs a scenario on the provisioned hosts and writes results/<run-id>/
bin/summarize.py      turns a run's metric snapshots into summary.json
bin/puback-probe.py   publish response time probe, QoS 1 or 2 (runs on a load generator)
bin/verify-delivery.sh exact delivery check into Kafka, QoS 1 or 2
bin/qos2-delivery.py  QoS 2 publishers for the delivery check (runs on a load generator)
deploy/terraform/     AWS: VPC, placement group, broker host, Kafka, load generators, monitoring host
deploy/ansible/       Docker, kernel tuning, Kafka, Zilla or EMQX, emqtt-bench, Prometheus, Grafana
docs/                 disclosure report template
```

## Not done yet

- Grafana dashboards (Prometheus is wired up as the data source; dashboards still need to be built)
- TLS variant of the scenario
- The Open MQTT Benchmark Suite's Enterprise Set (1M connections, 50K:50K point-to-point and others)
