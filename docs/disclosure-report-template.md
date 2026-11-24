# Full disclosure report: <scenario> on <broker> <version>

The Open MQTT Benchmark Suite asks every published result to come with enough detail to reproduce it. Fill in one
report per published run and keep it next to the run's `results/<run-id>/` directory.

## Summary

| Metric | Result |
|---|---|
| Connections (target / connected / failed) | |
| Message rate into Kafka (target / measured) | |
| Publish response time (avg / p50 / p90 / p99 / max) | |
| Broker host CPU (avg) | |
| Broker host memory used (avg) | |
| Exact delivery check (`bin/verify-delivery.sh`) | |
| Broker restarts / errors logged | |

## Scenario

`scenarios/<name>.env`, copied from `results/<run-id>/scenario.env`. State any change from the scenario file in this repo.

## Software

| Component | Version / image digest |
|---|---|
| Broker (Zilla or EMQX) | |
| Kafka | |
| emqtt-bench | |
| OS / kernel | |

Broker configuration: `results/<run-id>/zilla.yaml` or `emqx.hocon`. Kafka topics: name, partitions, replication factor, configs.

## Hardware and network

| Role | Count | Instance type | vCPU | Memory | Storage |
|---|---|---|---|---|---|
| Broker | | | | | |
| Kafka | | | | | |
| Load generators | | | | | |

Region, availability zone, placement group, and kernel settings (`deploy/ansible/files/99-mqtt-benchmark.conf`).

## Method

- How load was generated and split across hosts
- Warm-up and measurement window
- How each metric was measured (publish response time: `bin/puback-probe.py` on N probe connections, etc.)
- Load tool, cloud, Kafka version, and what publish response time includes

## Raw data

Link to `results/<run-id>/` (summary.json, client logs, broker log) and the Grafana dashboard snapshot.
