# Local MQTT single-worker harness

Measures Zilla's mqtt -> mqtt-kafka -> kafka pipeline on one pinned worker. `zilla.yaml` is the pipeline (tcp 7183, mqtt server with a memory store, mqtt-kafka proxy, kafka cache and client); `kafka.sh` starts a single-node Kafka 4.1.1 in Docker on cpu 1 and creates the three topics; `zilla.sh <flag> <jfr-out|->` starts Zilla with `-w 1` pinned to cpu 0 and `zilla.binding.mqtt.session.state.store.enabled=<flag>` (it needs `ZILLA_RT` pointing at a `zpmw install` runtime directory holding `.zpm`, built from the modules under test); `measure.sh <label> <pub|pubsub> <flag> [qos] [clients] [window]` runs emqtt-bench on cpus 2-3 (publishers, plus subscribers for `pubsub`), then reports worker CPU from the `/proc` task stat of `engine/worker#0`, the message count from the Kafka end-offset delta, and us per message; `dups.sh <label> <flag> <qos> [clients] [msgs per client]` publishes an exact number of numbered messages (`--payload-hdrs cnt64`) and `segparse.py` reads the Kafka segment files to count records against distinct (topic, counter) ids, which detects duplicate records; `alloc.py` summarizes JFR object allocation samples of the worker. The scripts assume a 4 vCPU host with Docker running, `emqx/emqtt-bench` and `apache/kafka:4.1.1` images, and JDK 25 with `jfr`. JFR does not record Java execution samples from the engine worker thread in this setup, so CPU profiling is not available from it.

## Results summary

Worker CPU is for the engine worker thread over a 60 s window on a 4 vCPU host (about plus or minus 25% noise). Heads: #3541 `d6c5fb1`, C3 `8f3975f964`, C4 `cd8477710a`, all with `zilla.binding.mqtt.session.state.store.enabled=true` unless noted.

| Scenario | Head | Records/s | Worker CPU |
| --- | --- | --- | --- |
| QoS 1 publish, 3,125 clients, 2 runs | #3541 | 3,128 / 3,133 | 35.1 s / 35.7 s (176-178 us per message) |
| QoS 1 publish, 3,125 clients, flag off | #3541 | 3,009 | 38.1 s (194 us) |
| QoS 2 publish, 3,125 clients, 2 runs | C3 | 3,748 / 3,760 (bench catch-up, no duplicates) | 45.3 s / 45.2 s |
| QoS 1 pub-only, 500 clients | C3 / C4 | 500 | 11.2 s / 11.2-11.5 s |
| QoS 1 pub+sub, 500 clients, 2 runs | C3 (rerun) | 500 (recv 500) | 17.0 s / 17.8 s |
| QoS 1 pub+sub, 500 clients, 2 runs | C4 | 500 (recv 500) | 18.4 s / 18.2 s |
| QoS 2 pub-only, 500 clients | C4 | 500 | 13.6 s |
| QoS 2 pub+sub, 500 clients | C3 (1 run) | 500 (recv 445) | 24.2 s |
| QoS 2 pub+sub, 500 clients, 2 runs | C4 | 500 (recv 500) | 22.7 s / 21.5 s |
| QoS 1 pub+sub, 3,125 clients, 2 runs | C4 | 1,425 / 1,565 (not sustained) | 54.3 s / 52.9 s |
| QoS 2 pub+sub, 3,125 clients, 2 runs | C4 | 858 / 809 (not sustained) | 53.2 s / 52.0 s |
| QoS 1, 3,125 publishers + 500 subscribers | C4 | 3,086 | 45.3 s |
| QoS 1, 500 publishers + 3,125 subscribers | C4 | 500 | 19.2 s |
| 3,125 subscribers, no publishers | C4 | 0 | 2.0 s |
| QoS 1 pub+sub, 3,125 clients, flag off, 2 runs | #3541 | 1,011 / 973 (not sustained) | 32.7 s / 32.7 s |

Delivery marginal per delivered message at 500 clients (pub+sub minus pub-only): QoS 1 about 190 us on C3 and about 220 us on C4; QoS 2 about 265 us on C4. The QoS 2 duplicate check (`dups.sh`, 3,125 clients x 40 messages) found 124,918 records for 125,000 offered, all distinct, no duplicates.
