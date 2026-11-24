# a5-aws-gap-repro — local MQTT->Kafka measurement of aklivity/zilla

Measurement/diagnosis only (no Zilla code changed). Goal: explain the ~1.1 ms/msg seen on AWS (100K QoS1/2 publishers) vs ~105-148 us/msg measured locally on one pinned worker.
Full write-up: [REPORT.md](REPORT.md). Run on 2026-11-24 (~01:50-03:30 PT).

## What it reproduces
1. **Quadratic active-segment hash scan on cache replay/ingest** — CONNACKs stall while one worker is pinned in `findAndMarkAncestor`.
2. **Connection-count cost** — idle CPU, per-message cost, heap/RSS and GC pauses as idle connections per worker grow.

## Environment
Zilla `fc288a3b5` (develop), host JDK 25, module-path launcher, `taskset -c 0`, `-w 1`, `-Xms1g -Xmx2g` (serial GC; one G1 run with `-XX:ActiveProcessorCount=4`). Kafka 4.1.1 single node in Docker on cpu 1. emqtt-bench (docker `emqx/emqtt-bench`) on cpus 2,3. **4 vCPU shared host**, 16 GB.

## How to run (scripts are the ones used; paths made relative, not re-run after that edit)
Build Zilla (`./mvnw clean install -Dskip.checks=true -DskipTests -DskipITs`; the docker-image module is only needed for `target/zpm/repository`, the jar set used on the module path; set `ZILLA_REPO`).
```
./kafka.sh start && ./kafka.sh reset
# sessions-topic replay repro (quadratic):
pip install kafka-python
python3 sample.py session-samples.pkl      # sample real record shapes from a topic populated by a short emqtt-bench run first
python3 prepop.py session-samples.pkl 20000    # 20k clients = 100k records; try 50000 (250k), 200000 (1M)
./replay.sh e1s-20000                      # restarts Zilla, reports time/cpu-s the worker stays busy after the port opens
# steady state / append runs / connection ladder:
./startz.sh X 0 1 && ./measure.sh X 3125 1 60      # 3,125 QoS1 pubs
./e1b.sh                                              # 6 append runs
./e2.sh e2-serial 0 1                                 # idle-conn ladder 0/5k/10k/15k (+3,125 pubs each)
N_LIST="0 15000" JFR=1 ./e2.sh e2-g1 0 1 -XX:+UseG1GC -XX:ActiveProcessorCount=4
```

## Headline numbers
- Baseline 3,125 QoS1 pubs, clean topics: worker 46%, 148 us/msg.
- Sessions-topic replay, no load: 100k records -> worker busy 26 s (14 cpu-s); 250k -> 68 s (52 cpu-s); 1M -> >400 s, killed while still ~90% busy. Fits ~8e-10 s * n^2.
- mqtt-messages replay: ~7.5 us/record (1.4M: 44 s busy; 3.0M: 55 s), linear; RSS grows with topic (mmap).
- Idle connections (serial GC, then +3,125 pubs): 0/5k/10k/15k -> idle CPU 1.6/2.0/4.3/5.4%, us/msg 142/170/165/192, old gen 14/347/680/1048 MB (full), ~66 KB retained per conn, serial full GC 5.6 s at 15k. G1: p50 pause 21 ms, max 421 ms.
- Attribution: connection count explains <=~5% of 1.1 ms at AWS-like 3-6k conns/worker; the quadratic scan explains the CONNACK stall, but its per-message share at 100k publishers is not demonstrated.

## Root-cause pointers (read-only)
- Stack: `KafkaCacheServerFetchFactory.java:1082` (`findAndMarkAncestor`), called from `onServerFanoutReplyData` (`:1015`) -> `KafkaCachePartition.writeEntryStart` (~`:603`) -> `Node.findAndMarkAncestor` (`KafkaCachePartition.java:1382-1416`).
- Active segment hash index is `KafkaCacheFile.HashScan` (`KafkaCacheFile.java:455`, created `KafkaCacheSegment.java:93`) extending `KafkaCacheIndexFile.SortedByValue`, whose `last()`/`lower()` (`KafkaCacheIndexFile.java:461-475, 597-620`) scan linearly over the segment. One call per ingested record; a new key scans the whole segment.
- Possible QoS2 leak, not exercised: `MqttKafkaSessionFactory.java:808` `clientMetadata.put(...)` with no `remove` found.

## Caveats
Shared 4 vCPU host; load generator tops out ~3k sustained pubs; 30-60 s windows; noise ~+-25%. E1(b) run 3 not captured. G1 run partial (14,403 of 15,000 idle connections connected, stages 0 and ~15k only). E2 stopped at 15k. 1M-record sessions replay never finished. QoS2, compaction and tombstone variants not run. `e2-serial-INVALID-idle-containers-failed.out.txt` is an INVALID pass (emqtt-bench `conn` rejected `-t`, idle containers never started) — kept only for completeness, ignore its numbers. Excluded from this directory: JFR (`e2-g1.jfr`), `cts*.txt`, `gc-*.log`, `lg-*.log`, `session-samples.pkl`, engine dir.
