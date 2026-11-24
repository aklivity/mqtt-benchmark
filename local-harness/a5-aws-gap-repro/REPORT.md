# Zilla MQTT->Kafka local measurement (E1, E2) — summary

Scratchpad: $S
Scripts: kafka.sh startz.sh zilla.sh measure.sh replay.sh e1b.sh e2.sh prepop.py sample.py idle-watch.sh
Raw: e1b.out e1s-replay.txt e1s-stacks.txt e2-serial.out e2-g1.out gc-*.log e2-g1.jfr cts6.txt lg-*.log
(e2-serial-INVALID-idle-containers-failed.out = invalid first E2 pass: `emqtt-bench conn` rejected `-t`, idle containers never started; discard)

## Setup / deviations from A2
- A2 scratchpad absent; harness rebuilt. Zilla from develop @ fc288a3b5, module-path launcher on host JDK 25 (jars from cloud/docker-image/target/zpm/repository, `manager*` jar excluded, `--add-exports java.base/jdk.internal.misc=org.agrona`, ZILLA_INCUBATOR_ENABLED=true), `taskset -c 0`, `-w 1`, -Xms1g -Xmx2g, serial GC (1 cpu), mqtt.kafka.proxy config with literal localhost:9092, no telemetry.
- Kafka 4.1.1 single node, docker --network host --cpuset-cpus 1; topics mqtt-messages(16) mqtt-devices(16,compact) mqtt-retained(compact) mqtt-sessions(1,compact).
- emqtt-bench (docker, cpus 2,3): pub v5 -s 1024 -I 1000 -C true -R 300 -q 1 topic place/%i/data. E2 publishers use 127.0.0.2 (port-range headroom); idle conns use `conn -k 60 -C true`.
- worker CPU from /proc/<pid>/task/<tid>/stat (engine/worker#0); records from kafka-get-offsets deltas. Windows 30-60 s (shorter than A2's).
- Docker Hub 429 broke the docker-image module only; irrelevant.

## E1(a) baseline, clean topics, 3,125 QoS1 pubs, 60 s
worker 46.3 %, 148 us/msg (A2: ~130), CONNACK ramp for 3,125 conns 36 s (load-gen -R 300 requested, ~86/s achieved).

## E1(b) append runs (Zilla restarted each run, Kafka data kept, 30 s window)
run | CONNACK s | worker % | us/msg | RSS MB | sessions recs (cum) | messages recs (cum)
1 | 44 | 51.5 | 165 | 4014 | 28k | 496k
2 | 36 | 45.8 | 146 | 4160 | 47k | 669k
4 | 46 | 58.5 | 181 | 4609 | 94k | 1.04M
5 | 47 | 46.0 | 147 | 4790 | 126k | 1.20M
6 | 65 | 62.1 | 199 | 5222 | 161k | 1.40M
(run 3 not recorded in my tail view.) Steady-state us/msg drifts up only modestly (146 -> 199, noisy). No connect failures. CONNACK completion +30 s in run 6 ~ the startup replay below.
Startup with data in place and NO load (replay.sh): port opens after ~7 s but the worker is 50-94 % busy for a long time:
 messages 1.4M recs (+sessions 190k): busy 44 s, 28 cpu-s;  messages 3.0M: busy 55 s, 40 cpu-s, RSS 6.4 GB (=> ~7.5 us/record, ~12 cpu-s per extra 1M x 1.1 KB records; linear, mmap RSS grows with topic).
## E1(c) sessions topic pre-populated (realistic shapes sampled from real records: base tombstone, #offsets tombstone + 1,412 B value, #expiry tombstone + 84 B value, 5 recs/client, uncompacted), clean mqtt-messages
sessions records | worker busy after port-ready | worker cpu-s (incl ~6 s JVM start)
100k  | 26 s  | 14.2
250k  | 68 s  | 51.6
1M    | >400 s, still 90 % busy when killed (398 cpu-s) | n/a
=> time ~ k * n^2 (k ~ 8e-10 s/record^2 from 100k net 8 s; predicts 250k ~50 s (obs 45), 1M ~800 s, 3M ~2 h). QUADRATIC, not linear.

### Root cause (confirmed by stacks + code, not fixed)
Stacks (3 dumps 5 s apart, e1s-stacks.txt) all in KafkaCacheServerFetchFanout.findAndMarkAncestor (KafkaCacheServerFetchFactory.java:1082; one in KafkaCacheIndexFile$SortedByValue.sortByKey via HashScan.freeze, i.e. segment roll).
Path: onServerFanoutReplyData (KafkaCacheServerFetchFactory.java:1015) -> KafkaCachePartition.writeEntryStart (KafkaCachePartition.java:~603, `findAncestor.apply(keyHash)`) -> findAndMarkAncestor -> Node.findAndMarkAncestor (KafkaCachePartition.java:1382-1416) -> `hashFile.last(hash)` / `hashFile.lower(hash,cursor)`.
The ACTIVE segment's hash index is `KafkaCacheFile.HashScan extends KafkaCacheIndexFile.SortedByValue` (KafkaCacheFile.java:455; KafkaCacheSegment.java:93). SortedByValue.last()/lower() (KafkaCacheIndexFile.java:461-475, 597-620) are LINEAR scans backward over every entry of the segment. It is called once per ingested record, and a key not yet in the segment (a new client id, a new topic key) scans the whole segment => O(n^2) per segment (segment = 1 GiB default, cache.segment.bytes). Only frozen segments (HashIndex, SortedByKey) are binary-searched.
This also executes for records written by Zilla itself (produce -> fetch-back into cache), and for the sessions topic (compact, 1 partition, unique key per client per suffix, so every new client = 3 full-miss scans).
=> Explains observation 1 (CONNACKs stop answering once mqtt-sessions/mqtt-messages hold enough data: single worker pinned inside this scan while connects queue). Real-world sessions accumulate 3-5 uncompacted records per connect (compaction does not run on the active segment).
Minimal repro: reset topics; produce N unique keys (e.g. 5 records x 20k..200k client ids) to mqtt-sessions (prepop.py); start Zilla with `-w 1`; watch worker thread CPU / Thread.print. Time grows ~n^2 as above.
I did not test compaction run effect (not enough time) nor key-less records (key.length()==-1 skip the scan; perf-test filler records replayed linearly).

## E2 idle connections + 3,125 QoS1 pubs, serial GC (Xmx2g, 1 cpu), clean topics at start
N idle | idle worker % | RSS MB (idle) | tenured used MB (idle) | us/msg @3,125 | pub worker % | CONNACK(3,125) s
0      | 1.6 | 327  | 14   | 142 | 44.4 | 38
5,000  | 2.0 | 3840 | 347  | 170 | 53.2 | 13.6
10,000 | 4.3 | 4861 | 680  | 165 | 51.6 | 12.5
15,000 | 5.4 | 5861 | 1048 (heap FULL) | 192 | 60.0 | 12.5
- idle cost ~ +0.25 % cpu per 1,000 keepalive-60s conns (~2.5 us cpu per conn-second; ~150 us per PINGREQ round-trip incl. read+write syscalls).
- heap ~ 66 KB/conn retained (tenured delta 5k->10k; A4 saw 39.6 KB); old gen fills at ~15k conns with Xmx2g.
- RSS is dominated by mmap'd Kafka cache/engine files (not heap): 3.5-4 GB at 3,125 pubs, grows with data in topics.
- us/msg rises ~ +3 to +3.5 us per 1,000 idle conns (noisy: clean-run repeats vary 146-199), i.e. ~ +50 us at 15k.
GC (serial): young pauses 250-390 ms each once old gen is 700 MB-1.2 GB; one FULL GC of 5,577 ms (1479M->1038M) at the 15k stage (gc-e2-serial.log); older fulls 22/34 ms.
G1 (+ActiveProcessorCount=4, still pinned to cpu 0), stages 0 and ~15k: us/msg 167 -> 190; idle 15k worker 8.3 % (14,403 of 15,000 idle conns actually connected; remaining stalled — unexplained, caveat); G1 pauses n=24 p50 21 ms max 421 ms (e2-g1 gc log); heap 811 MB used at 15k.
CPUTimeSample at ~15k idle + 3,125 pubs (e2-g1.jfr, 5,690 worker samples): socket write0 7.7 %, read0 4.4 %, MemorySegment bounds/layout checks (checkEnclosingLayout/checkBounds/get) ~9 %, Int2ObjectHashMap.getMapped 3.3 %, ring buffer claimCapacity 2.5 %, epoll 2.4 %, KafkaCacheIndexFile$SortedByValue.last 1.8 % (the linear scan, small here because 3k keys). No timer/clientMetadata/keepalive hotspot; nothing O(connections) visible in the top frames.
clientMetadata: MqttKafkaSessionFactory.java:808 `clientMetadata.put(affinity, metadata)` only when publishQosMax == 2, and I found no `remove` anywhere in binding-mqtt-kafka => leak per QoS2 connection (not exercised: all runs here QoS1). Not measured.

## Attribution of the 1.1 ms
- Steady-state publish path (A2) 105-140 us; reproduced 142-148 us here.
- Connection count (E2): at AWS's ~3k-6k conns/worker adds ~10-20 us/msg; at 15k ~+50 us. <= ~5 % of 1.1 ms. GC at 15k on 1 cpu is damaging (5.6 s full GC) but that needs >10k conns/worker with a 2 GB heap.
- Accumulated Kafka state (E1): explains the CONNACK stall completely (quadratic scan) and per-run ramp slowdowns; per-message steady-state cost from it is small at 3k keys but not measured at 100k distinct keys per partition/segment (distance-to-last-occurrence scan ~ records since the key's previous record; first record per key scans the whole segment). It could be a large part of ~1.1 ms in AWS, but I have NOT demonstrated that; no local number gets near 1.1 ms.

## E3 — still missing / cheapest next local experiment (not started)
Per-message scan cost at high distinct-key counts: with `-w 1`, a 1-partition mqtt-messages, run 8,000 publishers at 1 msg/s (and a variant with a pre-filled active segment of 1-2M keyed records via prepop-style producer) and compare us/msg vs the 3,125-publisher 148 us; CPUTimeSample share of SortedByValue.last/lower should scale ~ linearly. Also: 2-4 workers sharing the one sessions partition fanout (which worker runs findAndMarkAncestor and head-of-line blocks others), and the QoS2 path with clientMetadata growth.

## Caveats
4 vCPU shared host; load generator ceiling (~3k pubs sustained, ramp ~90-300 conn/s); 30-60 s windows; run-to-run noise +-25 %; E2 only to 15k (25k not done), G1 run shortened (2 stages) and only 14.4k idle conns connected; E1(b) run 3 not captured; compaction variant and tombstone-vs-no-tombstone variant not run; 1M sessions replay never finished (killed after >400 s); QoS2 not measured.
