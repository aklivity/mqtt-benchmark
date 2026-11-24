# b4: Kafka cache active-segment hash lookup

Local harness and evidence for the quadratic ancestor lookup in the Zilla Kafka cache's active
segment (`KafkaCacheIndexFile.SortedByValue.last/lower`, one scan per ingested keyed record), fixed in
aklivity/zilla#3537 (`fix(binding-kafka): index active segment hash entries by key for ancestor lookup`).
Everything here ran locally (no cloud) on a shared 4 vCPU host; treat results as noisy, about +-25%.

## Zilla builds compared
- before: parent of the fix commit, `698a114b6ca5ae9a187f7c06903ee0318dadf00b` (develop at the time)
- after: `eb6ee859635cf10ad06cc84b9bb2dd5841140ea2` (the PR #3537 commit, merged to develop)
- Both were run as jar sets on the Java module path (no Docker image build): engine, command,
  command-start, binding-tcp/mqtt/mqtt-kafka/kafka, store-memory, common-*, agrona, jakarta json,
  yasson. `dist/jars` held the after set and `dist/jars-before` the same set with the before
  `binding-kafka` jar. They are not committed (regenerate from the two commits).

## Layout
- `etc/zilla.yaml`: tcp -> mqtt -> mqtt-kafka -> kafka cache_client/cache_server/client -> tcp,
  `localhost:9092`, bootstrap topics `mqtt-sessions`, `mqtt-messages`, `mqtt-retained`.
- `gen.py <ids>`: emits `key:value` lines, 5 records per client id (`<id>`, `<id>#offsets`,
  `<id>#expiry`, a tombstone of `<id>#expiry`, an update of `<id>`).
- `prep.sh <ids>`: recreates the topics (1 partition) and produces the `gen.py` records into `mqtt-sessions`.
  Expects a Kafka container named `kafka` (apache/kafka 3.8.0, KRaft, host network, pinned to cpu 1).
- `replay.py <jars> <timeout> <label>`: starts Zilla (`taskset -c 0`, `-w 1`), and samples the
  `engine/worker#0` thread CPU from `/proc/<pid>/task/<tid>/stat` after the port opens until it is idle.
  Prints worker CPU seconds after port open.
- `prep2.sh <prefill>`: recreates the topics for the publish experiment (`mqtt-sessions` and
  `mqtt-retained` compacted with `min.cleanable.dirty.ratio=1.0`, see caveats) and prefills `mqtt-messages`
  with unique keyed records.
- `load.py <jars> <label> <publishers> <window-s>`: starts Zilla, waits for replay to go idle, runs
  `emqx/emqtt-bench:0.4.19 pub -c <n> -s 1024 -I 1000 -C true -R 300 -q 1 -V 5 -t bench/%i` on cpus 2,3,
  waits for the connect ramp, then measures the window: worker CPU, messages (from the `mqtt-messages`
  end offset) and a JFR `jdk.CPUTimeSample` recording (`NOJFR=1` skips JFR). `jdk.ExecutionSample` records
  nothing for the worker.
- `jfrshare.py <window.jfr>`: share of worker samples whose stack contains `SortedByValue.last/lower`
  and `findAndMarkAncestor`.
- `matrix.sh`: runs before/after x prefill 0/1,000,000 at 8,000 publishers, 60 s windows.
  Needs `dist/jars` and `dist/jars-before` next to the scripts.
- `results/`: `before.txt`, `after.txt` (JMH `KafkaCacheHashScanBM`), `matrix.log` (the final full
  matrix; the first attempt crashed, see below, and measured 109.1 us/msg for before with no prefill
  with a 25 s ramp, consistent with the rerun).
- `crash/`: two `hs_err` logs (sandbox JVM option and PATH lines redacted).

## How to run
1. Build the before and after `binding-kafka` jars and assemble the two jar sets.
2. `docker run -d --name kafka --network host --cpuset-cpus 1 ...` (single-node KRaft, 1 partition topics).
3. Replay: `./prep.sh 20000` (100k records), `python3 replay.py <jars> 600 <label>`; repeat with 50000 and 200000 ids.
4. Publish: `./matrix.sh`.
5. JMH: `./mvnw test-compile -pl runtime/binding-kafka`, then run `KafkaCacheHashScanBM` per `runtime/AGENTS.md`.

## Headline numbers
- JMH, per lookup, 10k to 500k keys indexed: miss 16.7 us to 960 us before, about 2-12 ns flat after;
  oldest key plus `lower` walk 35.8 us to 1,591 us before, about 20 ns after.
- Replay worker CPU after the port opens: 100k records 2.8 s to 1.4 s; 250k 11.6 s to 2.3 s; 1M 186 s to 5.4 s.
- Publish, 8,000 publishers, 1 KiB, 1 msg/s each, microseconds of worker CPU per message:
  before 110.5 (empty cache), after 92.7, before 116.7 (1M-record prefill), after 89.1.
  Replay of the 1M prefill before the fix took 261 s of worker CPU, after the fix 4 s.
- JFR: `findAndMarkAncestor` is about 20% of worker CPU samples before (21.3% and 19.7%) and 1.3% after.

## Publish path key
The mqtt-kafka publish path keys `mqtt-messages` records by the MQTT topic name
(`MqttKafkaPublishFactory.java:473-483`, set on every record at `:354`); the key is never -1, so
every message runs the lookup. `hashKey` (`:487-493`) only picks the partition.

## Caveats
- Shared 4 vCPU host, single run per configuration, noise about +-25%.
- 8,000 publishers saturates one worker (76-83% busy) and the generator shares the host; read the figures as
  CPU per message, not capacity.
- `SortedByValue.last/lower` frames show only 0.3-0.4% of samples, likely because the scan loop is inlined
  into `findAndMarkAncestor`; the subtree share is the reliable figure.
- Unrelated pre-existing bug: with 8,000 publishers and compacted `mqtt-sessions`/`mqtt-retained`, the JVM
  aborted with SIGSEGV in `KafkaCachePartition$Node.clean` (compaction signal reading a delta entry)
  about 110 s after publishers connected, on the before and after builds alike (`crash/`). The runs here set
  `min.cleanable.dirty.ratio=1.0` on those topics and shortened the ramp so the window ended before it.
  `compactAt` also uses `Math.min(dirtySince + minCompactionLag, now)`, so a long lag does not defer cleaning.
- `mqtt-sessions` must be compacted for CONNECT to succeed in this setup.
