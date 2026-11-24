# B5: SIGSEGV in `KafkaCachePartition$Node.clean` during produce-side segment compaction

Local repro harness and evidence for the Kafka cache crash fixed by
[aklivity/zilla#3540](https://github.com/aklivity/zilla/pull/3540) (`Fixes #3536`).

## Crash

```
SIGSEGV (0xb), si_code SEGV_MAPERR, si_addr 0x0
UnsafeBufferEx.getInt
  <- KafkaCacheDeltaFW.wrap
  <- KafkaCacheFile.readBytes
  <- KafkaCachePartition$Node.clean            (KafkaCachePartition.java:1334, deltaFile.readBytes)
  <- KafkaCacheClientProduceFactory$KafkaCacheClientProduceFan.onClientFanInitialSignalSegmentCompact
```

## Cause

`writeProduceEntryStart` reserves a second `trailersSizeMax` block (256 B) after an entry's trailers.
`writeProduceEntryFin` did not fold it into the entry's trailing `paddingLen`, so a produce entry's
`sizeof()` ended 256 B before the next entry. `clean()` walks the log by `limit()`, landed in the zero
gap, read an entry with `deltaPosition == 0` (not the `-1` sentinel) and read the always-empty
produce-side delta file at position 0, a zero-length mapping at address 0.

In-process dump at the crash (`evidence/diagnostic-runs/run5/zilla5.log`, `DIAG-ENTRY` lines):

| entry | pos | size | limit |
|---|---|---|---|
| 0 | 0 | 1491 | 1491 |
| next walked | 1491 | 97 (zeroed) | 1588 |

Bytes 1491-1746 are zero; the next real entry starts at 1747 (`DIAG-SCAN first=1747`). The 256 B gap equals
`trailersSizeMax`.

## Introduced by, affected versions

`91144fa5d` (aklivity/zilla#2457, 2026-08-30) added the claim block for every produce entry.
`trailersClaimMark` is present in tag 2.4.0 and absent from 2.3.0 and 1.3.4, so 2.4.0 through 2.7.1 and
2.8.0-rc7 are affected (2.4.1-2.7.0 not individually checked).

## Reproduce

- Zilla built from the repo, launched from the module path on the host JDK (no jlink image),
  `start -w 1`, pinned with `taskset -c 0` (`run.sh`; `build.sh` is the build used).
- Config: the `mqtt.kafka.proxy` example `zilla.yaml` with a literal `localhost:9092` bootstrap and
  no telemetry (`zilla.yaml`).
- Kafka 4.1.1 (`apache/kafka:4.1.1`) in Docker with `--network host --cpuset-cpus 1`. Topics
  `mqtt-sessions`, `mqtt-devices` and `mqtt-retained` compacted with `min.cleanable.dirty.ratio=0.01`,
  `segment.ms=10000`, `delete.retention.ms=1000`, `max.compaction.lag.ms=5000`,
  `min.compaction.lag.ms=0`; `mqtt-messages` default.
- Load: `emqx/emqtt-bench` in Docker with `--network host --cpuset-cpus 2,3`:
  `pub -h localhost -p 7183 -c 8000 -I 1000 -s 1024 -q 1 -C true -R 300 -V 5 -t 'place/%i/data'`
  (8,000 publishers, 1 msg/s each).
- Result before the fix: crash 110-135 s after the publishers connect, 4 of 4 runs
  (`evidence/hs_err_pid5338.log`, `evidence/diagnostic-runs/run2..run5`; runs 2-5 carry a temporary
  diagnostic patch that printed the `DIAG*` lines and was not committed).

## Before and after

| | Result |
|---|---|
| before fix | SIGSEGV at the first compaction of a rolled produce segment, 4 of 4 runs |
| after fix | 12m54s under the same load, no crash |

After the fix, `clean` rewrote produce-side segments three times (base offsets 0 at 16:31:35 UTC, 1223426
at 16:34:11, 3058535 at 16:38:01), detected from inode changes in `evidence/after/sample-after.log`
(`sample.sh`, 5 s interval, so closer events can be missed).

`test-before.txt` is the output of the new test against unfixed code:
`KafkaCachePartitionTest.shouldDelimitProduceEntriesByLimit` fails with `expected:<360> but was:<296>`.

## Host

Shared 4 vCPU cloud container, 15 GB RAM; Zilla, Kafka and the load generator pinned to separate cores,
so timings are noisy.

## Notes

- Zilla cache directories, built module jars and build logs are not included.
- Sandbox-specific environment details (proxy port, `PATH`) were redacted from the `hs_err` logs.
