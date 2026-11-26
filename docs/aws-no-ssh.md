# Running the benchmark without SSH

The normal flow ([README](../README.md#running-a-benchmark)) needs an SSH client and outbound port 22: Ansible and
`bin/*.sh` connect to every host. This flow is for an operator host that has neither but can call AWS over HTTPS.

A small **controller** instance in the benchmark subnet runs the unchanged Ansible playbook and `bin/run-scenario.sh` /
`bin/verify-delivery.sh` over SSH on private addresses. The operator drives only the controller, through Systems Manager
Run Command, and moves files through one S3 bucket. The SSH flow still works as before.

```
operator host --HTTPS--> SSM Run Command / S3 <--HTTPS-- controller --SSH, private IPs--> broker, Kafka, clients, monitor
```

## What you need

On the operator host: Python 3 with `boto3`, Terraform 1.5+, `tar`, and for the Zilla image option Docker. No `ssh`,
no `aws` CLI.

An SSH key pair that exists only for this benchmark: its public half goes on every host, and its private half is
copied onto the controller. Without `ssh-keygen`:

```bash
python3 - <<'PY'
import os
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat
key = Ed25519PrivateKey.generate()
path = os.path.expanduser("~/.ssh/mqtt-benchmark")
with open(path, "wb") as f:
    os.fchmod(f.fileno(), 0o600)
    f.write(key.private_bytes(Encoding.PEM, PrivateFormat.OpenSSH, NoEncryption()))
with open(path + ".pub", "wb") as f:
    f.write(key.public_key().public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH) + b"\n")
PY
```

Credentials, three sets (they may be the same ones):

| For | Needs |
|---|---|
| `deploy/terraform` | EC2, VPC and placement group management, `ssm:GetParameter` on the Canonical AMI parameter. No IAM. |
| `deploy/terraform-controller` | The above for one instance, plus creating an S3 bucket, an IAM role, an instance profile and `iam:PassRole` for it: the elevated set. |
| `bin/aws/*` | `ssm:SendCommand` (on the controller and the `AWS-RunShellScript` document), `ssm:GetCommandInvocation`, `ssm:CancelCommand`, and `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the bucket. Passed as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`. |

## Flow

1. **Provision the benchmark hosts.** In `deploy/terraform/terraform.tfvars` pass a harmless placeholder as
   `operator_cidrs` (`["127.0.0.1/32"]`): the hosts are then reachable only from inside the VPC. Set `public_key_path`
   to the key above, and set `max_lifetime_minutes` (for example 300, 5 hours; the default 0 leaves the guard off) so
   forgotten hosts terminate themselves.
   ```bash
   cd deploy/terraform && terraform init && terraform apply
   terraform output network      # vpc_id, subnet_id, security_group_id
   ```
   Besides `deploy/inventory.ini` this writes `deploy/inventory-private.ini`, the same hosts with private addresses.
2. **Create the controller**, with the elevated credentials. Copy `deploy/terraform-controller/terraform.tfvars.example`
   to `terraform.tfvars`, fill in the `subnet_id` and `security_group_id` from step 1, a globally unique `bucket_name`
   and `ssh_private_key_file`, then:
   ```bash
   cd deploy/terraform-controller && terraform init && terraform apply
   ```
   Every named resource carries the `name_prefix` variable (default `mqtt-benchmark`) in both Terraform modules: set a
   unique one in each `terraform.tfvars` when several deployments share the AWS account and region (the IAM role and
   instance profile names are account-wide).
   It creates the S3 bucket (public access blocked), the role `<name_prefix>-ssm` (default `mqtt-benchmark-ssm`; SSM agent permissions plus
   get/put/list/delete on that bucket only) with its instance profile, and one `t3.medium` in the benchmark subnet.
   Its first-boot script installs Ansible, the AWS CLI and `jq`, and puts the key and an `ssh_config` that skips host
   key checks inside the VPC on the controller.
3. **Switch to the operator credentials** and point the tools at the controller:
   ```bash
   export CONTROLLER_ID=$(cd deploy/terraform-controller && terraform output -raw controller_id)
   export BUCKET=$(cd deploy/terraform-controller && terraform output -raw bucket_name)
   export AWS_REGION=us-east-1
   bin/aws/ssm-run.py 'cloud-init status --wait'     # wait for the setup (retry if the instance is not yet registered)
   ```
4. **Deploy**, with the same `-e` variables as the SSH flow:
   ```bash
   bin/aws/deploy.sh
   bin/aws/deploy.sh -e broker=emqx
   ```
5. **Check, then run:**
   ```bash
   bin/aws/run-scenario.sh scenarios/smoke-1k.env
   bin/aws/verify-delivery.sh                 # QOS=2 bin/aws/verify-delivery.sh
   bin/aws/deploy.sh                          # empty Kafka and broker again before a full-size run
   DIAG=1 bin/aws/run-scenario.sh scenarios/mqtt-kafka-100k.env
   ```
   Results arrive in the local `results/<run-id>/`, as in the SSH flow. While a run is going, watch it with
   `bin/aws/ssm-run.py 'tail -n 20 /opt/mqtt-benchmark-src/results/*/run.log'`.
6. **Tear down, controller first.** The controller sits in the benchmark security group, so the group (and the VPC)
   cannot be deleted while it exists. Use the elevated credentials for the first command:
   ```bash
   (cd deploy/terraform-controller && terraform destroy)
   (cd deploy/terraform && terraform destroy)
   ```
   The bucket is emptied and removed with the controller root.

## Tools

| Tool | What it does |
|---|---|
| `bin/aws/ssm-run.py 'cmd'` | Runs `cmd` as `ubuntu` on the controller, prints stdout and stderr, exits with its exit code. `--timeout SECONDS` (default 7200, maximum 172800) sets both SSM timeouts. Output is also written to `s3://$BUCKET/ssm-output/` and read back from there when over the 24,000-character inline limit. |
| `bin/aws/push-bundle.sh` | Uploads `deploy/ansible`, `bin`, `scenarios` and `deploy/inventory-private.ini` as `s3://$BUCKET/bundle.tgz` and unpacks it in `/opt/mqtt-benchmark-src` on the controller. The scripts below call it first, so local edits are always what runs. |
| `bin/aws/deploy.sh [ansible args]` | `ansible-playbook deploy.yaml` on the controller with the private inventory. |
| `bin/aws/run-scenario.sh scenarios/x.env` | `bin/run-scenario.sh` on the controller; fetches `results/<run-id>` even when the run failed. `DIAG`, `BENCH_IMAGE` and `RUN_ID` are forwarded. `SSM_TIMEOUT` (default 14400). |
| `bin/aws/verify-delivery.sh` | `bin/verify-delivery.sh` on the controller; `QOS`, `KAFKA_TOPIC` and `QOS2_RECONNECT_N` are forwarded. Prints PASS or FAIL; there is no results directory. |
| `bin/aws/zilla-image.sh <docker-save.tar.gz>` | Loads a locally built Zilla image onto the broker host through the bucket. |
| `bin/table-driver.sh <scenario>...` | Runs on the controller (not from your machine): a clean deploy and `bin/run-scenario.sh` for each scenario in turn, so the sequence survives your session. See below. |

### Testing a Zilla image built from a checkout

```bash
docker save <image:tag> | gzip > zilla-image.tar.gz      # linux/amd64, like the broker host
bin/aws/deploy.sh                                        # installs Docker on the hosts (once)
bin/aws/zilla-image.sh zilla-image.tar.gz                # prints "Loaded image: <image:tag>"
bin/aws/deploy.sh -e zilla_image=<image:tag> -e zilla_image_loaded=true
```

`zilla_image_loaded=true` skips the `docker pull`, so Ansible starts the image that is already on the host. The same
variables work in the SSH flow.

### Running several scenarios in a row

`bin/table-driver.sh` runs on the controller from `/opt/mqtt-benchmark-src` (after `bin/aws/push-bundle.sh`) and takes
scenario file stems from `scenarios/`. For each one it redeploys from scratch, runs `bin/run-scenario.sh` with `DIAG=1`,
samples per-worker CPU (`top -H`) and, for the `mqtt-kafka-100k` scenarios, a thread-dump profile
(`bin/agg-dumps.py`), reads the end offsets of the four topics, and uploads `results/<run>.tgz` and a progress log
(`status/driver.log`) to the bucket. It needs `BUCKET`; `DEPLOY_ARGS` goes to `ansible-playbook`, split on spaces, so put
anything with a space in a vars file:

```bash
bin/aws/ssm-run.py 'cd /opt/mqtt-benchmark-src && BUCKET=<bucket> DEPLOY_ARGS="-e @/tmp/vars.yml" setsid nohup bin/table-driver.sh mqtt-kafka-100k mqtt-kafka-100k-q2 > /dev/null 2>&1 < /dev/null &'
```

### Run recorder

With `DIAG=1` (which `bin/table-driver.sh` always sets), or `REC=1`, `bin/run-scenario.sh` records the whole run, from the
start of the connection ramp to the end, on the broker and Kafka hosts. Every step is best effort. It saves, in
`results/<run>/`:

| File | Contents |
|---|---|
| `events.txt` | Epoch milliseconds of `run_start`, `ramp_start`, `ramp_end`, `warmup_end`, `probe_start`, `t0`, `t1`, `probe_done` |
| `offsets-t0.txt`, `offsets-t1.txt` | End offset of every partition of `KAFKA_TOPIC` at T0 and T1 (`topic:partition:offset`) |
| `recorder-broker.tgz` | `bin/record-host.py`, run as root every second: per-thread CPU (container tid, `utime`, `stime`, last cpu, `minflt`, `majflt`, and for `engine/*` threads the context switches and `wchan`), `/proc/<pid>/io`, `meminfo` (`Dirty`, `Writeback`, `MemAvailable`), `/proc/vmstat` (`nr_dirty`, `nr_writeback`, `nr_dirtied`, `nr_written`, the dirty thresholds, page faults), the pressure stall files, `/proc/stat` per cpu (user, system, iowait, irq, softirq, steal), a per-process table for the kworker flush threads, `kswapd`, `jbd2`, `ksoftirqd`, `dockerd`, `containerd` and `java`, `diskstats` and `vmstat`, and `iostat -x 1`; the cpu topology and hyperthread siblings (`system.txt`) |
| `recorder-client-<k>.tgz` | Every emqtt-bench metrics page of client host `k`, every 5 s, with the epoch ms of each read (`metrics/series.txt`) |
| `run-meta.json` | The Zilla container's environment (JVM options) and image labels, broker host uptime and boot time, and the run's index since the broker host started |
| `recorder-kafka.tgz` | The same without threads, plus `offsets/offsets.tsv`: the end offset of every partition of every `mqtt-*` topic every `OFFSET_INTERVAL_S` (default 10) s |
| `ebs-cloudwatch-broker.json`, `ebs-cloudwatch-kafka.json` | `bin/ebs-cloudwatch.py`: per-minute EBS volume and instance EBS metrics (throughput, ops, queue length, idle time), when the controller may call `cloudwatch:GetMetricStatistics`; CloudWatch lags a minute or two, so the last minutes may be missing |
| `locations.txt`, `ebs-broker.json`, `ebs-kafka.json` | Where the Zilla cache (`/var/run/zilla` in the container, on the broker's root volume) and the Kafka log (`/data`) live, and each instance's volume type, IOPS and throughput when the controller may call `ec2:DescribeVolumes` |
| `puback-probe.json`, `puback-probe-progress.txt` | The probe summary (waited for up to 60 s after T1) and its counters every 5 s |

The kernel cuts thread names to 15 characters, so `top` shows `engine/worker#10` to `#19` all as `engine/worker#1`.
`bin/worker-cpu.py <run-dir>` names each worker by joining the recorded container tid to the `nid` of the thread dumps and
prints CPU% per worker over the window and the ramp, the cpus each ran on, and whether the dumps lie inside the window
(`worker-cpu.txt`, `worker-cpu-series.tsv` for the 5 s series). The 100K scenarios take 10 thread dumps 20 s into the ramp and
60 from t0+30 s (`dumps-ramp.txt.gz`, `dumps-window.txt.gz`).

Where Zilla's files live: the image's `zilla.properties` sets `zilla.engine.directory=/var/run/zilla`, so the stream rings
(`data<N>`, `buffers<N>`, `events<N>`, `metrics/`) are in `/var/run/zilla`, the engine cache directory is `/var/run/zilla/cache`
and the Kafka cache `/var/run/zilla/cache/kafka`, all on the container's overlay on the broker's root volume.
`-e zilla_engine_tmpfs_gib=<n>` (default 24) mounts a tmpfs at `/dev/shm/zilla-engine`, sets `zilla.engine.directory` there and
pins `zilla.engine.cache.directory` to `/var/run/zilla/cache`, so only the engine directory lives in RAM and the Kafka cache
stays on disk. It is the default because the rings and buffers were about a third of Zilla's disk writes at 100K msg/s
(3.6 KB against 2.4 KB written per 1 KB message) and, in 4 runs per arm on a 61 GiB host, the runs with them on tmpfs reached
91-100K rec/s with 8-9% iowait where the all-disk runs split between 92K and 70-73K with 39-42% iowait. Keep `n` at 21 or more
on a 61 GiB host or Zilla's memory sizing changes (the smaller of 25% of RAM and 75% of the engine directory's free space); a
tmpfs only uses what is touched (about 8-15 GB). Pass `-e zilla_engine_tmpfs_gib=0` to keep everything on the root volume.
The Kafka cache is not offered on tmpfs: it is memory mapped and unbounded (about 2.3 KB per 1 KB publish, 230 MB/s at
100K msg/s).

Scenario knobs for the shape of the load: `PUB_CONN_RATE` (connections per second, in total), `WAIT_BEFORE_PUBLISHING`
(`true` by default: each emqtt-bench instance starts publishing once all of its connections are up; `false`: each publisher
starts as soon as it is connected), and the offered rate `PUBLISHERS x 1000 / PUBLISH_INTERVAL_MS`.

## Tuning variables and diagnostics

Pass with `-e` to `deploy.sh` (or `ansible-playbook`):

- `mqtt_devices_partitions` (16), `mqtt_messages_partitions` (16), `mqtt_sessions_partitions` (1),
  `mqtt_retained_partitions` (1).
- `zilla_cache_bootstrap` (`false`): `true` adds the cache_server `bootstrap` block, which keeps fetching every partition of
  the listed topics into the cache even when nothing reads them. Leave it off for publish-only runs.
- `zilla_diagnostics=true`: Zilla starts with GC logging and a continuous Flight Recorder recording, both under
  `/var/log/zilla` on the broker host.

`DIAG=1 bin/aws/run-scenario.sh ...` (or `DIAG=1 bin/run-scenario.sh ...` in the SSH flow) saves to
`results/<run-id>/diagnostics/`: per-thread CPU of the broker process (`top -H`, sampled mid-window), a Zilla class
histogram just before and just after the measurement window (needs `jcmd` in the image; it forces a full GC, so it is
never taken inside the window), `ss -s` of the broker host, and, with `zilla_diagnostics=true`, `gc.log` and a JFR dump.
Every diagnostic is best effort: a failure is logged in `run.log` and does not change the run.

## What auto-terminates

The controller shuts itself down `max_lifetime_minutes` (default 300) after it boots, and so does every benchmark
host when `max_lifetime_minutes` in `deploy/terraform` is above 0 (the default 0 turns the guard off there, as in the
SSH flow). Shutdown terminates the instance (`instance_initiated_shutdown_behavior = "terminate"`); root and data
volumes go with it. This is a guard against forgotten environments: set the value above the longest run plus deploy
time, and remember that hosts created earlier stop earlier. If the controller is the one that goes first, you lose the
way to drive the hosts.

Not auto-terminated, and removed only by `terraform destroy`: the VPC, subnet, security group, placement group and key
pair, the bucket and its contents, the IAM role and instance profile. Instances that already terminated are fine for
`terraform destroy`.

## Known limits

- Output of a command arrives when it ends, not live (the client only prints "still running" once a minute). If the
  operator host loses its connection, the command keeps running on the controller and still uploads
  `s3://$BUCKET/results/<run-id>.tgz` at its end; fetch it with `bin/aws/s3cp.py get results/<run-id>.tgz out.tgz`.
- The scenario must be a file under `scenarios/`, since that is what the bundle carries. `results/` on the controller
  is not part of the bundle and disappears with it, so fetch what you need before tear-down.
- The private key is in the controller's user data and in `/home/ubuntu/.ssh`: anyone who may read the instance's user
  data or open a session on it can use the key. Use a key made for this benchmark. Command lines and their output end
  up in `s3://$BUCKET/ssm-output/`, so do not pass secrets as `-e` values.
- The controller has a public address, as the benchmark hosts do, so that the SSM agent can reach AWS: there are no VPC
  endpoints. It accepts no inbound traffic except from the benchmark security group (and `operator_cidrs`).
- Ansible on the controller is ansible-core 2.17 (the last release for Ubuntu 22.04's Python 3.10), not whatever the
  SSH flow's operator host has.
- SSM writes command output to the bucket with the instance role. If the output upload is denied, that role may also
  need `s3:PutObjectAcl`; the role created here has only the four actions listed above.
- A Zilla image archive must be for the broker's CPU architecture (amd64 unless `arch` was changed in `deploy/terraform`).
