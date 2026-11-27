#!/usr/bin/env python3
"""Host recorder: runs on the broker and Kafka hosts for the whole of a run and writes plain files to <outdir>.

Usage: record-host.py <outdir> [--interval 1] [--process java] [--threads]   (run as root: /proc/<pid>/io and wchan need it)
Stops when <outdir>/stop exists (or on SIGTERM), then writes <outdir>/done.

Written once, at the start (system.txt):
  lscpu -e, every cpu's thread_siblings_list (hyperthread pairs), lsblk with serials (the EBS volume id), mounts
Written every --interval seconds (default 1), in tab-separated files with the epoch milliseconds in the first column:
  procio.tsv      /proc/<pid>/io of the first process called --process (rchar wchar read_bytes write_bytes ...)
  procmem.tsv     /proc/<pid>/status of that process: VmRSS, VmHWM (peak RSS), RssAnon, RssShmem (kB) and Threads
  procmaps.tsv    /proc/<pid>/maps of that process: file-backed mappings, mappings and files that are deleted but still mapped, and the mapped
                  MiB and the distinct files of the produce staging directory, the engine directory, the fetch cache and everything else
  threads.tsv     with --threads: one row per thread of that process: tid, nspid (the tid inside its container, which is
                  what a JVM thread dump prints as nid), comm, state, utime, stime (clock ticks), processor (last cpu),
                  minflt, majflt, and for engine/* threads also the voluntary and involuntary context switches and wchan
Written every second:
  meminfo.tsv     MemAvailable, Dirty, Writeback, WritebackTmp, Mapped, Cached, Shmem (kB)
  vmstat-proc.tsv nr_dirty nr_writeback nr_dirtied nr_written, the two dirty thresholds (pages), pgfault pgmajfault, pgscan_*
  pressure.tsv    /proc/pressure/io, memory and cpu (PSI) when the kernel has them
  cpustat.tsv     /proc/stat cpu lines (total and per cpu): user nice system idle iowait irq softirq steal, in clock ticks
  procs.tsv       pid, comm, utime, stime of every process or kernel thread whose name matches kworker, kswapd, jbd2,
                  ksoftirqd, containerd, dockerd, java, snapd, amazon-ssm (the kworker flush threads are the writeback)
  diskstats.tsv   /proc/diskstats lines of the whole disks (utilisation, await and throughput come from deltas)
  vmstat.txt      `vmstat -t 1`
  iostat.txt      `iostat -x -t 1`, when sysstat is installed

comm is cut to 15 characters by the kernel (engine/worker#10 and engine/worker#19 are both engine/worker#1), so a thread
is named by joining nspid with the nid of a thread dump; see bin/worker-cpu.py.
"""
import argparse
import os
import signal
import subprocess
import sys
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument('outdir')
parser.add_argument('--interval', type=float, default=1.0)
parser.add_argument('--process', default='java')
parser.add_argument('--threads', action='store_true')
args = parser.parse_args()
os.makedirs(args.outdir, exist_ok=True)
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())


def out(name):
    return open(os.path.join(args.outdir, name), 'a', buffering=1)


def now_ms():
    return int(time.time() * 1000)


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout


def system():
    with out('system.txt') as f:
        f.write(f'# started {now_ms()}\n# lscpu -e\n{sh("lscpu -e")}\n# thread_siblings_list per cpu\n')
        f.write(sh('for c in /sys/devices/system/cpu/cpu[0-9]*; do echo "$(basename $c) $(cat $c/topology/thread_siblings_list)"; done'))
        f.write(f'\n# lsblk\n{sh("lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,SERIAL,MODEL")}\n# mounts\n{sh("findmnt -rn -o TARGET,SOURCE,FSTYPE")}\n')
        f.write(f'# df\n{sh("df -h / /data /var/lib/docker /var/run/zilla 2>/dev/null")}\n')
        f.write(f'# uname\n{sh("uname -a")}# nproc {sh("nproc")}')
        f.write(f'# vm settings\n{sh("sysctl vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs vm.dirty_writeback_centisecs vm.dirty_bytes vm.dirty_background_bytes vm.swappiness 2>&1")}')


def find_pid():
    pid = sh(f'pgrep -x {args.process} | head -1').strip()
    return int(pid) if pid else None


def whole_disk(name):
    return not name.startswith(('loop', 'ram', 'dm-')) and os.path.exists(f'/sys/block/{name}')


MEM_KEYS = ('MemAvailable', 'Dirty', 'Writeback', 'WritebackTmp', 'Mapped', 'Cached', 'Shmem')
VM_KEYS = ('nr_dirty', 'nr_writeback', 'nr_dirtied', 'nr_written', 'nr_dirty_threshold', 'nr_dirty_background_threshold',
           'pgfault', 'pgmajfault', 'pgscan_direct', 'pgscan_kswapd', 'pgsteal_direct', 'pgsteal_kswapd', 'allocstall_normal')


def key_values(path, keys, sep):
    v = {}
    try:
        for line in open(path):
            k, _, rest = line.partition(sep)
            if k in keys:
                v[k] = rest.split()[0]
    except OSError:
        pass
    return v


PROC_NAMES = ('kworker', 'kswapd', 'jbd2', 'ksoftirqd', 'containerd', 'dockerd', 'java', 'snapd', 'amazon-ssm')


def sample_procs(f, t):
    for pid in os.listdir('/proc'):
        if pid.isdigit():
            try:
                raw = open(f'/proc/{pid}/stat').read()
                comm = raw[raw.index('(') + 1:raw.rindex(')')]
                if comm.startswith(PROC_NAMES):
                    rest = raw[raw.rindex(')') + 2:].split()
                    f.write(f'{t}\t{pid}\t{comm}\t{rest[11]}\t{rest[12]}\n')
            except (OSError, ValueError, IndexError):
                continue


def per_second():
    ds, mem, vm, psi = out('diskstats.tsv'), out('meminfo.tsv'), out('vmstat-proc.tsv'), out('pressure.tsv')
    cpus, procs = out('cpustat.tsv'), out('procs.tsv')
    while not stop.is_set():
        t = now_ms()
        for line in open('/proc/diskstats'):
            p = line.split()
            if len(p) > 13 and whole_disk(p[2]):
                ds.write(f'{t}\t{line.strip()}\n')
        for line in open('/proc/stat'):
            if line.startswith('cpu'):
                cpus.write(f'{t}\t' + '\t'.join(line.split()[:9]) + '\n')
        sample_procs(procs, t)
        m = key_values('/proc/meminfo', MEM_KEYS, ':')
        mem.write(f'{t}\t' + '\t'.join(f'{k}={m.get(k, "")}' for k in MEM_KEYS) + '\n')
        v = key_values('/proc/vmstat', VM_KEYS, ' ')
        vm.write(f'{t}\t' + '\t'.join(f'{k}={v.get(k, "")}' for k in VM_KEYS) + '\n')
        for name in ('io', 'memory', 'cpu'):
            try:
                psi.write(f'{t}\t{name}\t' + open(f'/proc/pressure/{name}').read().strip().replace('\n', ' | ') + '\n')
            except OSError:
                pass
        stop.wait(max(0.0, 1.0 - (now_ms() - t) / 1000.0))


def spawn(cmd, name):
    try:
        return subprocess.Popen(cmd, stdout=out(name), stderr=subprocess.DEVNULL)
    except OSError:
        return None


def sample_threads(pid, f, nspid):
    t = now_ms()
    for tid in os.listdir(f'/proc/{pid}/task'):
        try:
            raw = open(f'/proc/{pid}/task/{tid}/stat').read()
            if tid not in nspid:
                for line in open(f'/proc/{pid}/task/{tid}/status'):
                    if line.startswith('NSpid:'):
                        nspid[tid] = line.split()[-1]
            # comm sits between the first '(' and the last ')'; the fields after it start at the state
            comm = raw[raw.index('(') + 1:raw.rindex(')')]
            rest = raw[raw.rindex(')') + 2:].split()
            row = f'{t}\t{tid}\t{nspid.get(tid, tid)}\t{comm}\t{rest[0]}\t{rest[11]}\t{rest[12]}\t{rest[36]}\t{rest[7]}\t{rest[9]}'
            if comm.startswith('engine/'):
                vol = nonvol = ''
                for line in open(f'/proc/{pid}/task/{tid}/status'):
                    if line.startswith('voluntary_ctxt_switches'):
                        vol = line.split()[1]
                    elif line.startswith('nonvoluntary_ctxt_switches'):
                        nonvol = line.split()[1]
                try:
                    wchan = open(f'/proc/{pid}/task/{tid}/wchan').read().strip()
                except OSError:
                    wchan = ''
                row += f'\t{vol}\t{nonvol}\t{wchan}'
            f.write(row + '\n')
        except (OSError, ValueError, IndexError):
            continue   # the thread ended between listing and reading


MAP_CLASSES = (('produce', '/dev/shm/zilla-produce'), ('engine', '/dev/shm/zilla-engine'), ('cache', '/var/run/zilla/cache'))


def map_stats(lines):
    """One row for procmaps.tsv from the lines of /proc/<pid>/maps: total mappings, mappings and files of deleted files, and per
    class (produce staging, engine, fetch cache, other) the mapped MiB and the number of distinct files."""
    classes = {name: [0, set()] for name, _ in MAP_CLASSES}
    classes['other'] = [0, set()]
    total = deleted = 0
    deleted_files = set()
    for line in lines:
        parts = line.split(None, 5)
        if len(parts) < 6 or not parts[5].startswith('/'):
            continue
        path = parts[5].strip()
        lo, hi = parts[0].split('-')
        size = int(hi, 16) - int(lo, 16)
        total += 1
        if path.endswith('(deleted)'):
            deleted += 1
            deleted_files.add(path)
        name = next((n for n, prefix in MAP_CLASSES if prefix in path), 'other')
        classes[name][0] += size
        classes[name][1].add(path)
    cols = [f'mappings={total}', f'deleted_mappings={deleted}', f'deleted_files={len(deleted_files)}']
    for name, (size, files) in classes.items():
        cols.append(f'{name}_mib={size >> 20}')
        cols.append(f'{name}_files={len(files)}')
    return cols


def main():
    system()
    threading.Thread(target=per_second, daemon=True).start()
    children = [spawn(['vmstat', '-t', '1'], 'vmstat.txt'), spawn(['iostat', '-x', '-t', '1'], 'iostat.txt')]
    io, thr, memory, maps = out('procio.tsv'), out('threads.tsv'), out('procmem.tsv'), out('procmaps.tsv')
    nspid = {}
    stop_file = os.path.join(args.outdir, 'stop')
    while not stop.is_set() and not os.path.exists(stop_file):
        started = time.time()
        pid = find_pid()
        if pid:
            try:
                io.write(f'{now_ms()}\t{pid}\t' + '\t'.join(l.strip().replace(': ', '=') for l in open(f'/proc/{pid}/io')) + '\n')
            except OSError:
                pass
            try:
                maps.write(f'{now_ms()}\t{pid}\t' + '\t'.join(map_stats(open(f'/proc/{pid}/maps'))) + '\n')
            except (OSError, ValueError):
                pass
            memory.write(f'{now_ms()}\t{pid}\t' + '\t'.join(
                f'{k}={v}' for k, v in key_values(f'/proc/{pid}/status', ('VmRSS', 'VmHWM', 'RssAnon', 'RssShmem', 'Threads'), ':').items()) + '\n')
            if args.threads:
                sample_threads(pid, thr, nspid)
        stop.wait(max(0.0, args.interval - (time.time() - started)))
    stop.set()
    for c in children:
        if c:
            c.terminate()
    open(os.path.join(args.outdir, 'done'), 'w').write(f'{now_ms()}\n')


main()
