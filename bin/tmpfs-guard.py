#!/usr/bin/env python3
"""Watches a tmpfs directory on the Zilla host and asks for the run to stop before the tmpfs fills up.

Usage: tmpfs-guard.py <mount-or-dir> <outdir> [--name produce] [--warn-gib 10] [--abort-gib 12]
                      [--floor-window-s 60] [--floor-rise-s 30] [--floor-rise-mib 256] [--warmup-s 90] [--interval 1]
Every --interval seconds it writes to <outdir>/tmpfs-<name>.tsv: epoch ms, bytes used on the file system, its size, the number
of files below <dir>, and how many file names appeared since the previous sample (a segment roll creates a new file, so this
is the roll rate). It stops when <outdir>/stop exists, then writes <outdir>/tmpfs-<name>.done.

The decision is made by Guard (importable, see bin/test_tmpfs_guard.py):
  warn    used > --warn-gib
  abort   used > --abort-gib, or the sawtooth floor has been rising for --floor-rise-s seconds.
The floor at time t is the lowest usage in the trailing --floor-window-s seconds (a segment-per-log sawtooth fills and drops
within one window, so its floor is the memory that is not being reclaimed). The floor is rising when it is above the floor
--floor-rise-s seconds ago by more than --floor-rise-mib, and has not dropped in between. It is not evaluated during the first
--warmup-s seconds, while the first segments fill. The thresholds are provisional: they come from arithmetic, not from a load test.
On abort it writes the reason to <outdir>/abort (the run driver polls for that file) and keeps recording.
"""
import argparse
import collections
import os
import sys
import time

GIB = 1 << 30
MIB = 1 << 20


class Guard:
    def __init__(self, warn_bytes, abort_bytes, floor_window_s=60, floor_rise_s=30, floor_rise_bytes=256 * MIB, warmup_s=90):
        self.warn_bytes = warn_bytes
        self.abort_bytes = abort_bytes
        self.floor_window_s = floor_window_s
        self.floor_rise_s = floor_rise_s
        self.floor_rise_bytes = floor_rise_bytes
        self.warmup_s = warmup_s
        self.start = None
        self.samples = collections.deque()      # (t_s, used)
        self.floors = collections.deque()       # (t_s, floor)

    def feed(self, t_s, used):
        """Returns ('ok' | 'warn' | 'abort', reason)."""
        if self.start is None:
            self.start = t_s
        self.samples.append((t_s, used))
        while self.samples[0][0] < t_s - self.floor_window_s:
            self.samples.popleft()
        status, reason = 'ok', ''
        if used > self.abort_bytes:
            status, reason = 'abort', f'used {used / GIB:.2f} GiB above {self.abort_bytes / GIB:.2f} GiB'
        elif used > self.warn_bytes:
            status, reason = 'warn', f'used {used / GIB:.2f} GiB above {self.warn_bytes / GIB:.2f} GiB'
        if t_s - self.start >= self.warmup_s and t_s - self.samples[0][0] >= self.floor_window_s * 0.9:
            floor = min(u for _, u in self.samples)
            self.floors.append((t_s, floor))
            while self.floors[0][0] < t_s - self.floor_rise_s:
                self.floors.popleft()
            span = t_s - self.floors[0][0]
            values = [f for _, f in self.floors]
            rising = (span >= self.floor_rise_s * 0.9 and values[-1] - values[0] > self.floor_rise_bytes
                      and all(b >= a for a, b in zip(values, values[1:])))
            if rising and status != 'abort':
                status = 'abort'
                reason = (f'floor rose {(values[-1] - values[0]) / MIB:.0f} MiB in {span:.0f} s '
                          f'to {values[-1] / GIB:.2f} GiB (acks lagging)')
        return status, reason


def usage(path):
    st = os.statvfs(path)
    return (st.f_blocks - st.f_bfree) * st.f_frsize, st.f_blocks * st.f_frsize


def names_below(path):
    found = set()
    for root, _, files in os.walk(path):
        for f in files:
            found.add(os.path.join(root, f))
    return found


def main():
    p = argparse.ArgumentParser()
    p.add_argument('path')
    p.add_argument('outdir')
    p.add_argument('--name', default='produce')
    p.add_argument('--warn-gib', type=float, default=10.0)
    p.add_argument('--abort-gib', type=float, default=12.0)
    p.add_argument('--floor-window-s', type=float, default=60.0)
    p.add_argument('--floor-rise-s', type=float, default=30.0)
    p.add_argument('--floor-rise-mib', type=float, default=256.0)
    p.add_argument('--warmup-s', type=float, default=90.0)
    p.add_argument('--interval', type=float, default=1.0)
    a = p.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    guard = Guard(a.warn_gib * GIB, a.abort_gib * GIB, a.floor_window_s, a.floor_rise_s, a.floor_rise_mib * MIB, a.warmup_s)
    series = open(os.path.join(a.outdir, f'tmpfs-{a.name}.tsv'), 'a', buffering=1)
    log = open(os.path.join(a.outdir, f'tmpfs-{a.name}.log'), 'a', buffering=1)
    stop = os.path.join(a.outdir, 'stop')
    abort = os.path.join(a.outdir, 'abort')
    known, last_status = set(), 'ok'
    while not os.path.exists(stop):
        started = time.time()
        try:
            used, size = usage(a.path)
            names = names_below(a.path)
            series.write(f'{int(started * 1000)}\t{used}\t{size}\t{len(names)}\t{len(names - known)}\n')
            known = names
            status, reason = guard.feed(started, used)
            if status != last_status:
                log.write(f'{int(started * 1000)} {status} {reason}\n')
                last_status = status
            if status == 'abort' and not os.path.exists(abort):
                with open(abort, 'w') as f:
                    f.write(f'{int(started * 1000)} {a.name} {reason}\n')
        except OSError as e:
            log.write(f'{int(started * 1000)} error {e}\n')
        time.sleep(max(0.0, a.interval - (time.time() - started)))
    open(os.path.join(a.outdir, f'tmpfs-{a.name}.done'), 'w').write(f'{int(time.time() * 1000)}\n')


if __name__ == '__main__':
    main()
