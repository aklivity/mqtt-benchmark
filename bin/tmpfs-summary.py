#!/usr/bin/env python3
"""Summary of the tmpfs series the guard (bin/tmpfs-guard.py) recorded for a run.

Usage: tmpfs-summary.py <run-dir> [--series <name>]
Reads tmpfs-<name>.tsv (name: produce, engine) from <run-dir>/recorder-broker.tgz and the run's events.txt. For the measurement window
(t0..t1) it prints, per tmpfs: size, peak and mean used (GiB), the lowest usage in the last 60 s of the window (the sawtooth floor),
files, rolls per second (new file names a second, mean and peak), and whether the guard asked for the run to stop (the abort line).
"""
import os
import statistics
import sys
import tarfile

GIB = float(1 << 30)


def events(run):
    ev = {}
    path = os.path.join(run, 'events.txt')
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 2:
                ev[p[1]] = int(p[0])
    return ev


def series(run, name):
    rows = []
    with tarfile.open(os.path.join(run, 'recorder-broker.tgz')) as tar:
        for m in tar.getmembers():
            if m.name.endswith(f'tmpfs-{name}.tsv'):
                for line in tar.extractfile(m).read().decode().splitlines():
                    p = line.split('\t')
                    if len(p) == 5:
                        rows.append(tuple(int(x) for x in p))
    return rows


def log(run, name):
    with tarfile.open(os.path.join(run, 'recorder-broker.tgz')) as tar:
        for m in tar.getmembers():
            if m.name.endswith(f'tmpfs-{name}.log'):
                return tar.extractfile(m).read().decode().strip()
    return ''


def main():
    run = sys.argv[1]
    names = [sys.argv[sys.argv.index('--series') + 1]] if '--series' in sys.argv else ['produce', 'engine']
    ev = events(run)
    t0, t1 = ev.get('t0'), ev.get('t1')
    for name in names:
        rows = series(run, name)
        if not rows:
            print(f'{name}: no tmpfs series (that tmpfs was not mounted, or the guard did not start)')
            continue
        w = [r for r in rows if (t0 is None or r[0] >= t0) and (t1 is None or r[0] <= t1)] or rows
        tail = [r[1] for r in w if r[0] >= w[-1][0] - 60000]
        rolls = [r[4] for r in w[1:]]
        print(f'{name}: size {w[0][2] / GIB:.1f} GiB  used peak {max(r[1] for r in w) / GIB:.2f} GiB  mean '
              f'{statistics.mean(r[1] for r in w) / GIB:.2f} GiB  floor over the last 60 s {min(tail) / GIB:.2f} GiB  '
              f'files {w[-1][3]}  new files/s mean {statistics.mean(rolls) if rolls else 0:.1f} peak {max(rolls) if rolls else 0}')
        g = log(run, name)
        if g:
            print(f'{name} guard log:\n{g}')


main()
