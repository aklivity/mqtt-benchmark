#!/usr/bin/env python3
"""Per-worker CPU of a run, from the host recorder's per-thread samples (threads.tsv in recorder-broker.tgz).

Usage: worker-cpu.py <run-dir> [--series <file>]
Reads <run-dir>: recorder-broker.tgz, events.txt and the thread dumps dumps-ramp.txt.gz / dumps-window.txt.gz (or .txt).

The kernel cuts a thread's name to 15 characters, so engine/worker#10 .. #19 all read engine/worker#1 in top and in
/proc. A worker is named here by joining the container-side tid recorded for each sampled thread (NSpid) with the nid
that a JVM thread dump prints for "engine/worker#N". CPU% is the utime+stime delta over the wall-clock delta between
the first and last sample inside the interval (100 ticks per second), so 100 is one full core.

Prints, per worker: cpu% over the measurement window (t0..t1) and over the connection ramp (ramp_start..ramp_end), the
number of distinct cpus the thread ran on and the cpu it ran on most; then the mean/min/max of the groups #0-#15 and
#16-#31; then whether the thread dumps were taken inside the window. --series writes the cpu% of every worker for
every sampling interval (epoch ms, worker, cpu%, cpu).
"""
import collections
import gzip
import io
import os
import re
import statistics
import sys
import tarfile

TICKS = 100.0
HEADER = re.compile(r'"engine/worker#(\d+)".*?nid=(0x[0-9a-fA-F]+|\d+)')
STAMP = re.compile(r'^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.?(\d*)Z ')


def read_text(path):
    for p in (path + '.gz', path):
        if os.path.exists(p):
            return (gzip.open(p, 'rt', errors='replace') if p.endswith('.gz') else open(p, errors='replace')).read()
    return ''


def events(run):
    ev = {}
    path = os.path.join(run, 'events.txt')
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 2:
                ev[p[1]] = int(p[0])
    return ev


def nid_to_worker(dump_text):
    m = {}
    for line in dump_text.splitlines():
        h = HEADER.search(STAMP.sub('', line))
        if h:
            m[int(h.group(2), 0)] = int(h.group(1))
    return m


def dump_times(dump_text):
    import calendar
    out = []
    for line in dump_text.splitlines():
        s = STAMP.match(line)
        if s and 'Full thread dump' in line:
            y, mo, d, h, mi, sec, frac = s.groups()
            out.append(calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(sec))) * 1000 + int((frac + '000')[:3]))
    return out


def load_threads(run):
    path = os.path.join(run, 'recorder-broker.tgz')
    with tarfile.open(path) as tar:
        member = next(m for m in tar.getmembers() if m.name.endswith('threads.tsv'))
        return tar.extractfile(member).read().decode(errors='replace')


def main():
    run = sys.argv[1]
    series = sys.argv[sys.argv.index('--series') + 1] if '--series' in sys.argv else None
    ev = events(run)
    dumps = {k: read_text(os.path.join(run, f'dumps-{k}.txt')) for k in ('ramp', 'window')}
    n2w = nid_to_worker(dumps['window'] + dumps['ramp'])
    rows = collections.defaultdict(list)    # worker -> [(t_ms, ticks, cpu)]
    for line in load_threads(run).splitlines():
        p = line.split('\t')
        if len(p) >= 8 and p[2].isdigit() and int(p[2]) in n2w:
            rows[n2w[int(p[2])]].append((int(p[0]), int(p[5]) + int(p[6]), int(p[7])))
    if not rows:
        print('no engine/worker threads matched: need threads.tsv and a thread dump with the same container tids')
        return

    def pct(w, a, b):
        r = [x for x in rows[w] if (a is None or x[0] >= a) and (b is None or x[0] <= b)]
        return 100.0 * (r[-1][1] - r[0][1]) / TICKS / ((r[-1][0] - r[0][0]) / 1000.0) if len(r) > 1 and r[-1][0] > r[0][0] else None

    win = (ev.get('t0'), ev.get('t1'))
    ramp = (ev.get('ramp_start'), ev.get('ramp_end'))
    def cell(v, width):
        return f'{v:{width}.0f}' if v is not None else f'{"-":>{width}}'

    print(f'{"worker":>8} {"window%":>8} {"ramp%":>7} {"cpus":>5} {"main cpu":>8}')
    table = {}
    for w in sorted(rows):
        cpus = collections.Counter(x[2] for x in rows[w] if win[0] is None or win[0] <= x[0] <= win[1])
        table[w] = (pct(w, *win), pct(w, *ramp) if None not in ramp else None)
        print(f'{"#" + str(w):>8} {cell(table[w][0], 8)} {cell(table[w][1], 7)} {len(cpus):>5} '
              f'{cpus.most_common(1)[0][0] if cpus else "-":>8}')
    for name, ws in (('#0-#15', range(16)), ('#16-#31', range(16, 32))):
        v = [table[w][0] for w in ws if w in table and table[w][0] is not None]
        if v:
            print(f'{name}: window cpu% mean {statistics.mean(v):.0f} min {min(v):.0f} max {max(v):.0f}')
    for k in ('ramp', 'window'):
        t = dump_times(dumps[k])
        if t and win[0] is not None:
            inside = win[0] <= t[0] and t[-1] <= win[1]
            print(f'dumps-{k}: {len(t)} dumps, first at {(t[0] - win[0]) / 1000:+.0f} s, last at {(t[-1] - win[0]) / 1000:+.0f} s '
                  f'relative to t0; window is 0..{(win[1] - win[0]) / 1000:.0f} s' + ('; inside the window' if inside else '; NOT inside the window'))
        elif k == 'window':
            print('dumps-window: none found')
    if series:
        with open(series, 'w') as f:
            f.write('t_ms\tworker\tcpu_pct\tcpu\n')
            for w in sorted(rows):
                for a, b in zip(rows[w], rows[w][1:]):
                    if b[0] > a[0]:
                        f.write(f'{b[0]}\t{w}\t{100.0 * (b[1] - a[1]) / TICKS / ((b[0] - a[0]) / 1000.0):.1f}\t{b[2]}\n')


main()
