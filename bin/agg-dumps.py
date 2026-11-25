# Aggregate JVM thread dumps (many "Full thread dump" sections): per engine worker busy share, top frames, inclusive counts.
import re, sys, collections
path, top_n = sys.argv[1], int(sys.argv[2])
IDLE = ('sun.nio.ch.EPoll.wait', 'sun.nio.ch.EPollSelectorImpl.doSelect')
SKIP = ('AgentRunner.', 'Thread.run', 'Thread.runWith', 'EngineWorker.doWork', 'ManyToOneRingBuffer.readEx', 'EngineWorker.handleRead',
        'EngineWorker$$Lambda', 'Poller.doWork', 'SelectorImpl.')
def norm(line):
    m = re.match(r'\s*at (.*?)\(', line)
    if not m: return None
    s = m.group(1)
    s = re.sub(r'^[^/]*/', '', s) if '/' in s.split('(')[0] and re.match(r'^[^/]*/[A-Za-z]', s) else s
    s = s.replace('io.aklivity.zilla.runtime.', '').replace('binding.kafka.internal.', 'kafka.').replace('binding.mqtt.internal.', 'mqtt.')
    s = s.replace('java.base@25.0.5.1/', '')
    return re.sub(r'\$\$Lambda/0x[0-9a-f]+', '$$Lambda', s)
samples = collections.defaultdict(list)   # worker -> list of frame lists
n_dumps = 0
cur = None; frames = None
STAMP = re.compile(r'^\d{4}-\d\d-\d\dT[\d:.]+Z ')
for line in open(path, errors='replace'):
    line = STAMP.sub('', line)
    if line.startswith('Full thread dump'):
        n_dumps += 1
    m = re.match(r'"(engine/worker#\d+)"', line)
    if m:
        if cur is not None: samples[cur].append(frames)
        cur, frames = m.group(1), []
        continue
    if cur is not None:
        if line.startswith('\tat '):
            f = norm(line)
            if f: frames.append(f)
        elif line.strip() == '':
            samples[cur].append(frames); cur = None
def busy(fr): return bool(fr) and fr[0] not in IDLE and not any(f in IDLE for f in fr[:3])
rows = []
for w, ss in samples.items():
    b = sum(1 for s in ss if busy(s)); rows.append((int(w.split('#')[1]), w, len(ss), b))
rows.sort()
print(f'dumps={n_dumps}  workers={len(rows)}')
print('worker busy-share (samples not parked in epoll.wait):')
print(' '.join(f'#{i}:{100*b//max(n,1)}%' for i, w, n, b in rows))
def report(w):
    ss = [s for s in samples[w] if busy(s)]
    top = collections.Counter(s[0] for s in ss)
    inc = collections.Counter(); 
    for s in ss:
        for f in set(s):
            if not any(k in f for k in SKIP): inc[f] += 1
    print(f'--- {w}: busy samples {len(ss)} of {len(samples[w])}')
    print('  top frames:')
    for f, c in top.most_common(top_n): print(f'   {100*c//max(len(ss),1):3d}% {f}')
    print('  inclusive (share of busy samples):')
    for f, c in inc.most_common(top_n): print(f'   {100*c//max(len(ss),1):3d}% {f}')
hot = [w for i, w, n, b in rows if b * 2 > n]
idle = min(rows, key=lambda r: r[3])[1] if rows else None
for w in hot[:3]: report(w)
if idle and idle not in hot: report(idle)
