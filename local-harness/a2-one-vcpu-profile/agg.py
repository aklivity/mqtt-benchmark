import sys,re,collections,subprocess
f=sys.argv[1]
out=subprocess.run(['jfr','print','--events','jdk.CPUTimeSample','--stack-depth','64',f],capture_output=True,text=True).stdout
samples=[]
for blk in out.split('jdk.CPUTimeSample')[1:]:
    fr=[]
    m=re.search(r'stackTrace = \[(.*?)\n\s*\]',blk,re.S)
    if not m: continue
    if 'engine/worker#0' not in blk: continue
    for l in m.group(1).strip().split('\n'):
        l=l.strip()
        if l.startswith('...'): continue
        fr.append(re.sub(r'\s+line:.*','',l))
    samples.append(fr)
n=len(samples); print('samples',n)
selfc=collections.Counter(s[0] for s in samples if s)
print('TOP 25 self'); 
for k,v in selfc.most_common(25): print('%5.1f%% %s'%(100*v/n,k))
def comp(fr):
    # classify by leaf-most meaningful frame, with inclusive for kafka cache
    for fn in fr:
        if 'binding.mqtt.kafka' in fn: return 'mqtt-kafka binding'
        if 'binding.mqtt.internal' in fn: return 'mqtt binding'
        if 'binding.kafka.internal.cache' in fn or 'KafkaCache' in fn: return 'kafka cache (server/client side)'
        if 'binding.kafka' in fn: return 'kafka binding (client/cache streams)'
        if 'binding.tcp' in fn: return 'tcp binding'
        if 'org.agrona' in fn: return 'agrona'
        if 'runtime.engine' in fn: return 'engine worker loop/ring buffers'
    return 'JDK/other'
def comp_self(fr):
    return comp(fr)
c=collections.Counter(comp(s) for s in samples)
print('COMPONENT (by leafmost zilla/agrona frame)')
for k,v in c.most_common(): print('%5.1f%% %s'%(100*v/n,k))
# inclusive by component
inc=collections.Counter()
for s in samples:
    seen=set()
    for fn in s:
        for key,name in [('binding.mqtt.kafka','mqtt-kafka'),('binding.mqtt.internal','mqtt'),('binding.kafka.internal.stream.KafkaCache','kafka cache'),('binding.kafka.internal.cache','kafka cache'),('binding.kafka.internal.stream.KafkaClient','kafka client'),('binding.tcp','tcp'),('runtime.engine','engine'),('org.agrona','agrona')]:
            if key in fn and name not in seen: seen.add(name); inc[name]+=1
print('INCLUSIVE'); 
for k,v in inc.most_common(): print('%5.1f%% %s'%(100*v/n,k))
# top frames inclusive (zilla methods)
incm=collections.Counter()
for s in samples:
    for fn in set(s):
        if 'aklivity' in fn: incm[fn]+=1
print('TOP inclusive zilla methods')
for k,v in incm.most_common(25): print('%5.1f%% %s'%(100*v/n,k))
