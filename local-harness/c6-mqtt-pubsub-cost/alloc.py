import sys,re,collections,subprocess
f=sys.argv[1]; depth=sys.argv[2] if len(sys.argv)>2 else "3"
out=subprocess.run(["jfr","print","--events","jdk.ObjectAllocationSample","--stack-depth",depth,f],capture_output=True,text=True,env={"PATH":"/usr/bin:/usr/local/bin","JAVA_TOOL_OPTIONS":"","_JAVA_OPTIONS":""}).stdout
tot=collections.Counter(); byc=collections.Counter(); W=0
for ev in out.split("jdk.ObjectAllocationSample")[1:]:
    if 'eventThread = "engine/worker#0"' not in ev: continue
    m=re.search(r'objectClass = (\S+)',ev); w=re.search(r'weight = ([\d.]+) (\w+)',ev)
    if not (m and w): continue
    v=float(w.group(1))*{"bytes":1,"kB":1e3,"MB":1e6,"GB":1e9}.get(w.group(2),1)
    fr=re.findall(r'^\s+([\w.$<>]+)\(',ev.split("stackTrace = [")[1],re.M)
    top=" <- ".join(x.split('.')[-2]+'.'+x.split('.')[-1] for x in fr[:int(depth)])
    tot[top]+=v; byc[m.group(1)]+=v; W+=v
print("worker sampled alloc total MB:",W/1e6)
print("-- by class"); [print("%6.1f%% %s"%(100*v/W,k)) for k,v in byc.most_common(8)]
print("-- by site");  [print("%6.1f%% %s"%(100*v/W,k)) for k,v in tot.most_common(12)]
