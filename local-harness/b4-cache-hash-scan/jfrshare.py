import subprocess,sys,re
f=sys.argv[1]
out=subprocess.run(["jfr","print","--events","jdk.CPUTimeSample","--stack-depth","64",f],capture_output=True,text=True).stdout
evs=out.split("jdk.CPUTimeSample")
tot=w=idx=anc=0
for e in evs:
    if "eventThread" not in e: continue
    if "engine/worker#0" not in e: continue
    tot+=1
    if re.search(r"SortedByValue\.(last|lower)\(",e): idx+=1
    if "findAndMarkAncestor" in e: anc+=1
print("%s worker samples=%d SortedByValue.last/lower=%d (%.2f%%) findAndMarkAncestor=%d (%.2f%%)"%(f.split('/')[-2],tot,idx,100*idx/max(tot,1),anc,100*anc/max(tot,1)))
