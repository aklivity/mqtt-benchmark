import sys,glob,struct,collections
def varint(b,i):
    r=0;s=0
    while True:
        c=b[i];i+=1;r|=(c&0x7f)<<s;s+=7
        if not c&0x80:break
    return (r>>1)^-(r&1),i
def records(path):
    d=open(path,'rb').read();p=0
    while p+12<=len(d):
        base,blen=struct.unpack_from('>qi',d,p)
        if p+12+blen>len(d):break
        magic=d[p+16]; attrs=struct.unpack_from('>h',d,p+21)[0]
        assert magic==2 and attrs&7==0,(magic,attrs)
        n=struct.unpack_from('>i',d,p+57)[0];i=p+61
        for _ in range(n):
            ln,i=varint(d,i);end=i+ln;i+=1
            _,i=varint(d,i);od,i=varint(d,i)
            kl,i=varint(d,i);key=d[i:i+kl] if kl>0 else b'';i+=max(kl,0)
            vl,i=varint(d,i);val=d[i:i+vl] if vl>=0 else b'';i+=max(vl,0)
            hc,i=varint(d,i);hs=[]
            for _ in range(hc):
                a,i=varint(d,i);hk=d[i:i+a];i+=a;b,i=varint(d,i);hv=d[i:i+b];i+=b;hs.append((hk,hv))
            yield base+od,key,val,hs;i=end
        p+=12+blen
if __name__=="__main__":
    recs=[r for f in sorted(glob.glob(sys.argv[1]+"/*.log")) for r in records(f)]
    off=len(recs); cnt=collections.Counter()
    for o,k,v,h in recs: cnt[(k,v[:8])]+=1
    dup=sum(c-1 for c in cnt.values() if c>1)
    print("records=%d distinct(key,first8)=%d duplicates=%d"%(off,len(cnt),dup))
    ex=[(o,k,v[:8].hex(),[(a.decode('latin1'),b.hex()[:24]) for a,b in h][:6]) for o,k,v,h in recs[:3]]
    print("sample:",ex)
    dd=[(k,x) for (k,x),c in cnt.items() if c>1][:5]; print("dup examples:",dd)
    # offsets of dups
    first={};dups=[]
    for o,k,v,h in recs:
        kk=(k,v[:8])
        if kk in first: dups.append((first[kk],o))
        else: first[kk]=o
    print("first dup offset pairs:",dups[:10], "dup offset range:", (dups[0][1],dups[-1][1]) if dups else None, "total records", off)
