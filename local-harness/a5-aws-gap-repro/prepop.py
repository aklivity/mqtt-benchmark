# usage: prepop.py <samples.pkl> <clients> [compacted]  -> produces realistic session records for <clients> synthetic client ids
import pickle,sys
from kafka import KafkaProducer
ex=pickle.load(open(sys.argv[1],'rb')); n=int(sys.argv[2]); compacted=len(sys.argv)>3
get=lambda suf,tomb: next(v for k,v in ex.items() if k[0]==suf and k[1]==tomb)
off=get('offsets',False); exp=get('expiry',False); expt=get('expiry',True); bt=get('base',True); ot=get('offsets',True)
sid=exp[0].decode().split('#')[0]; L=len(sid)
p=KafkaProducer(bootstrap_servers='localhost:9092',linger_ms=50,batch_size=262144,acks=1,compression_type=None)
H=[('type',b'expiry-signal')]
for i in range(n):
    cid=('prepop_%d_' % i).ljust(L,'x')[:L]
    kb=cid.encode()
    ev=exp[1].replace(sid.encode(),kb)
    if not compacted:
        p.send('mqtt-sessions',key=kb,value=None)
        p.send('mqtt-sessions',key=kb+b'#offsets',value=None)
    p.send('mqtt-sessions',key=kb+b'#offsets',value=off[1])
    if not compacted: p.send('mqtt-sessions',key=kb+b'#expiry',value=None,headers=H)
    p.send('mqtt-sessions',key=kb+b'#expiry',value=ev,headers=H)
p.flush()
