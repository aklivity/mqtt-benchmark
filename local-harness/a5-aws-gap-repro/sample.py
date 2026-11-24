from kafka import KafkaConsumer
import collections,sys
c=KafkaConsumer('mqtt-sessions',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',consumer_timeout_ms=5000,enable_auto_commit=False)
kinds=collections.Counter(); ex={}
for i,m in enumerate(c):
    k=m.key.decode('latin1'); suf=k.split('#')[1] if '#' in k else 'base'
    hdr=tuple((a,b) for a,b in (m.headers or []))
    key=(suf,m.value is None,hdr)
    kinds[key]+=1
    ex.setdefault(key,(m.key,m.value,m.headers))
    if i>40000: break
for k,v in kinds.items(): print(k,v,len(ex[k][1] or b''))
import pickle; pickle.dump(ex,open(sys.argv[1],'wb'))
