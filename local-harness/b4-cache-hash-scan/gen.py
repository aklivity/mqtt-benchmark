import sys
n_ids=int(sys.argv[1])
val="x"*96
out=sys.stdout
for i in range(n_ids):
    cid="client-%07d"%i
    out.write("%s:%s\n"%(cid,val))
    out.write("%s#offsets:%s\n"%(cid,val))
    out.write("%s#expiry:%s\n"%(cid,val))
    out.write("%s#expiry:NULL\n"%cid)
    out.write("%s:%s\n"%(cid,val+"y"))
