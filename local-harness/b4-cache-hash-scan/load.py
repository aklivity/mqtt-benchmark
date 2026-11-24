import os, subprocess, sys, time, shutil, signal, socket, re
S=os.path.dirname(os.path.abspath(__file__))
jars=sys.argv[1]; label=sys.argv[2]; pubs=int(sys.argv[3]); window=int(sys.argv[4])
run=S+"/load-"+label
shutil.rmtree(run, ignore_errors=True); os.makedirs(run)
shutil.copy(S+"/etc/zilla.yaml", run)
cmd=["taskset","-c","0","java","-Xmx3g","--enable-native-access=ALL-UNNAMED","--add-opens","java.base/jdk.internal.misc=ALL-UNNAMED",
 "--add-exports","java.base/jdk.internal.misc=org.agrona","--add-opens","java.base/jdk.internal.misc=org.agrona",
 "-p",jars,"-m","io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain","start","-c","zilla.yaml","-w","1"]
log=open(run+"/zilla.log","w")
p=subprocess.Popen(cmd,cwd=run,stdout=log,stderr=subprocess.STDOUT)
hz=os.sysconf("SC_CLK_TCK")
def port_open():
    s=socket.socket(); s.settimeout(0.5)
    try: s.connect(('127.0.0.1',7183)); return True
    except Exception: return False
    finally: s.close()
def worker_tid():
    for t in os.listdir("/proc/%d/task"%p.pid):
        try:
            if open("/proc/%d/task/%s/comm"%(p.pid,t)).read().strip().startswith("engine/worker#0"): return t
        except: pass
def ticks(t):
    f=open("/proc/%d/task/%s/stat"%(p.pid,t)).read()
    f=f[f.rindex(")")+2:].split(); return int(f[11])+int(f[12])
def offset():
    o=subprocess.run("docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic mqtt-messages",shell=True,capture_output=True,text=True).stdout
    return int(o.strip().split(":")[-1])
t0=time.time(); tid=None
while tid is None or not port_open():
    tid=tid or worker_tid(); time.sleep(0.5)
    if time.time()-t0>120: raise SystemExit("no port")
print("port open %.1fs"%(time.time()-t0),flush=True)
hist=[]
while True:
    c=ticks(tid); now=time.time(); hist.append((now,c))
    while len(hist)>1 and now-hist[0][0]>10: hist.pop(0)
    recent=(hist[-1][1]-hist[0][1])/hz/max(hist[-1][0]-hist[0][0],1e-9)
    if now-t0>25 and recent<0.05: break
    if now-t0>1500: print("replay timeout"); break
    time.sleep(1)
print("replay idle after %.0fs of wall, worker cpu %.1fs"%(time.time()-t0,c/hz),flush=True)
subprocess.run("docker rm -f bench >/dev/null 2>&1",shell=True)
bench=subprocess.Popen(["docker","run","--rm","--name","bench","--network","host","--cpuset-cpus","2,3","emqx/emqtt-bench:0.4.19","pub","-h","127.0.0.1","-p","7183",
  "-c",str(pubs),"-s","1024","-I","1000","-C","true","-R","300","-q","1","-V","5","-t","bench/%i"],stdout=open(run+"/bench.log","w"),stderr=subprocess.STDOUT)
ramp=pubs/300.0+10
time.sleep(ramp); print("ramp done (%.0fs)"%ramp,flush=True)
jfr=run+"/window.jfr"
if not os.environ.get("NOJFR"): subprocess.run(["jcmd",str(p.pid),"JFR.start","name=w","filename="+jfr,"jdk.CPUTimeSample#enabled=true","jdk.CPUTimeSample#throttle=10ms"],capture_output=True)
c0=ticks(tid); o0=offset(); w0=time.time()
time.sleep(window)
c1=ticks(tid); o1=offset(); w1=time.time()
if not os.environ.get("NOJFR"): subprocess.run(["jcmd",str(p.pid),"JFR.dump","name=w","filename="+jfr],capture_output=True)
subprocess.run("docker rm -f bench >/dev/null 2>&1",shell=True)
cpu=(c1-c0)/hz; msgs=o1-o0; el=w1-w0
print("RESULT label=%s pubs=%d window=%.0fs worker_cpu=%.1fs (%.0f%%) msgs=%d (%.0f/s) us_per_msg=%.1f"%(label,pubs,el,cpu,100*cpu/el,msgs,msgs/el,1e6*cpu/max(msgs,1)),flush=True)
p.send_signal(signal.SIGTERM)
try: p.wait(20)
except: p.kill()
