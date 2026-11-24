import os, subprocess, sys, time, shutil, signal
S=os.path.dirname(os.path.abspath(__file__))
jars=sys.argv[1]; timeout=int(sys.argv[2]); label=sys.argv[3]
run=S+"/run-"+label
shutil.rmtree(run, ignore_errors=True); os.makedirs(run)
shutil.copy(S+"/etc/zilla.yaml", run)
cmd=["taskset","-c","0","java","-Xmx3g","--enable-native-access=ALL-UNNAMED","--add-opens","java.base/jdk.internal.misc=ALL-UNNAMED",
 "--add-exports","java.base/jdk.internal.misc=org.agrona","--add-opens","java.base/jdk.internal.misc=org.agrona",
 "-p",jars,"-m","io.aklivity.zilla.runtime.command/io.aklivity.zilla.runtime.command.internal.ZillaMain","start","-c","zilla.yaml","-w","1"]
log=open(run+"/zilla.log","w")
p=subprocess.Popen(cmd,cwd=run,stdout=log,stderr=subprocess.STDOUT)
hz=os.sysconf("SC_CLK_TCK")
def port_open():
    import socket
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
t0=time.time(); tid=None; opened=None; base=0; hist=[]
while time.time()-t0<timeout+120:
    if p.poll() is not None: print("zilla exited",p.returncode); break
    if tid is None: tid=worker_tid()
    if opened is None and tid and port_open():
        opened=time.time(); base=ticks(tid); hist=[]; print("port open after %.1fs"%(opened-t0),flush=True)
    if opened:
        c=ticks(tid); now=time.time(); hist.append((now,c))
        el=now-opened
        while len(hist)>1 and now-hist[0][0]>10: hist.pop(0)
        recent=(hist[-1][1]-hist[0][1])/hz/max(hist[-1][0]-hist[0][0],1e-9)
        if int(el)%10==0 and abs(el-round(el))<0.6: print("t=%5.0fs worker cpu=%6.1fs recent=%.2f"%(el,(c-base)/hz,recent),flush=True)
        if el>20 and recent<0.05: print("IDLE: wall %.0fs, worker cpu after port open %.1fs"%(el,(c-base)/hz)); break
        if el>timeout: print("TIMEOUT %ds: worker cpu %.1fs recent=%.2f"%(el,(c-base)/hz,recent)); break
    time.sleep(1)
p.send_signal(signal.SIGTERM)
try: p.wait(20)
except: p.kill()
