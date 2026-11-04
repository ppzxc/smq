# 실험(#149) 집계: S1 3바퀴 중앙값, randomBench walMax, [exp-hold] lock 보유 시간, 채택 기준 1~4.
import json,glob,os,re,statistics as st,sys
R=sys.argv[1]
MODES=("manual","auto-2048","auto-3072")
def load(mode):
    out={}
    for r in (1,2,3):
        f=f"{R}/fixed-{mode}-r{r}-S1.json"
        if os.path.exists(f):
            for k,v in json.load(open(f))["entries"].items(): out.setdefault(k,[]).append(v)
    return out
d={m:load(m) for m in MODES}
fmt=lambda xs:'/'.join(f'{x:.0f}' if x>100 else f'{x:.2f}' for x in xs)
print("== S1 msg/s  key | "+" | ".join(f"{m} r1/r2/r3 (med)" for m in MODES)+" | 2048/m | 3072/m")
ratios={m:{} for m in MODES[1:]}
for k in sorted(d["manual"]):
    mm=st.median(d["manual"][k]); row=[]
    for m in MODES:
        v=d[m].get(k,[]); row.append(f"{fmt(v)} ({st.median(v):.4g})" if v else "-")
    rs=[]
    for m in MODES[1:]:
        v=d[m].get(k,[]); r=st.median(v)/mm if v else float('nan'); ratios[m][k]=r; rs.append(f"{r:.2f}")
    print(f"{k.split('/',1)[1]:45s} | "+" | ".join(row)+" | "+" | ".join(rs))
print("== randomBench walMax / hold")
wal={m:[] for m in MODES}; hold={m:{"restart":[], "commit":[], "tx":[]} for m in MODES}
for m in MODES:
    for f in sorted(glob.glob(f"{R}/random-{m}-s*-out/runs-*.jsonl")):
        for line in open(f):
            j=json.loads(line); wal[m].append(j["walMaxBytes"])
            print(f"{f.split('/')[-2]:24s} msg/s={j['msgPerSec']:.0f} p50={j['p50Ms']} p99={j['p99Ms']} walMax={j['walMaxBytes']/1e6:.2f}MB verdict={j['verdict']}")
    for f in sorted(glob.glob(f"{R}/*-{m}-*.log")):
        for line in open(f, errors="replace"):
            mt=re.search(r"\[exp-hold\] mode=(\S+) (tx|commit|restart) .*max=([\d.]+)ms", line)
            if mt and mt.group(1)==m: hold[m][mt.group(2)].append(float(mt.group(3)))
        for line in open(f, errors="replace"):
            if "[exp-open]" in line and "shm=false" not in line or ("[exp-open]" in line and "exclusive" not in line):
                print("!! not exclusive/shm:", f, line.strip())
for m in MODES:
    print(f"{m:10s} walMax max={max(wal[m])/1e6 if wal[m] else float('nan'):.2f}MB ({', '.join(f'{x/1e6:.2f}' for x in wal[m])})  "
          + "  ".join(f"{k} max={max(v) if v else float('nan'):.2f}ms(n={len(v)})" for k,v in hold[m].items()))
print("== 기준")
P=lambda k: "POWER_LOSS" in k
for m in MODES[1:]:
    c1=all(r>=0.9 for k,r in ratios[m].items() if not P(k))
    c2=all(r>=0.9 for k,r in ratios[m].items() if P(k) and re.search(r"/P[357]/POWER_LOSS/",k))
    c3=(max(hold[m]["commit"]) if hold[m]["commit"] else 1e9) < 100
    c4=max(wal[m]) <= max(wal["manual"]) if wal[m] and wal["manual"] else False
    print(f"{m}: c1={c1} (min {min(r for k,r in ratios[m].items() if not P(k)):.2f}) c2={c2} c3={c3} c4={c4}")
