import json,glob,os,re,statistics as st,sys
R=sys.argv[1]
def load(mode,sc):
    out={}
    for r in (1,2,3):
        f=f"{R}/fixed-{mode}-r{r}-{sc}.json"
        if os.path.exists(f):
            for k,v in json.load(open(f))["entries"].items(): out.setdefault(k,[]).append(v)
    return out
for sc in ("S1","S3"):
    m,a=load("manual",sc),load("auto",sc)
    print(f"== {sc}  key | manual r1/r2/r3 (med) | auto r1/r2/r3 (med) | auto/manual")
    for k in sorted(m):
        mv,av=m[k],a.get(k,[])
        mm,am=st.median(mv),st.median(av) if av else float('nan')
        print(f"{k.split('/',1)[1]:45s} | {'/'.join(f'{x:.0f}' if x>100 else f'{x:.2f}' for x in mv)} ({mm:.4g}) | {'/'.join(f'{x:.0f}' if x>100 else f'{x:.2f}' for x in av)} ({am:.4g}) | {am/mm:.2f}")
print("== randomBench")
for f in sorted(glob.glob(f"{R}/r*-out/runs-*.jsonl")):
    tag=f.split('/')[-2][:-4]
    for line in open(f):
        d=json.loads(line); p=d["params"]
        print(f"{tag:18s} dur={p['durability']:13s} rate={p.get('targetRate')} msg/s={d['msgPerSec']:.0f} p50={d['p50Ms']} p99={d['p99Ms']} walMax={d['walMaxBytes']/1e6:.1f}MB verdict={d['verdict']} pullWait={p['pullWaitMillis']} batch={p['publishBatch']}")
