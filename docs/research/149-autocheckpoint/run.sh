#!/usr/bin/env bash
# 실험(#149): manual / auto-2048 / auto-3072를 바퀴마다 순서를 돌려 3바퀴 잰다.
set -u
W=$(cd "$(dirname "$0")/../../.." && pwd)
O=$W/docs/research/149-autocheckpoint/raw
cd $W
vmstat -t 5 > $O/vmstat.log &
VM=$!
trap "kill $VM" EXIT
log(){ echo "$(date '+%F %T') $*" | tee -a $O/progress.log; }
ORDERS=("manual auto-2048 auto-3072" "auto-2048 auto-3072 manual" "auto-3072 manual auto-2048")
for round in 1 2 3; do
  for mode in ${ORDERS[$((round-1))]}; do
    tag=fixed-$mode-r$round
    log "START $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q benchmark --repeat 3 -Pbenchmark.args="--filter S1/ --output $O/$tag-S1.json" > $O/$tag-S1.log 2>&1
    log "END $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
  done
done
for seed in 1 2 3; do
  for mode in ${ORDERS[$((seed-1))]}; do
    tag=random-$mode-s$seed
    log "START $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
    rm -rf build/random-bench
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q randomBench --load 60s --seed $seed > $O/$tag.log 2>&1
    cp -r build/random-bench $O/$tag-out 2>/dev/null
    log "END $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
  done
done
log DONE
