#!/usr/bin/env bash
set -u
W=/home/ppzxc/orca/workspaces/smq/exp139v2
O=/tmp/claude-1000/-home-ppzxc-orca-workspaces-smq-elephantfish/cc87d7ed-5f6c-41d6-a6d3-99f555a16c09/scratchpad/run
cd $W
vmstat -t 5 > $O/vmstat.log &
VM=$!
trap "kill $VM" EXIT
log(){ echo "$(date '+%F %T') $*" | tee -a $O/progress.log; }
for round in 1 2 3; do
  for mode in manual auto; do
    tag=fixed-$mode-r$round
    log "START $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q benchmark --repeat 3 -Pbenchmark.args="--filter S1/ --output $O/$tag-S1.json" > $O/$tag-S1.log 2>&1
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q benchmark --repeat 1 -Pbenchmark.args="--filter S3/ --output $O/$tag-S3.json" > $O/$tag-S3.log 2>&1
    log "END $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
  done
done
for seed in 1 2; do
  for mode in manual auto; do
    tag=rate-$mode-s$seed
    log "START $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
    rm -rf build/random-bench
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q randomBench --rate 500 --load 3m --seed $seed > $O/$tag.log 2>&1
    cp -r build/random-bench $O/$tag-out 2>/dev/null
    log "END $tag rc=$? load: $(cut -d' ' -f1-3 /proc/loadavg)"
  done
done
for seed in 1 2 3; do
  for mode in manual auto; do
    tag=random-$mode-s$seed
    log "START $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
    rm -rf build/random-bench
    SMQ_EXP_CHECKPOINT=$mode ./gradlew -q randomBench --load 60s --seed $seed > $O/$tag.log 2>&1
    cp -r build/random-bench $O/$tag-out 2>/dev/null
    log "END $tag load: $(cut -d' ' -f1-3 /proc/loadavg)"
  done
done
log DONE
