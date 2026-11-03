#!/bin/bash
cd /home/ppzxc/orca/workspaces/smq/elephantfish
for r in 1 2 3; do
  for m in manual auto; do
    for sc in S1 S3; do
      echo "=== $(date +%T) round $r mode $m $sc"
      SMQ_EXP_CHECKPOINT=$m ./gradlew benchmark --repeat 3 -Pbenchmark.args="--filter $sc/ --output build/exp139/$sc-$m-r$r.json" > build/exp139/$sc-$m-r$r.log 2>&1
      grep -m1 "\[exp\]" build/exp139/$sc-$m-r$r.log
    done
  done
done
echo DONE
