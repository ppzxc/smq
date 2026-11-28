#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
REPORT_DIR=${1:?usage: run-backlog-suite.sh <report-directory>}
cd "$ROOT_DIR"
if [[ "$REPORT_DIR" != /* ]]; then
    REPORT_DIR="$ROOT_DIR/$REPORT_DIR"
fi
SAMPLE_DIR="$REPORT_DIR/samples"
AGGREGATE="$REPORT_DIR/backlog-report.json"
mkdir -p "$SAMPLE_DIR"

HOST=$(hostname -f)
if [ "$HOST" != "ns0270.nanoit.kr" ]; then
    echo "designated-host benchmark must run on ns0270.nanoit.kr; current host is $HOST" >&2
    exit 1
fi
export SMQ_BENCHMARK_HOST="$HOST"

FILESYSTEM=$(stat -f -c '%T' "$REPORT_DIR")
if [ "$FILESYSTEM" != "xfs" ]; then
    echo "benchmark work directory must use xfs; current filesystem is $FILESYSTEM" >&2
    exit 1
fi

./gradlew jar

for repeat in 1 2 3; do
    for durability in PROCESS_CRASH POWER_LOSS; do
        for queue_kind in DEFAULT MAX_DELIVERIES SINGLE_ACTIVE_CONSUMER LEASED_HEAD; do
            for depth in EMPTY BACKLOG; do
                report="$SAMPLE_DIR/round-$repeat-$durability-$queue_kind-$depth.json"
                work="$REPORT_DIR/work/round-$repeat-$durability-$queue_kind-$depth"
                echo "benchmark sample $repeat $durability $queue_kind $depth"
                ./gradlew -p smoke-harness run --args="backlog --durability $durability --queue-kind $queue_kind --depth $depth --backlog-count 1000000 --duration-ms 10000 --warmup-ms 3000 --repeat 1 --payload-bytes 256 --batch-size 100 --chunk-size 100 --publishers 1 --consumers 1 --output $report --work-dir $work"
            done
        done
    done
done

python3 smoke-harness/release_gate.py aggregate --report-dir "$SAMPLE_DIR" --output "$AGGREGATE"
echo "aggregated report: $AGGREGATE"
