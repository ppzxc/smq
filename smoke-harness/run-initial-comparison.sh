#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"
REVISION=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain)" ]; then
    echo "comparison run requires a clean checkout at $REVISION" >&2
    exit 1
fi

HOST=$(hostname -f)
if [ "$HOST" != "ns0270.nanoit.kr" ]; then
    echo "comparison run must use ns0270.nanoit.kr; current host is $HOST" >&2
    exit 1
fi
export SMQ_BENCHMARK_HOST="$HOST"

OUT=${SMQ_INITIAL_RESULTS_DIR:-"build/initial-comparison-$REVISION"}
if [[ "$OUT" != /* ]]; then
    OUT="$ROOT_DIR/$OUT"
fi
mkdir -p "$OUT"
if [ "$(stat -f -c '%T' "$OUT")" != "xfs" ]; then
    echo "comparison results must be on xfs" >&2
    exit 1
fi

docker pull rabbitmq:4.4.1-management >/dev/null
docker pull apache/kafka:4.4.0 >/dev/null
./gradlew jar

run_old_benchmark() {
    local round=$1
    local output="$OUT/legacy-benchmark-round-$round.json"
    local work="$OUT/legacy-work-round-$round"
    local status=0
    if ./gradlew benchmark --repeat 3 -Pbenchmark.args="--work-dir $work --output $output" \
        >"$OUT/legacy-benchmark-round-$round.log" 2>&1; then
        status=0
    else
        status=$?
    fi
    if [ ! -s "$output" ]; then
        echo "legacy benchmark did not write $output; see its log" >&2
        return 1
    fi
    echo "legacy-benchmark-round-$round=$status" >> "$OUT/exit-codes.txt"
}

run_backlog_benchmark() {
    local round=$1
    local reports="$OUT/new-backlog-round-$round"
    "$ROOT_DIR/smoke-harness/run-backlog-suite.sh" "$reports" >"$OUT/new-backlog-round-$round.log" 2>&1
    test -s "$reports/backlog-report.json"
    echo "new-backlog-round-$round=0" >> "$OUT/exit-codes.txt"
}

run_smq_smoke() {
    local round=$1
    local output="build/smoke-harness/initial-smq-round-$round.json"
    ./gradlew smokeHarness -Psmoke.args="--durability BOTH --mode BOTH --count 10000 --warmup 1000 --repeat 3 --payload-bytes 256 --batch-size 100 --chunk-size 100 --max-in-flight 1000 --output $output --work-dir build/smoke-harness/initial-smq-work-round-$round" \
        >"$OUT/smq-round-$round.log" 2>&1
    cp "$ROOT_DIR/smoke-harness/$output" "$OUT/smq-round-$round.json"
}

run_rabbitmq() {
    local round=$1
    local port=5674
    if [ "$round" = 2 ]; then
        port=5675
    fi
    local container="smq-rabbitmq-initial-r$round"
    local volume="smq-rabbitmq-initial-r$round-$REVISION"
    RABBITMQ_CONTAINER="$container" \
        RABBITMQ_VOLUME="$volume" \
        RABBITMQ_AMQP_PORT="$port" \
        RABBITMQ_COUNT=10000 \
        RABBITMQ_WARMUP=1000 \
        RABBITMQ_REPEAT=3 \
        "$ROOT_DIR/smoke-harness/rabbitmq-integration.sh" >"$OUT/rabbit-round-$round.log" 2>&1
    cp "$ROOT_DIR/smoke-harness/build/smoke-harness/rabbit-report.json" "$OUT/rabbit-round-$round.json"
    cp "$ROOT_DIR/smoke-harness/build/smoke-harness/rabbit-recovery-report.json" "$OUT/rabbit-recovery-round-$round.json"
    docker stop "$container" >/dev/null
}

run_kafka() {
    local round=$1
    local report="$ROOT_DIR/smoke-harness/build/smoke-harness/initial-kafka-round-$round.json"
    local work="$ROOT_DIR/smoke-harness/build/smoke-harness/initial-kafka-work-round-$round"
    ./gradlew -p smoke-harness "-PsmqJar=$ROOT_DIR/build/libs/smq-0.1.0-SNAPSHOT.jar" run \
        --args="kafka run --count 10000 --warmup 1000 --repeat 3 --batch-size 100 --payload-bytes 256 --output $report --work-dir $work" \
        >"$OUT/kafka-round-$round.log" 2>&1
    cp "$report" "$OUT/kafka-round-$round.json"
}

run_engine() {
    local engine=$1
    local round=$2
    echo "comparison round $round: $engine"
    case "$engine" in
        smq) run_smq_smoke "$round" ;;
        rabbitmq) run_rabbitmq "$round" ;;
        kafka) run_kafka "$round" ;;
    esac
}

run_old_benchmark 1
run_backlog_benchmark 1
run_backlog_benchmark 2
run_old_benchmark 2

for engine in smq rabbitmq kafka; do
    run_engine "$engine" 1
done
for engine in kafka rabbitmq smq; do
    run_engine "$engine" 2
done

printf 'source revision: %s\n' "$REVISION" > "$OUT/environment.txt"
printf 'host: %s\n' "$HOST" >> "$OUT/environment.txt"
printf 'work filesystem: %s\n' "$(stat -f -c '%T' "$OUT")" >> "$OUT/environment.txt"
printf 'Java: %s\n' "$(java -version 2>&1 | head -n 1)" >> "$OUT/environment.txt"
printf 'Docker: %s\n' "$(docker --version)" >> "$OUT/environment.txt"
echo "comparison artifacts: $OUT"
