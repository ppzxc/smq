#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=rabbitmq:4.4.1-management
CONTAINER=${RABBITMQ_CONTAINER:-smq-rabbitmq-441-integration}
PORT=${RABBITMQ_AMQP_PORT:-5674}
USERNAME=${RABBITMQ_USERNAME:-bench}
PASSWORD=${RABBITMQ_PASSWORD:-bench}
COUNT=${RABBITMQ_COUNT:-1000}
WARMUP=${RABBITMQ_WARMUP:-100}
REPEAT=${RABBITMQ_REPEAT:-3}
export RABBITMQ_USERNAME="$USERNAME" RABBITMQ_PASSWORD="$PASSWORD"

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
    printf 'Container %s already exists; choose another RABBITMQ_CONTAINER or inspect it first.\n' "$CONTAINER" >&2
    exit 1
fi

VOLUME=${RABBITMQ_VOLUME:-"smq-rabbitmq-441-integration-$(date +%s)-$$"}
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    printf 'Volume %s already exists; choose another RABBITMQ_VOLUME or inspect it first.\n' "$VOLUME" >&2
    exit 1
fi
docker volume create "$VOLUME" >/dev/null

docker run --rm \
    --user rabbitmq \
    --volume "$VOLUME:/var/lib/rabbitmq" \
    --entrypoint bash \
    "$IMAGE" -ec 'test -r /var/lib/rabbitmq && test -w /var/lib/rabbitmq && touch /var/lib/rabbitmq/.smq-write-check && rm /var/lib/rabbitmq/.smq-write-check'

docker run --detach \
    --name "$CONTAINER" \
    --pull=missing \
    --user rabbitmq \
    --publish "127.0.0.1:$PORT:5672" \
    --env "RABBITMQ_DEFAULT_USER=$USERNAME" \
    --env "RABBITMQ_DEFAULT_PASS=$PASSWORD" \
    --volume "$VOLUME:/var/lib/rabbitmq" \
    "$IMAGE"

for attempt in $(seq 1 60); do
    if docker exec "$CONTAINER" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    if [[ "$(docker inspect --format '{{.State.Status}}' "$CONTAINER")" != running ]]; then
        printf 'RabbitMQ exited during startup in container %s:\n' "$CONTAINER" >&2
        docker logs --tail 80 "$CONTAINER" >&2
        exit 1
    fi
    if [[ "$attempt" == 60 ]]; then
        printf 'RabbitMQ did not become ready in container %s. It has been left running for inspection.\n' "$CONTAINER" >&2
        exit 1
    fi
    sleep 1
done

if [[ "$(docker exec "$CONTAINER" rabbitmqctl version | tr -d '\r\n')" != 4.4.1 ]]; then
    printf 'Unexpected RabbitMQ version in %s. Expected 4.4.1.\n' "$CONTAINER" >&2
    exit 1
fi

cd "$ROOT"
./gradlew jar
./gradlew -p "$ROOT/smoke-harness" \
    "-PsmqJar=$ROOT/build/libs/smq-0.1.0-SNAPSHOT.jar" \
    run \
    --args="rabbit run --host 127.0.0.1 --port $PORT --mode both --count $COUNT --warmup $WARMUP --repeat $REPEAT --payload-bytes 256 --batch-size 100 --max-in-flight 1000 --chunk-size 100 --output build/smoke-harness/rabbit-report.json --work-dir build/smoke-harness/rabbit-work"

./gradlew -p "$ROOT/smoke-harness" \
    "-PsmqJar=$ROOT/build/libs/smq-0.1.0-SNAPSHOT.jar" \
    run \
    --args="rabbit recovery --host 127.0.0.1 --port $PORT --container $CONTAINER --output build/smoke-harness/rabbit-recovery-report.json --work-dir build/smoke-harness/rabbit-recovery-work"

printf 'Reports: %s/smoke-harness/build/smoke-harness/rabbit-report.json and rabbit-recovery-report.json\n' "$ROOT"
printf 'Container %s and Docker volume %s mounted at /var/lib/rabbitmq are left in place.\n' "$CONTAINER" "$VOLUME"
