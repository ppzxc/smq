# smq 공개 API 스모크 하네스

공개 API 스모크 하네스는 별도 Gradle 빌드다. smq 배포 JAR과 SQLite JDBC를 모듈 경로에 올리고, `io.github.ppzxc.smq` 공개 API를 호출한다. 기본 스모크·백로그·soak 명령에는 브로커 서버가 필요하지 않다. RabbitMQ 직접 비교는 별도 선택 실행이다.

저장소 루트에서 기본 실행은 두 내구성 수준과 단건·배치 발행을 모두 실행한다. 루트 JAR 빌드도 함께 한다.

```sh
./gradlew smokeHarness
```

기본값은 측정 반복마다 1,000건, 워밍업 100건, 반복 3회, payload 256B, 배치 최대 100건, 미정산 최대 1,000건, 꺼내기 청크 최대 100건이다. `POWER_LOSS`의 단건 발행은 저장 장치의 fsync 속도에 따라 오래 걸릴 수 있다. 측정 크기를 바꾸려면 다음처럼 넘긴다.

```sh
./gradlew smokeHarness -Psmoke.args="--count 100 --warmup 10 --repeat 2 --payload-bytes 256 --batch-size 16 --max-in-flight 64 --chunk-size 16"
```

보고서는 `smoke-harness/build/smoke-harness/report.json`에, SQLite 파일은 `smoke-harness/build/smoke-harness/work/`에 생긴다. 보고서에는 설정을 그대로 다시 넘길 수 있는 `reproduction.args`가 포함된다. 내구성이나 모드 하나만 실행하려면 `--durability PROCESS_CRASH` 또는 `--mode batch`를 추가한다.

별도 빌드만 실행할 수도 있다. 이 방법은 먼저 smq JAR을 만들어야 한다.

```sh
./gradlew jar
./gradlew -p smoke-harness run --args="--count 100 --warmup 10 --repeat 1"
```

프로세스 강제 종료 뒤 복구를 짧게 검증하려면 다음 명령을 실행한다. 외부 제어 프로세스가 대상 JVM을 종료한 뒤 공개 API로 같은 SQLite 저장소를 다시 연다. 브로커는 필요하지 않다.

```sh
./gradlew smokeHarness -Psmoke.args="recovery --durability BOTH --message-count 8 --lease-ms 150"
```

결과는 `smoke-harness/build/smoke-harness/recovery-report.json`에 기록된다. `--negative-control drop-confirmed-publish`를 추가하면 대상 JVM 종료 후 격리 저장소 파일을 제거해, 확인된 발행 유실을 하네스가 실패로 잡는지 확인한다.

## 동기화 스냅샷 기반 저장소 장애 모델

`storage-loss`는 별도 임시 저장소에서 `POWER_LOSS`로 확인된 발행·정산과 `PROCESS_CRASH` 결과를 JVM 강제 종료 뒤 공개 API로 다시 확인한다. 완료 응답은 저장소 디렉터리 밖의 `completion-ledger.txt`에 기록한다. 격리 모델은 동기화된 상태를 `Smq.snapshot()`으로 보관한 뒤, 성공 응답을 받은 `PROCESS_CRASH` 쓰기를 만든다. 대상 JVM을 강제 종료하고 동기화 스냅샷으로 저장소를 되돌려, 스냅샷에 있던 상태는 남고 그 뒤 쓰기는 없어지는지 확인한다. 이 동작은 파일 스냅샷 복구 모델이며 실제 전원 차단이나 장치의 캐시 손실을 시험하지 않는다.

기본 실행은 두 내구성 수준의 JVM 종료 관측과 저장소 모델을 모두 요구한다.

```sh
./gradlew smokeHarness -Psmoke.args="storage-loss --durability BOTH --simulate-storage-loss true --require-storage-loss true --output smoke-harness/build/smoke-harness/storage-loss-report.json --work-dir smoke-harness/build/smoke-harness/storage-loss-work"
```

보고서의 `faultModel`, `observationCoverage`, `storageModel`, `physicalPowerCut`, `completionLedger`는 실행한 장애 종류, 저장소 복구 방법, 외부 완료 기록 경로와 실제 전원 차단을 수행하지 않았다는 점을 구분한다. `--simulate-storage-loss false`는 모델을 건너뛰며, 함께 `--require-storage-loss true`를 주면 보고서에 미실행 사유를 남기고 종료 코드 1을 반환한다.

검증기를 확인된 발행의 유실과 함께 시험하려면 다음 부정 대조를 실행한다. 이 명령은 의도한 대로 실패 코드 1을 반환해야 한다.

```sh
./gradlew smokeHarness -Psmoke.args="storage-loss --durability POWER_LOSS --simulate-storage-loss true --require-storage-loss true --negative-control drop-confirmed-publish --output smoke-harness/build/smoke-harness/storage-loss-negative-report.json --work-dir smoke-harness/build/smoke-harness/storage-loss-negative-work"
```

## 빈 큐와 백로그 처리량 비교

`backlog` 명령은 같은 SQLite 내구성 수준과 큐 설정에서 빈 큐와 사전 적재 큐를 비교한다. 기본 백로그 크기는 의도적으로 정하지 않았다. 측정 크기를 직접 지정해야 하며, 자동화 검사는 작은 값만 사용한다.

빠르게 실행해 보고서를 확인하려면 다음을 실행한다.

```sh
./gradlew smokeHarness -Psmoke.args="backlog --durability PROCESS_CRASH --backlog-count 100 --duration-ms 250 --warmup-ms 50 --repeat 1 --payload-bytes 256 --batch-size 16 --chunk-size 16 --publishers 1 --consumers 1"
```

전체 측정은 다음과 같이 실행한다. 두 내구성 수준과 기본, 최대 전달 횟수 5, 단일 활성 컨슈머, 리스 중인 백로그 조건을 각각 빈 큐와 100만 건 백로그에서 3회 잰다.

```sh
./gradlew smokeHarness -Psmoke.args="backlog --durability BOTH --backlog-count 1000000 --duration-ms 10000 --warmup-ms 3000 --repeat 3 --payload-bytes 256 --batch-size 100 --chunk-size 100 --publishers 1 --consumers 1"
```

보고서는 `smoke-harness/build/smoke-harness/backlog-report.json`에, 각 워밍업·측정 조건의 독립 SQLite 파일은 `smoke-harness/build/smoke-harness/backlog-work/` 아래 고유 실행 디렉터리에 생긴다. 같은 실행 안에서도 내구성·큐 종류·깊이·반복마다 DB가 다르다. 재현 명령과 인자는 보고서에 포함된다.

| 보고 필드 | 의미 |
|---|---|
| `publishThroughputMessagesPerSecond` | 측정 구간 안에 완료된 발행 메시지 수 / 설정된 측정 시간 |
| `deliveryLatencyP50Nanos`, `deliveryLatencyP99Nanos` | 측정 구간 안에 꺼낸 측정 발행 메시지의 발행 시작부터 `pull` 반환까지 지연. nearest-rank 방식이며 표본이 없으면 `null` |
| `settleThroughputMessagesPerSecond` | 측정 구간 안에 ACK 정산을 마친 메시지 수 / 설정된 측정 시간 |
| `startDepth`, `endDepth` | 워크로드 직전과 종료 후의 ready + leased + delayed 합계. `startCounts`와 `endCounts`에 상태별 수가 함께 기록됨 |
| `queueConfig` | 최대 전달 횟수와 dead letter 큐, 단일 활성 컨슈머 여부, leased-head 설정 |
| `depthRatios` | 같은 내구성·큐 종류·반복에서 백로그 측정값을 빈 큐 측정값으로 나눈 값. 처리량은 백로그 / 빈 큐, 지연도 백로그 / 빈 큐 |

두 조건은 같은 payload 크기와 내용, 발행자·컨슈머 수, 발행 배치, 꺼내기 청크, 측정 시간으로 실행한다. 측정 전 워밍업은 내구성·큐 종류별 별도 빈 큐 DB에서 실행하고 결과에는 포함하지 않는다. 측정 DB는 각 깊이와 반복마다 새로 만든다. `LEASED_HEAD`에서는 미리 넣은 메시지를 최대 10,000개씩 한 시간 리스로 꺼내 두어, 리스가 걸린 백로그에서 새 발행을 처리한다.

이전 하네스의 [깊이 기준과 비교 방식](../docs/benchmark.md#비교-보고)은 S1의 100만 건 처리량이 빈 큐 처리량의 70% 이상인지 본다. 여기서는 `settleThroughput` 비율이 ACK 처리량에 가장 가까운 지표이며, 새 시나리오는 동시 프로듀서·컨슈머를 사용하므로 70%는 참고 기준이지 자동 합격 판정은 아니다. 이전 S3 지연 기준은 POWER_LOSS p99 10ms 이하였고, 새 `deliveryLatencyP99Nanos`는 같은 발행-꺼내기 경로를 보지만 배치 간격과 부하가 달라 직접적인 같은 조건 비교는 아니다. 이전 절대 처리량 floor(P1/P3/P7)는 이 시나리오의 메시지 배치 크기와 달라 적용하지 않는다.

## Kafka 단일 노드 내구성 비교

Kafka CLI의 계획은 Docker 없이 확인할 수 있다. 먼저 smq JAR을 만들고 `plan`을 실행한다.

```sh
./gradlew jar
./gradlew -p smoke-harness run --args="kafka plan"
```

로컬 Docker가 실행 중일 때 아래 명령으로 단건·배치 발행 측정과 네 가지 강제 종료 시점의 복구 관찰을 수행한다. `kafka run`은 두 토픽 설정을 각각 별도 단일 브로커와 데이터 볼륨에서 실행한다.

```sh
./gradlew -p smoke-harness run --args="kafka run --count 1000 --warmup 100 --repeat 3 --batch-size 100 --payload-bytes 256 --port 19092 --work-dir smoke-harness/build/kafka-work --output smoke-harness/build/kafka-report.json"
```

두 설정 모두 토픽 복제 계수와 `min.insync.replicas`가 1이고, 발행자는 `acks=all`을 사용한다. 기본 설정은 토픽 `flush.messages`를 덮어쓰지 않아 브로커 기본값을 사용한다. 두 번째 설정은 토픽을 `flush.messages=1`로 만든다. 보고서의 `brokerConfigurations`와 `measurements[].topicConfiguration.settings`에는 AdminClient가 읽은 설정값, 설정 출처, 기본값 여부가 들어간다. `serverVersion`과 `clientVersion`은 실제 실행 버전이며 서버 이미지는 `apache/kafka:4.4.0`으로 고정되어 있다.

측정 보고서에는 성공한 producer 응답 수, consumer-group `commitSync` 응답에 따른 정산 수와 지연, 측정 전·후 그룹 lag가 기록된다. 복구 시나리오는 publish 응답 직후, 전달 후 commit 전, commit 응답 직후, 응답이 오기 전 요청 진행 중에 브로커를 강제 종료하고 같은 Docker 볼륨과 `/var/lib/kafka/data`로 다시 시작한다. 보고서에는 재시작 후 노드·컨트롤러·파티션 leader·replica·ISR 확인 결과, 외부 완료 원장, 레코드와 그룹 offset 관찰이 포함된다. 요청 중 연결이 끊겨 응답을 받지 못한 발행은 `UNKNOWN`으로 집계한다. 재시작 뒤 레코드가 관찰되지 않더라도 그 사실만으로 요청이 적용되지 않았다고 판정하지 않는다.

Docker 기반 CLI 테스트는 명시적으로 켰을 때만 브로커를 시작한다. 이 테스트는 기본 설정과 `flush.messages=1` 설정 모두를 실행한다.

```sh
SMQ_KAFKA_DOCKER_TESTS=true ./gradlew -p smoke-harness test --tests io.github.ppzxc.smq.harness.KafkaCliTest --rerun-tasks
```

테스트가 만든 컨테이너와 데이터 볼륨은 테스트 종료 때 제거된다. 결과 보고서와 외부 원장은 지정한 `--output` 및 `--work-dir` 경로에 남는다.

하네스 실행에 쓰는 Kafka 클라이언트는 Gradle이 이름 있는 모듈로 올릴 수 있도록 `build/kafka-module/` 아래에 manifest만 보강한 빌드 전용 JAR로 준비한다. 이 JAR과 Kafka 의존성은 smq 배포 JAR이나 소스 JAR에 포함되지 않는다.

루트의 `./gradlew check`는 smq JAR/소스 JAR에서 하네스와 그 의존성이 빠졌는지 검사하고 하네스 CLI 테스트도 실행한다. 이 테스트는 별도 JVM에서 배포 JAR을 이름 있는 JPMS 모듈로 실행한다.

## 지정 장비 기준선과 릴리스 게이트

`backlog`는 `--queue-kind DEFAULT|MAX_DELIVERIES|SINGLE_ACTIVE_CONSUMER|LEASED_HEAD`와 `--depth EMPTY|BACKLOG`로 단일 조건을 선택할 수 있다. 지정 장비 실행 스크립트는 내구성·큐 설정·깊이·반복 조합마다 별도 Java 프로세스와 새 데이터베이스를 만들고, 같은 조건의 별도 데이터베이스에서 워밍업한 뒤 3회 측정한다. 각 측정 보고서에 실행 호스트, 파일시스템, 코드 revision과 설정을 기록한다.

첫 구·신 하네스 및 smq·RabbitMQ·Kafka 비교는 저장소 루트에서 다음을 실행한다. 이 작업은 ns0270의 로컬 XFS에서만 실행되고, 고정 하네스와 새 하네스의 실행 순서를 바꾸며, RabbitMQ 4.4.1·Kafka 4.4.0 단일 노드 비교 보고서도 만든다.

```sh
./smoke-harness/run-initial-comparison.sh
```

원시 보고서와 실행 로그는 `build/initial-comparison-<revision>/`에 남는다. 새 하네스의 `new-backlog-round-1/backlog-report.json`에는 내구성·큐 설정·빈 큐/100만 건 조건별 세 반복이 들어간다. 이를 첫 기준선으로 만든다.

```sh
python3 smoke-harness/release_gate.py baseline \
  --report build/initial-comparison-<revision>/new-backlog-round-1/backlog-report.json \
  --output smoke-harness/release-baseline.json
```

기준선을 만든 뒤 같은 ns0270 checkout에서 필수 릴리스 게이트를 실행한다. 게이트는 깨끗한 checkout, 현재 코드 revision, 지정 호스트와 XFS, 48개 필수 표본, 보고서 설정, 처리량·지연 회귀 및 깊이 기준을 확인한다. 누락되거나 불일치하는 표본은 실패한다.

```sh
./smoke-harness/run-release-gate.sh
```

리포트는 `build/release-benchmark/<revision>/backlog-report.json`에 기록된다. 새 처리량 지표는 기존 S1 단건/묶음 절대 하한과 워크로드가 달라 절대값을 직접 복사하지 않는다. 각 새 조건의 기준선 반복 중앙값에서 처리량 80% 하한과 p99 지연 120% 상한을 적용해 기존 20% 회귀 허용치를 유지한다. 기존 S1 백로그 처리량 기준은 새 `settleThroughput`의 100만 건/빈 큐 비율에 대응시켜 70%를 유지하며, 반복 비율의 중앙값으로 판정한다. 기존 S3의 p99 10ms는 부하·발행 간격이 달라 직접 대응할 수 없어 새 기준선 대비 20% 증가 상한으로 바꾼다. 설정·원시 결과와 기준선은 첫 측정 뒤 `docs/benchmark-results/`에 보존한다.

## RabbitMQ 비교 (선택 실행)

RabbitMQ 비교기는 smq API가 아니라 공식 RabbitMQ Java 클라이언트로 직접 메시지를 발행·가져온다. RabbitMQ Server 이미지는 `rabbitmq:4.4.1-management`, 클라이언트는 `com.rabbitmq:amqp-client:5.36.0`으로 고정한다. 클래식 큐와 단일 멤버 quorum 큐 모두 durable이며, 메시지는 persistent (`deliveryMode=2`)로 발행한다. quorum 큐는 `x-quorum-initial-group-size=1`로 한 멤버를 지정한다.

선택적인 Docker 통합 실행은 루트에서 다음 명령으로 시작한다. 기본값은 측정 반복당 1,000건, 워밍업 100건, 3회 반복이다. 짧은 재현은 예시처럼 크기를 낮춘다.

```sh
RABBITMQ_COUNT=24 RABBITMQ_WARMUP=2 RABBITMQ_REPEAT=1 ./smoke-harness/rabbitmq-integration.sh
```

이 스크립트는 Docker가 필요하며 로컬 포트 5674에 컨테이너를 띄우고 새 Docker named volume을 `/var/lib/rabbitmq`에 마운트한다. 컨테이너는 `rabbitmq` 사용자로 실행하며, 시작 전에 해당 사용자가 volume을 읽고 쓸 수 있는지 확인한다. 컨테이너와 volume은 그대로 남긴다. 기본 컨테이너 이름은 `smq-rabbitmq-441-integration`이다. 다른 이름·포트·volume 이름은 `RABBITMQ_CONTAINER`, `RABBITMQ_AMQP_PORT`, `RABBITMQ_VOLUME`로 지정할 수 있다. volume 이름은 새 이름이어야 한다. 사용자와 암호 기본값은 로컬 실험용 `bench`이며 `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`로 덮어쓸 수 있다.

측정 결과는 `smoke-harness/build/smoke-harness/rabbit-report.json`, 강제 종료 복구 결과는 `smoke-harness/build/smoke-harness/rabbit-recovery-report.json`에 기록한다. 두 보고서에 실제 브로커·클라이언트 버전, queue 설정, 작업량, 발행 확인·라우팅 오류, consumer ACK 전송, 복구 뒤 관측값과 재현 명령이 들어간다. 성능 보고서는 단건·배치 발행과 가져오기·settle 처리의 처리량과 지연 표본을 기록한다. 로컬 Docker 파일시스템과 자원 제한은 결과에 영향을 주므로 이 수치는 기준 성능으로 간주하지 않는다.

발행은 `mandatory=true`와 ReturnListener로 라우팅 실패를 오류로 세고, publisher confirm을 받은 라우팅 성공 메시지만 완료로 센다. Consumer `basic.ack`는 별도로 전송을 기록한다. RabbitMQ는 consumer ACK에 publisher confirm을 보내지 않으므로, ACK 적용은 연결을 닫은 뒤 새 consumer가 메시지를 다시 가져오는지와 ready-message 수를 별도로 관찰한다.

복구 실행은 확인 직후, 메시지를 가져온 뒤 ACK 전, 그리고 broker를 잠시 멈춘 동안 publish 요청을 보내 confirm을 얻기 전에 각각 같은 컨테이너와 데이터 디렉터리를 강제 종료·재시작한다. 새 큐를 만든 뒤 2초 기다린 다음 해당 메시지 관측 직후 broker를 중단한다. 각 결과의 `queueStableForMs`와 method 설정에 이 대기 시간을 기록한다. 이는 RabbitMQ가 quorum 큐 생성 직후 Raft 디렉터리 동기화를 지연하는 알려진 동작을 피하기 위한 설정이다 ([upstream discussion #17710](https://github.com/rabbitmq/rabbitmq-server/discussions/17710)).

이 비교의 quorum 큐는 멤버가 하나뿐이므로 노드 장애 허용성이 없다. 관측하는 것은 단일 노드 프로세스 재시작 뒤 같은 저장소에서의 복구이며, quorum failover나 노드 손실 내구성을 검증하지 않는다. 다중 노드 quorum 큐는 과반수 멤버가 유지될 때 메시지를 보존한다는 RabbitMQ의 [quorum queue 문서](https://www.rabbitmq.com/docs/quorum-queues)를 참고한다. 일반 `./gradlew check`는 Docker를 사용하지 않는다.

## 시드 재현 soak

일반 공개 API 스모크는 `./gradlew smokeHarness`로 실행한다. 큐 동작 불변식을 짧게 확인하려면 고정 시드와 작은 라운드 예산으로 seeded soak을 실행한다.

```sh
./gradlew smokeHarness -Psmoke.args="soak --seed 42 --budget 2 --delay-ms 20 --durability PROCESS_CRASH"
```

`--budget`은 내구성 수준마다 실행할 라운드 수다. 라운드마다 발행·꺼내기 대조, 최대 전달 횟수와 dead letter 이동, 지연과 근사 counts, 단일 활성 컨슈머를 공개 API로 검사한다. 시드는 메시지 payload, 메시지 수와 청크 크기를 정한다. 같은 시드, 예산, 지연 및 내구성 설정은 같은 연산을 만들며, SHA-256 `workloadDigest`로 실행 간 워크로드를 비교할 수 있다. 실행 중 시간에 따라 달라지는 것은 지연이 끝나기를 기다리는 시간뿐이다.

지정 환경에서 장시간 반복할 때는 아래처럼 시드와 예산을 고정하고 두 내구성 수준을 모두 실행한다. 라운드별 결과는 `schemaVersion: 1` 공통 보고서의 `results` 배열에 각각 남는다.

```sh
./gradlew smokeHarness -Psmoke.args="soak --seed 20261128 --budget 10000 --delay-ms 250 --durability BOTH --output build/smoke-harness/soak-long-report.json --work-dir build/smoke-harness/soak-long-work"
```

보고서는 `smoke-harness/build/smoke-harness/soak-report.json`에, SQLite 파일은 시드와 내구성 수준별 실행 디렉터리에 남는다. 실패하면 명령은 코드 1로 끝나며, 보고서에는 위반한 불변식, 시드, 설정, 관련 메시지 id와 기대·관측 결과가 들어간다. 하네스가 실패를 감지하는지 확인하는 음성 대조는 `--negative-control drop-first-delivery`다.

| 기존 불변식 | seeded 시나리오와 구현 | 공개 API 테스트 | 유지되는 계약 검사 |
|---|---|---|---|
| 유실 (LOSS) | `SoakRunner#checkMessageAccounting` | `SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule` | `StorageConservationPropertiesTest#commandSequencesPreserveSqliteQueueState` |
| 미발행 메시지 전달 (GHOST_DELIVERY) | `SoakRunner#checkMessageAccounting` | 같은 `SoakCliTest` | `StorageConservationPropertiesTest#pull` |
| 허용 범위를 넘긴 중복 (DUPLICATE_LIMIT) | `SoakRunner#checkDeliveryLimit` | 같은 `SoakCliTest` | `SmqConformanceTest#messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck1` |
| dead letter 이동 | `SoakRunner#checkDeliveryLimit` | 같은 `SoakCliTest` | 같은 `SmqConformanceTest` 시나리오의 이동 헤더 검사 |
| 지연 (DELAY) | `SoakRunner#checkDelayAndCounts` | 같은 `SoakCliTest` | `StorageContractTest#delayedMessageCannotBePulledBeforeDeadline`, `#delayedMessageCanBePulledAtDeadline` |
| 단일 활성 컨슈머 | `SoakRunner#checkSingleActiveConsumer` | 같은 `SoakCliTest` | `StorageContractTest#ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck1` |
| 근사 counts | `SoakRunner#checkDelayAndCounts` | 같은 `SoakCliTest` | `StorageContractTest#recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck1`, ADR-0018 |
| 청크 내 순서 (CHUNK_ORDER) | `SoakRunner#checkMessageAccounting` | 같은 `SoakCliTest` | `SmqConformanceTest#fifoOrderFollowsPublishOrderAcrossBatches` |

이 seeded soak은 교체 전환을 위한 추가 경로다. `randomBench`와 `randomBenchSmoke`는 티켓 #267까지 유지한다.
