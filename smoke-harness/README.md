# smq 공개 API 스모크 하네스

하네스는 별도 Gradle 빌드다. smq 배포 JAR과 SQLite JDBC만 모듈 경로에 올리고, `io.github.ppzxc.smq` 모듈이 내보낸 공개 API만 호출한다. 브로커 서버는 필요하지 않다.

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

루트의 `./gradlew check`는 smq JAR/소스 JAR에서 하네스와 그 의존성이 빠졌는지 검사하고 하네스 CLI 테스트도 실행한다. 이 테스트는 별도 JVM에서 배포 JAR을 이름 있는 JPMS 모듈로 실행한다.

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
