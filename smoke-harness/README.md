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

루트의 `./gradlew check`는 smq JAR/소스 JAR에서 하네스와 그 의존성이 빠졌는지 검사하고 하네스 CLI 테스트도 실행한다. 이 테스트는 별도 JVM에서 배포 JAR을 이름 있는 JPMS 모듈로 실행한다.

## 시드 재현 soak

일반 공개 API 스모크는 `./gradlew smokeHarness`로 실행한다. 큐 동작 불변식을 짧게 확인하려면 고정 시드와 작은 라운드 예산으로 seeded soak을 실행한다.

```sh
./gradlew smokeHarness -Psmoke.args="soak --seed 42 --budget 2 --delay-ms 20 --durability PROCESS_CRASH"
```

`--budget`은 내구성 수준마다 실행할 라운드 수다. 라운드마다 발행·꺼내기 대조, 최대 전달 횟수와 dead letter 이동, 지연과 근사 counts, 단일 활성 컨슈머를 공개 API로 검사한다. 시드는 메시지 payload, 메시지 수와 청크 크기를 정한다. 같은 시드, 예산, 지연 및 내구성 설정은 같은 연산을 만들며, SHA-256 `workloadDigest`로 실행 간 워크로드를 비교할 수 있다. 실행 중 시간에 따라 달라지는 것은 지연이 끝나기를 기다리는 시간뿐이다.

지정 환경에서 장시간 반복할 때는 아래처럼 시드와 예산을 고정하고 두 내구성 수준을 모두 실행한다. 라운드별 결과는 `schemaVersion: 1` 공통 보고서의 `results` 배열에 각각 남는다.

```sh
./gradlew smokeHarness -Psmoke.args="soak --seed 20261128 --budget 10000 --delay-ms 50 --durability BOTH --output build/smoke-harness/soak-long-report.json --work-dir build/smoke-harness/soak-long-work"
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
