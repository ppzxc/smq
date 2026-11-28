# 벤치마크·무작위 검증 전환표

새 기준선은 [초기 비교 보고서](../benchmark-results/initial/comparison.md)와 [릴리스 기준선](../smoke-harness/release-baseline.json)에 기록한다. 측정 절차와 실행 명령은 [스모크 하네스 안내](../smoke-harness/README.md)에 있다.

## 성능 판정 기준

| 이전 항목 | 이전 판정 | 새 대응 | 대응 근거와 한계 |
|---|---|---|---|
| 고정 프로파일 S1/S2의 P0–P7 처리량 floor | 메시지 크기·배치·내구성별 절대 msg/s 하한 | 새 하네스 조건별 3개 표본 중앙값에서 처리량이 기준선의 80% 이상 | 새 워크로드의 payload, 배치, 동시 프로듀서·컨슈머가 달라 기존 절대값을 직접 복사하지 않는다. 기존 20% 회귀 허용치는 유지한다. |
| S1 백로그 깊이 | 빈 큐 처리량 대비 100만 건 큐 처리량 70% 이상 | `settleThroughput`의 backlog/empty 비율, 반복 비율 중앙값 70% 이상 | 새 시나리오는 발행과 정산을 함께 측정한다. 정산 처리량이 기존 소비 처리량에 가장 가까운 지표다. |
| S3 POWER_LOSS p99 지연 | 기존 P1 부하에서 p99 10ms 이하 | 기준선 p99 대비 최대 120%, 빈 큐 조건에만 적용 | 새 배치 간격과 동시 처리량이 달라 10ms를 직접 이식하지 않는다. 빈 큐에서도 producer가 consumer보다 빨라 큐 대기 시간이 포함된다. 100만 건 backlog에서는 측정 중 새 발행분까지 도달하지 못해 p99가 `null`일 수 있어 판정하지 않는다. |
| 무작위 벤치의 처리량·p50/p99 | 기록만 하고 판정하지 않음 | 고정된 새 기준선의 처리량, p99, 깊이 판정에 포함 | 새 표본은 지정 호스트·XFS에서 반복 실행해 보고한다. 무작위 시드 실행은 릴리스 기준선이 아니다. |

기준선과 후보 결과는 각각 `smoke-harness/release-baseline.json`과 릴리스 보고서에 원시 표본을 보존한다. 누락된 48개 조건 표본, 잘못된 깊이, 호스트·파일시스템 불일치도 실패다.

## 무작위 하네스 불변식의 대응

무작위 하네스의 임의 조합 실행은 제거했다. 아래의 결정적 계약·보존 테스트는 남지만, 여러 동작을 무작위 순서로 엮는 탐색 범위와 완전히 같은 보장은 아니다.

| 이전 검사 | 남기는 결정적 검증 | 새 내구성 하네스의 역할 |
|---|---|---|
| 성공 발행의 유실·dead letter 이중 ACK 없음 (`LOSS`) | `StorageConservationPropertiesTest`, 저장소 계약 테스트, `SmqConformanceTest` | JVM 강제 종료 복구와 모의 저장소 손실의 확인된 완료 원장 검증 |
| 발행하지 않은 메시지나 바뀐 payload/header 없음 (`GHOST`) | `OutgoingMessagePropertiesTest`, 저장소 계약 및 공개 API conformance | 복구 스모크가 복구 전후 메시지 식별과 외부 완료 원장을 비교 |
| 정당하지 않은 중복·deliveryCount 공백·ACK 뒤 재전달 없음 (`DUPLICATE`) | 리스 만료·전달 횟수·정산 결과를 다루는 `SmqConformanceTest`, `LeaseCounterexampleTest` | `recovery` 시나리오가 미정산 메시지 재전달과 확인된 정산 보존을 검증 |
| reject/최대 전달 횟수와 dead letter 이동·헤더 일치 (`DEAD_LETTER`) | `SmqConformanceTest`와 저장소 계약의 최대 전달 횟수·dead letter 테스트 | RabbitMQ 비교 하네스의 queue-policy 시나리오만 비교 용도로 실행 |
| 발행 지연·NACK 지연 준수 (`DELAY`) | 가상 시계 기반 `SmqConformanceTest`, `MutableClock` 테스트 | 내구성 복구는 지연 스케줄러의 무작위 조합을 대체하지 않는다 |
| 단일 활성 컨슈머 lease 중 다른 청크 차단 (`SINGLE_ACTIVE`) | `SmqConformanceTest`의 단일 활성 컨슈머 계약 | 새 backlog 측정은 `SINGLE_ACTIVE_CONSUMER` queue-kind 처리량을 별도로 기록 |
| 종료 counts 0 및 부하 중 음수 없음 (`COUNTS`) | `StorageConservationPropertiesTest`, `SqliteStorageCountersTest`, 저장소 계약 | 복구 스모크가 재시작 뒤 ready/leased/delayed 수와 완료 원장을 대조 |
| 청크 안 메시지 id 순서 (`CHUNK_ORDER`) | `SmqConformanceTest` FIFO 청크 계약, memory/SQLite 저장소 계약 | RabbitMQ/Kafka 비교 결과는 외부 MQ 지표이며 smq 순서 계약을 대신하지 않는다 |

| 기존 무작위 시나리오 | seeded 검증 경로 | 공개 API 테스트 | 유지되는 계약 검사 |
|---|---|---|---|
| 유실 (LOSS) | `SoakRunner#checkMessageAccounting` | `SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule` | `StorageConservationPropertiesTest#commandSequencesPreserveSqliteQueueState` |
| 미발행 메시지 전달 (GHOST_DELIVERY) | `SoakRunner#checkMessageAccounting` | 같은 `SoakCliTest` | `StorageConservationPropertiesTest#pull` |
| 허용 범위를 넘긴 중복 (DUPLICATE_LIMIT) | `SoakRunner#checkDeliveryLimit` | 같은 `SoakCliTest` | `SmqConformanceTest#messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck1` |
| dead letter 이동 | `SoakRunner#checkDeliveryLimit` | 같은 `SoakCliTest` | 같은 `SmqConformanceTest` 시나리오의 이동 헤더 검사 |
| 지연 (DELAY) | `SoakRunner#checkDelayAndCounts` | 같은 `SoakCliTest` | `StorageContractTest#delayedMessageCannotBePulledBeforeDeadline`, `#delayedMessageCanBePulledAtDeadline` |
| 단일 활성 컨슈머 | `SoakRunner#checkSingleActiveConsumer` | 같은 `SoakCliTest` | `StorageContractTest#ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck1` |
| 근사 counts | `SoakRunner#checkDelayAndCounts` | 같은 `SoakCliTest` | `StorageContractTest#recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck1`, ADR-0018 |
| 청크 내 순서 (CHUNK_ORDER) | `SoakRunner#checkMessageAccounting` | 같은 `SoakCliTest` | `SmqConformanceTest#fifoOrderFollowsPublishOrderAcrossBatches` |

무작위 하네스가 제공하던 교차 조합의 결함 탐색은 seeded soak, 결정적 단위·저장소 계약 테스트와 독립 JVM 복구 테스트로 나눠 추적한다. 이 테스트들은 무작위 fuzzing 전체를 완전하게 재현한다고 주장하지 않는다.
