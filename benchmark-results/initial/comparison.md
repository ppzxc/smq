# 단일 노드 초기 비교 결과 (#258)

첫 기준 장비 비교는 `ns0270.nanoit.kr`에서 같은 XFS 저장소와 로컬 Docker로 순차 실행했다. 결과 원시 JSON과 backlog 개별 표본을 이 디렉터리에 보존했다. 이 비교는 단일 노드 성능·재시작 관측이며, 제품별 완료 의미가 달라 하나의 처리량 숫자로 제품 순위를 매기지 않는다.

## 실행 조건

- 장비: Rocky Linux 9.9, kernel `5.14.0-741.22.1.el9_9.x86_64`, 24 CPU, OpenJDK `25.0.5+9-LTS`, XFS.
- 엔진 비교: smq source revision `12ffb7584e19493de6aca34fdf62b5715dcc92a6`; 각 엔진/설정에서 10,000건, 워밍업 1,000건, 반복 3회, payload 256B, 발행 batch 100건. 결과 표의 중앙값은 두 순서의 여섯 반복에서 계산했다.
- 교차 순서: 1차 `smq → RabbitMQ → Kafka`, 2차 `Kafka → RabbitMQ → smq`. 각 반복은 새 실행 디렉터리·저장소 또는 브로커를 사용했다.
- 구·신 하네스 반복은 게이트 수정 및 baseline 생성 전후라 revision이 서로 다르다. 각 보고서에 기록된 SHA를 보존했다. 구 하네스 Java 소스는 두 구 반복 사이에서, 새 측정 Java 소스는 두 새 반복 사이에서 변경되지 않았다. 엔진 간 두 순서는 모두 같은 revision에서 측정했다.
- RabbitMQ: server `4.4.1` (`rabbitmq:4.4.1-management`), official client `5.36.0`; durable classic 또는 durable 단일 멤버 quorum, persistent delivery, mandatory publish와 publisher confirm.
- Kafka: server/client `4.4.0`, replication factor 1, `min.insync.replicas=1`, producer `acks=all`; topic 기본 flush 설정 또는 `flush.messages=1`.

## 완료 기준

| 엔진 | 발행 완료 | 정산 완료로 측정한 것 | 해석 한계 |
|---|---|---|---|
| smq | `queue.publish` 성공 반환 | `queue.settle` 결과가 모두 성공 | 처리량은 publish, pull, settle을 차례로 끝낸 전체 구간이다. |
| RabbitMQ | 라우팅 오류가 없고 publisher confirm을 받은 발행 | consumer `basic.ack` 전송과 ACK 뒤 관측한 ready 수 | consumer ACK 자체에는 publisher confirm이 없다. 측정 처리량은 publish부터 ACK 전송까지이며 ACK 적용은 재시작 후 재전달 관측으로 확인한다. |
| Kafka | `acks=all` producer 성공 응답 | consumer-group `commitSync` 응답과 committed offset | 발행률과 정산률은 별도 구간이다. `UNKNOWN` 요청은 응답을 받지 못한 요청이며 성공/실패 어느 쪽으로도 단정하지 않는다. |

아래 latency 칸은 각 반복에서 계산된 p99의 중앙값이며 단위는 ms다. 처리량 단위는 messages/s다.

### smq 공개 API

| 내구성 | 모드 | 전체 처리량 | 발행 p99 | 정산 p99 |
|---|---|---:|---:|---:|
| `PROCESS_CRASH` | 단건 | 5,023 | 0.280 | 2.956 |
| `PROCESS_CRASH` | batch 100 | 19,332 | 3.733 | 2.181 |
| `POWER_LOSS` | 단건 | 2,758 | 0.618 | 2.615 |
| `POWER_LOSS` | batch 100 | 11,993 | 4.348 | 2.872 |

### RabbitMQ

| Queue | 모드 | 전체 처리량 | 발행 p99 | ACK 전송 p99 |
|---|---|---:|---:|---:|
| durable classic | 단건 | 879 | 1.124 | 0.139 |
| durable classic | batch 100 | 2,476 | 9.380 | 0.073 |
| single-member quorum | 단건 | 484 | 1.384 | 0.083 |
| single-member quorum | batch 100 | 922 | 9.310 | 0.081 |

### Kafka

| Topic flush | 모드 | 발행률 | 정산률 | 발행 p99 | 전달 p99 | commit p99 |
|---|---|---:|---:|---:|---:|---:|
| 기본값 | 단건 | 2,385 | 1,822 | 0.710 | 5,482 | 0.692 |
| 기본값 | batch 100 | 78,626 | 15,962 | 1.300 | 165 | 1.137 |
| `flush.messages=1` | 단건 | 1,059 | 1,921 | 1.353 | 9,443 | 0.715 |
| `flush.messages=1` | batch 100 | 32,845 | 16,609 | 3.789 | 329 | 0.915 |

Kafka 전달 p99는 발행 완료 뒤 별도로 consume하는 구간의 메시지 대기 시간을 포함한다. smq와 RabbitMQ는 발행 윈도마다 전달·정산을 진행한다. 따라서 이 표의 구성 간 수치는 같은 완료 조건을 맞춘 동등 비교값이 아니다. 특히 Kafka producer 발행률과 commit 정산률은 독립 지표로 읽는다.

## 기존 기준선과 새 게이트

- 기존 고정 benchmark는 1차와 2차 모두 43개 판정을 통과했다. 결과: [legacy round 1](legacy-benchmark-round-1.json), [legacy round 2](legacy-benchmark-round-2.json), [round 1 로그](legacy-benchmark-round-1.log).
- 새 baseline은 [반복 표본 보고서](new-backlog-round-1/backlog-report.json)로 만들었고, 저장소의 [release baseline](../../smoke-harness/release-baseline.json)에 기록했다. 48개 표본과 개별 표본 파일은 `new-backlog-round-1/`에 있다.
- 다음 새 backlog 반복은 측정 보고서 생성에는 성공했지만 baseline 비교에서 22개 통과·26개 실패했다. [round 2 보고서](new-backlog-round-2/backlog-report.json), [게이트 출력](new-backlog-round-2/new-backlog-round-2-check.txt).
- 기존 하네스를 제거하기 전 release gate 실행은 39개 통과·9개 실패였다. 실패는 주로 `PROCESS_CRASH`의 MAX_DELIVERIES/SINGLE_ACTIVE_CONSUMER backlog 정산 처리량과 깊이 비율, 두 기본 조건의 처리량 하한이었다. [보고서](pre-delete-release-gate/backlog-report.json), [게이트 출력](pre-delete-release-gate/pre-delete-gate-check.txt). 이 실패를 기준 완화로 숨기지 않았다.
- #267 삭제 적용 후 source revision `83efc61b254ec6f4520381927f9e15bc23e3a2c2`에서 새 릴리스 게이트를 실행해 48/48 표본을 만들었다. 측정은 성공했지만 기준 비교는 33개 통과·15개 실패로 종료했다. 주된 실패는 backlog settle 처리량과 70% 깊이 비율, `PROCESS_CRASH`의 두 queue-kind p99였다. [보고서](post-removal-release-gate/backlog-report.json), [실행 로그](post-removal-release-gate/release-gate-post-removal.log), [커밋 보고서 검사](post-removal-release-gate/check-committed-output.txt), [릴리스 입력 보고서](../release-report.json). 누락 표본이나 stale source 때문에 실패한 것이 아니라 측정된 기준 위반이다. 이 결과는 이 변경을 성능 릴리스 PASS로 간주할 수 없음을 뜻한다.

기존 benchmark의 절대 처리량 floor, 70% backlog 비율, p99 목표와 무작위 검증의 대응 근거는 [전환표](../../docs/benchmark-transition.md)에 있다. 새 backlog 표본에서는 100만 건 큐가 측정 중 새 발행분까지 따라잡지 못하는 경우가 있어 p99가 `null`일 수 있다. 릴리스 판정은 빈 큐 p99와 backlog settle 처리량·깊이 비율을 구분한다.

## 내구성 관측과 범위

- RabbitMQ 두 queue 모두 확인 발행 뒤 복구, 미정산 전달 재전달, 요청 중단의 `UNKNOWN`을 두 번의 비교에서 기록했다. [round 1 복구](rabbit-recovery-round-1.json), [round 2 복구](rabbit-recovery-round-2.json).
- Kafka는 기본 flush와 `flush.messages=1` 각각에서 확인 발행 복구, offset commit 복구, commit 전 재전달, 요청 진행 중 `UNKNOWN`을 관측했다. 두 보고서 모두 `unknownRequests: 2`다. [round 1](kafka-round-1.json), [round 2](kafka-round-2.json).
- Kafka 복구 보고서의 `key` 값은 관측을 연결하는 테스트 메시지 UUID다. Gitleaks의 `generic-api-key`가 이를 오탐해 두 보고서의 정확한 finding fingerprint만 저장소 루트 `.gitleaksignore`에 등록했다.
- Rabbit quorum에는 멤버가 하나뿐이고 Kafka replication factor가 1이다. 이 실험은 노드 장애 허용성이나 quorum failover를 검증하지 않는다.
- `POWER_LOSS` 비교는 SQLite 동기화 설정과 모의 저장소 스냅샷 검증을 다룬다. 실제 물리 전원 차단은 수행하지 않았다.

## 원시 산출물

각 엔진의 완전한 JSON 보고서와 실행 로그, 기존·새 benchmark 보고서, gate 표본을 이 디렉터리에서 찾을 수 있다. 재현 명령은 각 JSON의 `reproduction` 필드에 들어 있다. 새 기준 장비 릴리스 gate는 기존 기준을 통과하지 못하는 후보를 실패시켰으며, 이는 게이트가 기준 위반을 놓치지 않는지 확인하는 결과다.
