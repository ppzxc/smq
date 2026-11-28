# SQLite 큐 인덱스와 성능 판정 근거 (#166)

- 조사일: 2026-11-23
- 범위: [#166](https://github.com/ppzxc/smq/issues/166)의 저장 상태·부분 인덱스와 고정 벤치 판정. 1차 자료와 저장소의 기존 기준을 구분한다. 외부 문서는 smq의 실측 결과를 보증하지 않는다.
- 관련 기록: [ADR 0019](../adr/0019-stored-state-and-promotion-for-sqlite-pull.md), [현재 기준선 절차](../release.md), [구·신 기준 대응](../benchmark-transition.md).

## SQLite가 보장하거나 설명하는 것

1. 부분 인덱스는 조건이 참인 행만 포함한다. SQLite는 적절한 부분 인덱스가 파일 크기와 읽기·쓰기 성능을 개선할 수 있다고 설명한다. 조건에 비결정적 함수나 바인드 매개변수를 둘 수 없다. **추론:** 현재 시각에 따라 인덱스 회원이 저절로 바뀌는 설계는 이 제약에 맞지 않으며, 저장 상태를 갱신하는 승격은 그 대안이다. 갱신으로 인한 인덱스 유지 비용은 별도 측정해야 한다. [SQLite Partial Indexes](https://sqlite.org/partialindex.html)
2. 부분 인덱스 사용에는 질의 조건이 인덱스 조건을 함의해야 한다. SQLite는 제한된 규칙으로만 이를 증명하며 일반적인 대수 변환은 하지 않는다. 비교식은 `IS NOT NULL` 조건을 만족시키는 경우가 있지만, 수학적으로 동등하다는 사실만으로 임의의 표현식이 인식되는 것은 아니다. **적용:** 전달 횟수의 부분 인덱스 조건을 질의에도 명시하고 실제 계획을 확인한다. [SQLite Partial Indexes §3](https://sqlite.org/partialindex.html#queries_using_partial_indexes)
3. 다중 열 인덱스는 왼쪽 열부터 정렬되며, 적절한 열 순서와 조건은 검색과 정렬을 함께 수행하게 할 수 있다. `EXPLAIN QUERY PLAN`의 `SEARCH`는 일부 행을 방문한다는 뜻이고 `SCAN ... USING INDEX`도 전체 순회일 수 있다. 임시 정렬은 `USE TEMP B-TREE FOR ORDER BY`로 보고될 수 있다. **적용:** 인덱스 사용 문구만 확인하지 말고 각 하위 질의의 검색 조건과 정렬도 확인한다. `SEARCH` 자체는 검색 범위 내 후보가 적다는 보장이나 실행 시간 보장이 아니다. [SQLite Query Planning](https://sqlite.org/queryplanner.html), [SQLite EXPLAIN QUERY PLAN](https://sqlite.org/eqp.html)
4. SQLite는 `EXPLAIN QUERY PLAN` 출력을 대화형 진단용이라고 명시하고 버전별 형식 변경을 경고한다. **적용:** #166이 요구한 계획 테스트는 의도적인 버전 의존 테스트로 취급하고 SQLite/드라이버 갱신 시 계획과 테스트를 함께 검토한다. 계획 테스트만으로 FIFO, 경계 시각, 실제 성능을 증명할 수 없다. [SQLite EXPLAIN QUERY PLAN](https://sqlite.org/eqp.html)
5. 플래너 선택은 통계의 영향을 받는다. SQLite는 현재 `PRAGMA optimize`를 통한 통계 관리 방법을 설명한다. **적용:** 실측과 계획 재현 때 SQLite 버전, 통계 유무와 생성 방법을 기록한다. 이는 smq가 운영 중 통계를 새로 수집해야 한다는 결정이 아니다. [SQLite ANALYZE](https://sqlite.org/lang_analyze.html)

## 벤치 판정에 적용할 구분

- Google Benchmark는 CPU 주파수 변화, 스케줄링, 캐시, SMT, NUMA 등을 반복 측정의 변동 원인으로 설명한다. OpenJDK는 Java 마이크로벤치마크에 초기화와 컴파일을 포함한 워밍업을 권한다. **추론:** 동일한 장비·부하·JDK·저장 설정, 워밍업과 반복 결과를 기록해야 작은 차이를 해석할 수 있다. 반복 중앙값은 잡음 완화 수단이지 통계적 확실성 보증은 아니다. [Google Benchmark: Reducing Variance](https://google.github.io/benchmark/reducing_variance.html), [OpenJDK MicroBenchmarks](https://wiki.openjdk.org/display/HotSpot/MicroBenchmarks)
- SQLite WAL 문서는 `synchronous=FULL`과 `NORMAL`의 동기화 및 내구성 차이를 설명한다. **적용:** 모드가 다른 수치를 같은 성능 기준선으로 직접 비교하거나, 속도 개선을 위해 내구성 모드를 바꾸었다면 같은 보장으로 해석해서는 안 된다. [SQLite WAL](https://sqlite.org/wal.html)
- 저장소의 전환 전 절차는 기준 장비에서의 반복 중앙값, 깊이 비율과 기준선 대비 회귀 차단을 명시했다. #166은 새 깊은 조건과 스키마 변경 후 기준선 재측정을 요구하며 WSL2 단발 프로브는 문제 크기 설명용이라고 한정한다. **판단:** 새 인덱스는 깊은 큐 이득과 함께 얕은 큐의 발행·꺼내기·정산 비용을 지정 장비 게이트로 평가한다. 허용 범위를 벗어나면 스펙대로 인덱스 구성을 재검토하며, 프로브 값으로 게이트를 통과시키거나 기준선을 임의로 대체하지 않는다. [현재 기준선 절차](../release.md), [#166](https://github.com/ppzxc/smq/issues/166)
- 승격은 만료된 행 수에 비례하는 일괄 쓰기이므로 정상 상태 처리량 외에 한꺼번에 만료된 리스·지연의 다음 꺼내기 지연과 그때 다른 연산이 기다리는 시간도 관찰할 가치가 있다. 이는 설계에서 도출한 **추론**이며 #166의 프로브에서 보고한 약 120ms를 다른 환경의 예측값으로 쓰지 않는다. [#166](https://github.com/ppzxc/smq/issues/166)

## 미확인

실제 스키마·데이터 분포에서 각 질의가 선택할 계획, 인덱스별 쓰기 증폭, 승격 꼬리 지연, 기준 장비에서의 새 기준선은 이 문헌 조사로 확인하지 못했다. 시간별 FIFO와 늦은 정산의 정확성은 SQLite 문서가 아니라 smq 계약 테스트로 확인해야 한다.


## 유사 프로젝트와 명세 차이

아래는 조사 시점의 원본 코드에 대한 비교다. 처리량 수치나 기능 우열의 비교가 아니다.

| 사례 | 확인한 구현 | smq에 그대로 대입할 수 없는 이유 |
|---|---|---|
| [River SQLite](https://github.com/riverqueue/river/blob/1f19003d1b65e32206f18bd73b222d61b1773972/riverdriver/riversqlite/internal/dbsqlc/river_job.sql) | `available` 상태를 둔 뒤 `UPDATE ... WHERE id IN (SELECT ... ORDER BY ... LIMIT ...) RETURNING`으로 여러 작업을 가져온다. 스케줄러는 예정된 작업을 `available`로 바꾸고, [복합 인덱스](https://github.com/riverqueue/river/blob/1f19003d1b65e32206f18bd73b222d61b1773972/riverdriver/riversqlite/migration/main/006_bulk_unique.up.sql)는 state, queue, priority, scheduled_at, id 순이다. | 작업 상태·우선순위·스케줄 순서와 smq의 id FIFO, 리스 토큰, 지연 순서, 늦은 정산은 동일하지 않다. 조회 한 문장의 사용이 같은 카운터 전이 또는 성능을 보장하지 않는다. |
| [Goqite](https://github.com/maragudk/goqite/blob/71a935991bf7971440a7e09f50d2fca5fbb73eb8/goqite.go) | 시각 조건과 priority/created 순서로 하나를 골라 timeout·received를 UPDATE RETURNING한다. [인덱스](https://github.com/maragudk/goqite/blob/71a935991bf7971440a7e09f50d2fca5fbb73eb8/schema_sqlite.sql)는 queue, priority, created 순이고 timeout은 인덱스 키에 없다. | 인덱스 정렬 순서로 훑으며 timeout을 평가하므로 앞에 비가용 항목이 많은 경우 비용이 깊이와 무관하다고 추론할 수 없다. ID 기반 Delete/Extend와 smq의 토큰 기반 낡은 정산 계약도 다르다. |
| [Huey SQLite](https://github.com/coleifer/huey/blob/7ea73730b9b6d413effdd67880970eb3e674974c/huey/storage.py) | 빈 큐를 읽기 경로에서 먼저 확인하고, 작업을 선택해 삭제하는 트랜잭션을 사용한다. [변경 기록](https://github.com/coleifer/huey/blob/7ea73730b9b6d413effdd67880970eb3e674974c/CHANGELOG.md)은 빈 큐 시 배타 락 회피와 queue 열을 포함한 인덱스를 설명한다. | dequeue 즉시 삭제하는 구현은 smq의 리스·재전달·늦은 ACK와 같은 경로가 아니므로 처리량을 비교하거나 삭제 방식을 그대로 이식할 수 없다. |
| [Amazon SQS FIFO](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html) | 메시지 그룹별 순서를 보장하고 같은 그룹의 in-flight 메시지가 정산되거나 다시 보일 때까지 후속 수신을 제한한다. | 관리형 분산 서비스의 공개 동작 참고다. SQLite 구현·인덱스나 smq의 지연 발행 규칙을 증명하지 않는다. |

SQLite `RETURNING`은 변경 결과를 메모리에 모은 뒤 전달한다. 이는 ACK의 `DELETE ... RETURNING` 및 큰 pull 배치의 비용을 계측할 **근거**이지 이 절만으로 병목의 크기나 대체안의 우월함을 증명하지는 않는다. [SQLite RETURNING 처리 순서](https://www.sqlite.org/lang_returning.html#processing_order)

## #166에 대한 권고와 미결정

1. **게이트를 임의로 낮추지 않는다.** 이전 기준 `45186cf` 대비 현 후보는 ns0270 교차 비교에서 P1 깊은 큐 중앙값 −21.83%, P4 빈 큐 중앙값 −15.79%이나 P4 개별 표본 1회 FAIL이다. 프로젝트는 20% 경계와 단일 실패 규칙을 이미 정했다. 외부 문헌은 그 수치가 보편적 업계 표준이라고 말하지 않는다. [실측 기록](https://github.com/ppzxc/smq/issues/166#issuecomment-7101427478)
2. **다음 실험은 P1 깊은 큐의 pull UPDATE/RETURNING과 인덱스 유지 비용을 타깃으로 한다.** 기준 장비의 진단에서 UPDATE+결과 읽기가 청크당 약 618–645µs였으나 계측 오버헤드와 예열이 섞인 값이다. 기존 FIFO·늦은 ACK·카운터·migration을 보존하는 설계 가설만 시험하고 정확성/쿼리 계획과 얕은·깊은 전체 gate를 함께 검증한다. [실측 기록](https://github.com/ppzxc/smq/issues/166#issuecomment-7101427478)
3. **ACK 후보는 아이디어로 보존하고 통합을 미룬다.** `Long.MAX_VALUE` 토큰 중복 fallback을 포함한 후보가 단위·계약 검사와 일부 짧은 측정을 통과해도, 전체 기준 장비 gate는 아직 실패했다. [현재 구현](../../src/main/java/io/github/ppzxc/smq/storage/sqlite/SqliteStorage.java)
4. 다음 성능 판단은 같은 장비에서 기준·후보를 교차 실행하고 각 표본과 median, 환경, JIT 워밍업, 큐 깊이, durability, 하네스 버전을 공개한다. #177의 6회 중앙값은 별도이며, 정확성 테스트가 통과하는 것만으로 성능 gate를 통과한 것은 아니다. [Google Benchmark variance](https://google.github.io/benchmark/reducing_variance.html), [프로젝트 기준선 절차](../release.md)
