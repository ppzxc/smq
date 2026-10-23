# 백엔드별 큐 구현 요건 비교 (SQLite, PostgreSQL, 인메모리)

- 티켓: [#3](https://github.com/ppzxc/smq/issues/3) (지도 [#1](https://github.com/ppzxc/smq/issues/1))
- 조사일: 2026-10-23
- 관련 티켓: SQLite 다중 프로세스 동시성과 sqlite-jdbc 드라이버의 세부 사항(`SQLITE_BUSY`, `busy_timeout`, WAL, 연결 풀 크기 등)은 [#4](https://github.com/ppzxc/smq/issues/4)에서 다룬다. 이 문서는 그 결론을 전제로 하고 반복하지 않는다.

## 질문

같은 큐 의미론을 SQLite, PostgreSQL, 인메모리 백엔드에서 각각 어떻게 구현할 수 있고, 그 차이가 저장소 추상화 경계(SPI)에 어떤 제약을 주는가?

## 요약 (결론 먼저)

1. **리스(lease)는 커밋된 데이터여야 한다.** 처리하는 동안 트랜잭션이나 락을 계속 쥐고 있는 방식(Hangfire의 트랜잭션 fetch, Que의 세션 advisory lock)은 SQLite에서 쓸 수 없다. SQLite에서는 쓰기 락 하나를 모든 프로세스가 공유하기 때문이다. 클레임은 행의 상태(`visible_at` 또는 `lease_until`)로 표현하고, 클레임 트랜잭션은 바로 커밋한다.
2. **ack/nack/extend에는 펜싱 토큰이 필요하다.** 리스가 만료되어 다른 소비자가 다시 가져간 메시지를 이전 소비자가 ack하면 안 된다. litequeue의 `claim_id`, Hangfire의 `FetchedAt = @fetchedAt` 조건이 같은 해법이다. "이미 만료된 클레임"은 예외가 아니라 정상적인 반환값으로 다룬다.
3. **가시성 타임아웃은 PGMQ 방식으로 한다.** 클레임 쿼리 자체가 `visible_at <= now` 조건으로 만료된 리스를 다시 집어 간다. 그러면 별도의 rescuer 작업이나 리더 선출이 필요 없다. 시도 횟수는 클레임 시점에 같은 문장 안에서 증가시킨다.
4. **"지금"을 누가 정하는지 SPI에 명시해야 한다.** PostgreSQL은 서버 시계(`clock_timestamp()`)를 쓸 수 있다. SQLite에는 서버가 없으므로 호출자가 epoch 기준 벽시계 값을 넘겨야 한다. `System.nanoTime()`은 프로세스 사이에서 비교할 수 없다.
5. **새 메시지 알림은 선택적인 힌트이고, 정확성은 폴링이 보장한다.** PostgreSQL은 `LISTEN/NOTIFY`를 쓸 수 있다. SQLite에는 프로세스 간 푸시 수단이 없으므로 `PRAGMA data_version` 폴링이나 알림 아웃박스 테이블 폴링을 쓴다. 인메모리는 `Condition`으로 즉시 깨울 수 있다.
6. **선행 사례는 모두 잡 큐이고, smq처럼 exchange/binding이 있는 경우는 없다.** 여러 프로세스가 파일을 공유하므로 토폴로지(exchange, queue, binding)도 백엔드에 저장해야 한다. "binding 조회 후 일치하는 모든 큐에 삽입"을 한 트랜잭션으로 묶을지가 SPI 경계를 정한다.
7. **SPI 후보** (자세한 내용은 [마지막 절](#결론-백엔드가-책임져야-하는-최소-연산-집합-후보)): 토폴로지 선언/삭제/조회, 원자적 `publish`, `claim`, `ack`, `release`(nack/지연 재전달), `extend`, `deadLetter`(최대 시도 초과 시), `purge`, 선택 연산 `awaitChange`, 그리고 기능 기술자(capabilities). 폴링 루프, 디스패치, 재시도 정책은 코어가 맡는다.

---

## 1. 원자적 dequeue(클레임)

### 1.1 PostgreSQL: `FOR UPDATE SKIP LOCKED`

PostgreSQL 문서는 `SKIP LOCKED`의 용도를 이렇게 설명한다. 즉시 잠글 수 없는 행은 건너뛰고, 일관된 뷰를 주지 않으므로 범용 작업에는 맞지 않지만 "여러 소비자가 큐 형태의 테이블에 접근할 때 락 경합을 피하는 데" 쓸 수 있다. 또한 `SKIP LOCKED`는 행 수준 락에만 적용되고 `ROW SHARE` 테이블 락은 평소처럼 잡힌다.
출처: https://www.postgresql.org/docs/current/sql-select.html (The Locking Clause)

**PGMQ의 `read`**는 이 패턴의 정석적인 형태다. CTE에서 `vt <= clock_timestamp()`인 행을 `ORDER BY msg_id LIMIT n FOR UPDATE SKIP LOCKED`로 고른다. 그리고 `UPDATE ... SET vt = clock_timestamp() + vt, read_ct = read_ct + 1 ... RETURNING`으로 클레임과 반환을 한 문장에서 끝낸다.
출처: https://github.com/pgmq/pgmq/blob/8efb078dbe7afe16c83be72e9fffefbb03ebd9e9/pgmq-extension/sql/pgmq.sql#L375-L413

**River(PostgreSQL 드라이버)의 `JobGetAvailable`**도 같은 형태다. `WITH locked_jobs AS (SELECT ... ORDER BY priority, scheduled_at, id LIMIT @max_to_lock FOR UPDATE SKIP LOCKED) UPDATE ... SET state = 'running', attempt = attempt + 1 ... RETURNING`.
출처: https://github.com/riverqueue/river/blob/436cbb72ffc7f0fcfe39a83bb8923739cf1b0fc6/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql#L203-L243

**Oban의 Basic 엔진**(PostgreSQL)도 `lock("FOR UPDATE SKIP LOCKED")`를 쓴다.
출처: https://github.com/oban-bg/oban/blob/722f020c0fc119f3115a4392dfac8e29e22e70d2/lib/oban/engines/basic.ex#L117

**대안: Que의 advisory lock.** Que는 행 락 대신 `pg_try_advisory_lock(id)`를 쓴다. README는 장점으로 디스크 쓰기 없이 메모리에서 락을 잡는 점, 그리고 "Ruby 프로세스가 죽으면 그 프로세스가 처리하던 잡이 즉시 다른 워커에게 넘어간다"는 점을 든다. 다만 이 락은 세션 수준이므로 처리하는 동안 연결 하나를 계속 점유해야 한다. PostgreSQL 문서는 세션 수준 advisory lock이 트랜잭션 의미론을 따르지 않는다는 점(롤백해도 락이 남는다)과, `LIMIT`이 있는 쿼리에서 락 함수가 LIMIT보다 먼저 평가될 수 있어 위험하다는 점을 경고한다.
출처: https://github.com/que-rb/que/blob/57cfa4a4d4ad3f23a4f20972ff2c7f4ceed58d85/README.md#L7-L11 , https://github.com/que-rb/que/blob/57cfa4a4d4ad3f23a4f20972ff2c7f4ceed58d85/lib/que/locker.rb#L428 , https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS

**참고: SQL Server(Hangfire).** `readpast, updlock, rowlock` 힌트가 `SKIP LOCKED`에 해당한다. 클레임 방식은 두 가지다.
- (a) 트랜잭션 fetch: 트랜잭션 안에서 `delete top (1) ... output DELETED.*`를 실행하고 잡을 처리하는 동안 트랜잭션을 열어 둔다. 프로세스가 죽으면 롤백되어 잡이 복원된다.
- (b) `SlidingInvisibilityTimeout`: `update ... set FetchedAt = GETUTCDATE() output INSERTED.*`로 클레임하고 즉시 커밋한 뒤, 하트비트로 `FetchedAt`을 갱신한다.
- 출처: https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.SqlServer/SqlServerJobQueue.cs#L211-L221 , 같은 파일 #L239-L250, #L311-L317

### 1.2 SQLite: `BEGIN IMMEDIATE` + `UPDATE ... WHERE id IN (SELECT ... LIMIT n) RETURNING`

SQLite는 쓰기를 실제로 직렬화해서 serializable 격리를 구현한다. 공식 문서 표현으로 "한 번에 하나의 writer만 존재할 수 있다". 따라서 `SKIP LOCKED`에 해당하는 기능이 필요 없다. 클레임 트랜잭션 두 개가 동시에 진행되는 일 자체가 없다. 경합은 행 락 대기가 아니라 `SQLITE_BUSY`로 나타나며, 그 처리 방법은 #4가 다룬다.
출처: https://www.sqlite.org/isolation.html

**트랜잭션 모드.** `DEFERRED`(기본값)로 시작해서 SELECT 뒤에 쓰기를 하면 읽기 트랜잭션을 쓰기로 올려야 한다. 그 사이에 다른 연결이 이미 DB를 수정했다면 승격할 수 없고 `SQLITE_BUSY`가 난다. `IMMEDIATE`는 시작할 때 바로 쓰기 트랜잭션을 연다. WAL 모드에서는 `EXCLUSIVE`와 `IMMEDIATE`가 같다. 그러므로 클레임처럼 "읽고 나서 쓰는" 트랜잭션은 반드시 `BEGIN IMMEDIATE`로 시작해야 한다.
출처: https://www.sqlite.org/lang_transaction.html

**`RETURNING`의 제약** (3.35.0, 2021-03-12부터 지원):
- 출력 행의 순서는 임의적이며 순서를 지정할 방법이 없다. 따라서 배치 클레임 결과는 애플리케이션에서 정렬해야 한다.
- DML+RETURNING을 서브쿼리나 CTE로 쓸 수 없다. PostgreSQL처럼 `WITH ... UPDATE ... RETURNING`을 다른 쿼리에 연결할 수 없다는 뜻이다.
- RETURNING이 내보내는 값은 AFTER 트리거가 바꾸기 전의 값이다.
- 출처: https://www.sqlite.org/lang_returning.html

선행 사례 세 곳 모두 같은 형태로 수렴한다.

| 구현 | 쿼리 형태 | 출처 |
|---|---|---|
| River SQLite 드라이버 | `UPDATE river_job SET attempt = attempt + 1, state = 'running' WHERE id IN (SELECT id ... ORDER BY priority, scheduled_at, id LIMIT @max_to_lock) RETURNING ...` | https://github.com/riverqueue/river/blob/436cbb72ffc7f0fcfe39a83bb8923739cf1b0fc6/riverdriver/riversqlite/internal/dbsqlc/river_job.sql#L144-L188 |
| Oban Lite 엔진 | `Job |> where(id in subquery(subset)) |> Repo.update_all(set: [state: "executing"], inc: [attempt: 1])` | https://github.com/oban-bg/oban/blob/722f020c0fc119f3115a4392dfac8e29e22e70d2/lib/oban/engines/lite.ex#L95-L124 |
| litequeue | `transaction(mode="IMMEDIATE")` 안에서 `UPDATE ... SET status = LOCKED, lock_time = :now, claim_id = claim_id + 1 WHERE rowid = (SELECT rowid ... ORDER BY message_id LIMIT 1) RETURNING *`. 3.35 미만에서는 SELECT 후 UPDATE로 대체 | https://github.com/litements/litequeue/blob/9e286af166f1c483057e28ab5fb45e1c38943f21/src/litequeue/__init__.py#L554-L615 |

River 문서는 SQLite 지원이 실험적이라고 밝히고, SQLite의 SQL 표현력이 낮다는 점(예: CTE 안에서 변경 불가)을 짚는다. 또 enqueue 트랜잭션에는 `BEGIN IMMEDIATE`를 권장한다.
출처: https://riverqueue.com/docs/sqlite

### 1.3 인메모리: 락 + 조건 변수

인메모리 백엔드는 정의상 **단일 프로세스**다. 여러 프로세스가 파일을 공유한다는 요구 사항은 이 백엔드에 적용되지 않으며, 문서와 capabilities에 그렇게 명시해야 한다.
- 큐별로 `ReentrantLock` 하나를 두고, ready 메시지 덱(deque)과 in-flight 맵(`token -> (msg, leaseUntil)`)을 그 락으로 보호하면 SQL 백엔드와 같은 의미론(원자적 클레임, 펜싱, 리스 만료)을 쉽게 맞출 수 있다. 코어가 여러 큐에 걸친 원자적 publish를 요구하면 토폴로지 전역 락(또는 읽기/쓰기 락)이 필요하다.
- 메시지마다 CAS(`AtomicReference` 상태 전이)를 쓰는 방식은 락 없이 클레임할 수 있지만, FIFO 순서와 "만료 시 재가시화"를 함께 맞추기가 까다롭다. 처리량 목표가 정해지기 전까지는 락 기반이 적절하다(판단).
- 지연이나 만료 관리에 `DelayQueue`를 쓸 수 있다. 원소는 `getDelay(NANOSECONDS) <= 0`일 때 꺼낼 수 있다. 다만 `release(delay)`와 `extend`로 데드라인이 바뀌므로, 실제로는 SQL 백엔드처럼 클레임할 때 `leaseUntil <= now`인 in-flight 항목을 함께 회수하는(lazy reclaim) 방식이 일관성이 높다(판단).
- 출처: https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/DelayQueue.html

### 1.4 경계에 주는 제약

- 클레임은 **배치(`max n`)를 반환하는 단일 연산**이어야 한다. 세 백엔드 모두 한 문장이나 한 번의 락 구간에서 끝낼 수 있다. "후보 조회"와 "선점"을 SPI에서 분리하면 SQLite에서는 트랜잭션을 SPI 바깥까지 늘려야 하므로 피한다.
- 반환 순서는 백엔드가 보장하지 않을 수 있다(SQLite RETURNING). 순서가 의미론의 일부라면 SPI 계약에 "반환 리스트는 id(또는 순서 키) 오름차순으로 정렬해서 돌려준다"고 적고, SQLite 구현이 메모리에서 정렬한다.
- 클레임 트랜잭션은 **즉시 커밋**한다. 처리가 끝날 때까지 트랜잭션을 열어 두는 Hangfire (a)형 방식은 SQLite에서 다른 모든 프로세스의 쓰기를 막는다. Que형 세션 락은 SQLite에 대응 기능이 없다. 그러므로 리스는 행의 데이터로 표현한다(2절).

## 2. 가시성 타임아웃 / 리스

### 2.1 두 가지 설계

| 방식 | 사례 | 만료 회수 | 비고 |
|---|---|---|---|
| **visible_at 비교** (클레임 시 `vt = now + timeout`, 클레임 조건이 `vt <= now`) | PGMQ | 클레임 쿼리가 자동으로 회수 | 별도의 스윕이나 리더가 필요 없음 |
| **상태 + 주기적 rescuer** (`state = running`, `attempted_at`) | River `RescueStuckJobsAfter`, Oban `Lifeline`, litequeue `retry_expired` | 리더나 플러그인이 주기적으로 UPDATE | 리더 선출이나 스케줄러가 필요함 |

- PGMQ: "가시성 타임아웃 안에서는 정확히 한 번 전달을 보장"하고, 그 시간 안에 delete나 archive를 하지 않으면 메시지가 다시 보인다. `set_vt(queue, msg_id, vt)`로 타임아웃을 연장하거나 단축한다. 출처: https://github.com/pgmq/pgmq (README), https://github.com/pgmq/pgmq/blob/8efb078dbe7afe16c83be72e9fffefbb03ebd9e9/pgmq-extension/sql/pgmq.sql#L1047-L1066
- River: `RescueStuckJobsAfter`(기본 1시간)가 지나도록 running 상태인 잡은 재시도로 돌리거나 폐기한다. 아직 처리 중인 잡이 중복 실행될 수 있다고 문서가 직접 경고한다. 출처: https://github.com/riverqueue/river/blob/436cbb72ffc7f0fcfe39a83bb8923739cf1b0fc6/client.go#L379-L397
- Oban `Lifeline`: 판정이 "순전히 시간 기준"이고 실제로 실행 중인 잡을 옮겨서 중복 실행을 일으킬 수 있다고 문서에 적혀 있다. 출처: https://github.com/oban-bg/oban/blob/722f020c0fc119f3115a4392dfac8e29e22e70d2/lib/oban/lifeline.ex#L1-L17
- litequeue: `retry_expired(threshold_seconds)`를 호출자가 직접 부른다. 출처: https://github.com/litements/litequeue/blob/9e286af166f1c483057e28ab5fb45e1c38943f21/src/litequeue/__init__.py#L796-L813

**권장: visible_at 방식.** 임베디드 라이브러리에서 여러 프로세스가 대등하게 동작하므로 리더 선출을 피하는 편이 단순하다. 메시지 상태는 `ready`와 `in-flight`를 따로 두지 않고 `visible_at` 하나로 표현할 수 있다. 다만 관측성을 위해 상태 컬럼을 두는 것은 자유다.

### 2.2 펜싱 토큰

리스가 만료되어 다른 소비자 B가 메시지를 다시 클레임한 뒤에 원래 소비자 A가 늦게 ack하면, B가 처리 중인 메시지가 사라진다. 사례별로 보면 이렇다.
- **PGMQ는 이 문제를 막지 않는다.** `delete(queue, msg_id)`와 `set_vt`는 `WHERE msg_id = $1`만 검사한다. 출처: https://github.com/pgmq/pgmq/blob/8efb078dbe7afe16c83be72e9fffefbb03ebd9e9/pgmq-extension/sql/pgmq.sql#L678-L698
- **litequeue**는 클레임할 때 `claim_id = claim_id + 1`을 하고, `done`과 `mark_failed`는 `AND status = LOCKED AND claim_id = :claim_id`일 때만 성공한다(오래된 클레임이면 `False`). 출처: https://github.com/litements/litequeue/blob/9e286af166f1c483057e28ab5fb45e1c38943f21/src/litequeue/__init__.py#L668-L695
- **Hangfire**(sliding 모드)는 delete, requeue, keep-alive 모두 `WHERE ... AND FetchedAt = @fetchedAt` 조건을 쓴다. 출처: https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.SqlServer/SqlServerTimeoutJob.cs#L83 , #L104 , #L162

**SPI 요구:** `claim`은 `(messageId, deliveryToken)`을 돌려준다. `ack`, `release`, `extend`는 토큰을 받아 조건부로 갱신하고, 결과로 `OK | STALE`을 반환한다. 토큰은 시도 횟수(`attempt`, 클레임마다 증가)나 별도 일련번호로 만들 수 있다. 인메모리에서도 같은 계약을 지킨다.

### 2.3 시도 횟수와 데드레터

PGMQ의 `read_ct`, River와 Oban의 `attempt`는 모두 **클레임 문장 안에서** 증가한다(1.1, 1.2의 쿼리). 그래서 "최대 시도 초과 시 데드레터" 판정을 클레임과 같은 원자 구간에서 할 수 있다. 소비자가 ack 전에 죽어도 횟수가 남는다. Oban Lite는 클레임 조건에 `attempt < max_attempts`를 넣는다(lite.ex L103).

### 2.4 시간 원천

- PGMQ는 DB 서버 시계(`clock_timestamp()`)를 쓴다. 여러 호스트가 붙어도 시계가 하나이므로 일관된다.
- SQLite에는 서버가 없다. `datetime('now')`나 `unixepoch()`도 결국 그 문장을 실행하는 프로세스의 OS 시계다. smq는 같은 호스트에서만 공유하므로 벽시계 epoch(ms) 값이면 프로세스 사이에서도 비교할 수 있다. `System.nanoTime()`은 JVM마다 기준점이 달라서 쓸 수 없다. litequeue도 `time_ns()`(epoch)를 호출자 쪽에서 넘긴다(위 L556-L570).
- **SPI 결정 사항:** "now"를 (a) 백엔드가 정할지(PG는 서버 시계), (b) 코어가 `java.time.InstantSource`로 주입해서 인자로 넘길지 정해야 한다. 테스트 용이성과 세 백엔드의 일관성을 보면 (b)가 유리하다. 다만 PG에서 여러 호스트가 붙는 경우는 범위 밖(#1 Out of scope)이므로 (b)로 충분하다(판단).

## 3. 새 메시지 알림

**공통 원칙: 알림은 지연을 줄이는 힌트이고, 정확성은 폴링으로 보장한다.** River도 `FetchPollInterval`(기본 1초)을 "LISTEN/NOTIFY로 보통 즉시 가져가지만 이것이 폴백"이라고 설명한다. `PollOnly` 모드를 두어 LISTEN을 쓸 수 없는 환경(예: PgBouncer 트랜잭션 풀링)을 지원한다. `FetchCooldown`(기본 100ms)은 fetch 빈도의 상한이다.
출처: https://github.com/riverqueue/river/blob/436cbb72ffc7f0fcfe39a83bb8923739cf1b0fc6/client.go#L144-L176 , #L340-L353

### 3.1 PostgreSQL: LISTEN/NOTIFY

- NOTIFY는 트랜잭션이 커밋될 때만 전달된다. 그래서 "커밋된 메시지에 대해서만 알린다"는 성질을 공짜로 얻는다.
- 페이로드는 기본 설정에서 8000바이트 미만이어야 한다. 알림 큐는 8GB이고, 가득 차면 NOTIFY를 호출한 트랜잭션이 커밋에 실패한다.
- 같은 트랜잭션에서 같은 채널과 페이로드로 여러 번 보내면 하나로 합쳐진다.
- 수신 세션이 트랜잭션 안에 있으면 그 트랜잭션이 끝난 뒤에 전달된다. 즉 **LISTEN 전용 연결**이 필요하다.
- 출처: https://www.postgresql.org/docs/current/sql-notify.html
- Java: pgjdbc의 `PGConnection.getNotifications(int timeoutMillis)`는 알림이 올 때까지 최대 timeout만큼 블록한다(0이면 무한). 이전 문서에는 "드라이버가 비동기 알림을 받을 수 없으니 폴링해야 한다"는 설명이 남아 있지만, 현재는 timeout 오버로드를 쓰는 것이 맞다. 출처: https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/PGConnection.html , https://jdbc.postgresql.org/documentation/server-prepare/ (Listen / Notify 절)
- PGMQ는 INSERT 트리거로 `PG_NOTIFY('pgmq.q_<queue>.INSERT', NULL)`을 보내고, `notify_insert_throttle` 테이블로 기본 250ms 스로틀을 건다. 출처: https://github.com/pgmq/pgmq/blob/8efb078dbe7afe16c83be72e9fffefbb03ebd9e9/pgmq-extension/sql/pgmq.sql#L1775-L1806
- PGMQ의 `read_with_poll`은 서버 측에서 `pg_sleep` 루프를 돈다(연결을 점유함). 출처: 같은 파일 #L551-L611
- Que는 LISTEN 기반 locker가 잡을 워커에 배분하고 poller가 폴백 역할을 한다. 출처: https://github.com/que-rb/que/blob/57cfa4a4d4ad3f23a4f20972ff2c7f4ceed58d85/lib/que/poller.rb#L62

### 3.2 SQLite: 프로세스 간 푸시는 없다

- **`sqlite3_update_hook`**은 등록한 연결에서 rowid 테이블의 행이 바뀔 때 호출된다. 문서는 등록한 연결 기준으로만 설명하고, 다른 연결이나 프로세스의 변경에 대해서는 말하지 않는다. WITHOUT ROWID 테이블, truncate 최적화, REPLACE 충돌 삭제에는 호출되지 않는다. **같은 프로세스 안에서 쓰는 쪽의 연결에서만 쓸모가 있다.** sqlite-jdbc도 `SQLiteConnection.addUpdateListener`와 `addCommitListener`로 이를 노출한다(세부 내용은 #4). 출처: https://www.sqlite.org/c3ref/update_hook.html , https://github.com/xerial/sqlite-jdbc/blob/master/src/main/java/org/sqlite/SQLiteConnection.java
- **`PRAGMA data_version`**: 같은 연결에서 두 번 호출한 값이 다르면 그 사이에 **다른 연결이 커밋한 것**이다. 이 동작은 별도 프로세스의 연결에도 똑같이 적용된다고 문서에 명시되어 있다. 값은 연결마다 고유하므로 같은 연결에서 나온 값끼리만 비교할 수 있다. 이 쿼리는 테이블을 읽지 않으므로 가벼울 것으로 본다(판단). 그래서 **"가벼운 변경 감지 폴링 → 변경이 있으면 클레임 시도"**라는 2단계 폴링을 할 수 있다. 단점은 어떤 테이블이든 커밋이 있으면 값이 바뀐다는 것(큐를 구분하지 못함)이다. 출처: https://www.sqlite.org/pragma.html#pragma_data_version
- **알림 아웃박스 테이블(River SQLite)**: SQLite에는 LISTEN/NOTIFY가 없으므로 River는 `NotifyMany`에서 `river_notification` 테이블에 행을 추가하고, Listener가 기본 50ms 간격으로 `id > lastID`인 행을 폴링한다. 소스 주석은 `lastID` 커서가 안전한 이유를 "SQLite가 writer를 직렬화하기 때문"이라고 밝힌다. 낮은 id를 삽입한 트랜잭션이 커밋하기 전까지 다른 트랜잭션은 더 높은 id를 커밋할 수 없다는 것이다. 그리고 "다중 writer DB에서는 성립하지 않는다"고 적는다. 즉 **같은 아웃박스 기법을 PG 시퀀스에 그대로 옮기면 알림이 누락될 수 있다.** 이것도 백엔드 경계 안에 숨겨야 하는 차이다. 출처: https://github.com/riverqueue/river/blob/436cbb72ffc7f0fcfe39a83bb8923739cf1b0fc6/riverdriver/riversqlite/river_sqlite_listener.go#L17-L46
- 그 밖에 OS 파일 감시(WAL 파일 변경 감지)도 생각할 수 있지만, 1차 출처에서 보장하는 동작이 아니므로 이 조사에서는 후보로 두지 않는다.
- 같은 프로세스 안에서는 백엔드와 무관하게 코어가 publish 직후 로컬 소비자를 직접 깨울 수 있다. Hangfire도 같은 프로세스의 enqueue 신호를 대기 핸들로 받는다(`GetWaitArrayForQueueSignals`, SqlServerJobQueue.cs L237).

### 3.3 인메모리

같은 락의 `Condition.signal`/`awaitNanos`로 즉시 깨운다. 폴링이 필요 없다.

### 3.4 경계에 주는 제약

- 알림은 별도의 선택적 SPI로 분리한다. Oban이 저장소 `Engine`(Basic/Lite/Dolphin/Inline)과 `Notifier`(Postgres/PG/Isolated)를 독립된 behaviour로 나눈 것이 선례다. 출처: https://github.com/oban-bg/oban/tree/722f020c0fc119f3115a4392dfac8e29e22e70d2/lib/oban/engines , https://github.com/oban-bg/oban/tree/722f020c0fc119f3115a4392dfac8e29e22e70d2/lib/oban/notifiers
- SPI 모양으로는 `awaitChange(queues, timeout) -> boolean`(가짜 깨움과 누락 모두 허용) 정도로 최소화한다. 코어는 결과와 무관하게 `pollInterval`마다 클레임을 시도한다. 구현 매핑은 PG는 LISTEN 연결의 `getNotifications(timeout)`, SQLite는 `data_version` 폴링이나 아웃박스, 인메모리는 `Condition`이다.

## 4. 트랜잭션 경계

- **클레임**: 한 트랜잭션, 즉시 커밋(1.4).
- **publish (smq 고유 문제)**: 선행 사례는 모두 "큐 하나에 잡 하나를 삽입"하는 잡 큐다. smq는 exchange → binding → 여러 queue로 팬아웃한다. 여러 프로세스가 파일을 공유하므로 binding은 프로세스 메모리가 아니라 백엔드에 저장해야 한다. 그렇지 않으면 다른 프로세스가 선언한 binding을 볼 수 없다. 선택지는 두 가지다.
  - (A) SPI에 `publish(exchange, routingKey, messages)`를 두고, 백엔드가 한 트랜잭션 안에서 binding을 조회하고 일치하는 모든 큐에 삽입한다. 팬아웃이 원자적이고 binding 변경과도 직렬화된다. 라우팅 규칙(direct/topic/fanout 매칭)을 백엔드마다 구현해야 하는 부담이 있는데, 매칭 로직을 코어의 순수 함수로 두고 백엔드는 binding 목록만 트랜잭션 안에서 읽게 하면 줄일 수 있다.
  - (B) 코어가 binding을 읽어 라우팅하고 SPI는 `enqueue(Set<QueueId>, messages)`만 원자적으로 처리한다. binding 조회와 삽입 사이에 binding이 바뀔 수 있다(약한 일관성). 이 경우 문서에 그렇게 정의해야 한다.
  - 권장은 (A) 계약에 (B) 구현 보조를 섞는 것이다. SPI 메서드 시그니처는 (A)이고, 백엔드 구현은 트랜잭션 안에서 `bindings(exchange)`를 읽은 뒤 코어가 제공하는 매처를 호출한다(판단). 결정은 ADR 대상이다.
- **ack/release/extend**: 각각 단일 조건부 UPDATE나 DELETE(펜싱 조건 포함)이고 별도의 트랜잭션 묶음이 필요 없다. 배치 ack를 지원하면 한 트랜잭션으로 묶어 SQLite의 쓰기 횟수(곧 fsync와 락 획득 횟수)를 줄인다.
- **애플리케이션 트랜잭션과 함께 enqueue하기(transactional outbox)**: Que, River, Oban의 큰 장점이다(Que README "Transactional Control"). SQL 백엔드에서만 의미가 있고 인메모리에서는 불가능하다. 또 사용자가 같은 DB 파일이나 연결을 공유해야 한다. 코어 SPI에 넣지 말고, SQL 백엔드 전용 확장(예: `publish(Connection tx, ...)`)으로 남겨 두는 편이 경계를 깨끗하게 유지한다. v1 범위에 넣을지는 별도로 결정한다.
- **장기 트랜잭션 금지**: Que README는 장기 트랜잭션이 잡 테이블 bloat의 주원인이라고 경고한다(PG). SQLite에서는 장기 쓰기 트랜잭션이 곧 전체 정지다. River SQLite 문서는 트랜잭션을 "한 자릿수 밀리초"로 유지하라고 권한다. 출처: https://github.com/que-rb/que/blob/57cfa4a4d4ad3f23a4f20972ff2c7f4ceed58d85/README.md#L30 , https://riverqueue.com/docs/sqlite

## 5. 백엔드 비교표

| 항목 | SQLite | PostgreSQL | 인메모리 |
|---|---|---|---|
| 클레임 | `BEGIN IMMEDIATE` + `UPDATE ... WHERE id IN (SELECT ... LIMIT n) RETURNING` | `WITH c AS (SELECT ... FOR UPDATE SKIP LOCKED) UPDATE ... RETURNING` | 큐 락 + deque |
| 동시 클레임 | writer 직렬화(경합 = `SQLITE_BUSY`, #4) | 행 락 + SKIP LOCKED로 병렬 | 락 직렬화 |
| 결과 순서 | 보장 없음 → 메모리에서 정렬 | `ORDER BY`로 고정 가능(RETURNING 순서는 별도 확인 필요) | 직접 제어 |
| 리스 표현 | `visible_at` 컬럼(epoch ms, 호출자의 시계) | `visible_at` (서버 시계 가능) | in-flight 맵 |
| 만료 회수 | 클레임 조건에 포함 | 클레임 조건에 포함 | 클레임할 때 lazy 회수 |
| 펜싱 | `attempt`나 토큰 조건부 UPDATE | 같음 | 토큰 비교 |
| 알림 | 프로세스 간 푸시 없음: `data_version`이나 아웃박스 폴링, 같은 프로세스는 update/commit hook | LISTEN/NOTIFY(전용 연결) + 폴링 폴백 | `Condition` |
| 다중 프로세스 | 지원(같은 호스트) | 지원 | **불가** |
| 앱 트랜잭션과 함께 enqueue | 가능(같은 파일과 연결) | 가능 | 불가 |

## 결론: 백엔드가 책임져야 하는 최소 연산 집합 (후보)

원칙: **원자성이 백엔드 고유의 메커니즘(트랜잭션, 락)에 의존하는 연산만 SPI에 둔다.** 폴링 루프, 디스패치(소비자 스레드), 재시도와 백오프 정책, 라우팅 매칭 로직, 직렬화는 코어가 맡는다.

```java
// 스케치일 뿐이다. 이름과 타입은 GLOSSARY/ADR에서 확정한다.
public interface StorageBackend extends AutoCloseable {

    BackendCapabilities capabilities(); // multiProcess, pushNotification, transactionalEnqueue, ...

    // --- 토폴로지: 프로세스 사이에 공유되어야 하므로 영속한다 ---
    void declareExchange(ExchangeSpec spec);              // 멱등, 기존 정의와 충돌 시 예외
    void declareQueue(QueueSpec spec);                    // maxAttempts, deadLetter 대상, 기본 lease 등
    void bind(BindingSpec spec);
    void unbind(BindingSpec spec);
    void deleteExchange(String name);
    void deleteQueue(String name);
    Topology loadTopology();                              // 캐시/검증용 스냅샷

    // --- 쓰기 경로 ---
    // binding 조회와 일치하는 모든 큐에 대한 삽입을 한 트랜잭션에서 처리한다. 반환값은 라우팅된 큐 수(0 = unroutable).
    PublishResult publish(String exchange, String routingKey, List<OutboundMessage> msgs,
                          RoutingMatcher matcher, Instant now);

    // --- 읽기 경로 ---
    // visible_at <= now 이고 (attempt < maxAttempts)인 메시지 최대 max개를 원자적으로 클레임한다.
    // attempt++, visible_at = now + lease, 새 deliveryToken 발급. 결과는 큐 순서 키로 정렬해서 반환한다.
    // attempt가 maxAttempts에 도달한 메시지는 같은 트랜잭션에서 dead-letter로 옮기거나 표시한다.
    List<Delivery> claim(String queue, int max, Duration lease, String consumerId, Instant now);

    // 모두 펜싱 조건부. 토큰이 현재 클레임과 다르면 STALE.
    Outcome ack(DeliveryToken token);                                  // 삭제(또는 보존 정책에 따라 완료 표시)
    Outcome release(DeliveryToken token, Duration delay, Instant now); // nack/requeue: visible_at = now + delay
    Outcome extend(DeliveryToken token, Duration lease, Instant now);  // 하트비트
    Outcome deadLetter(DeliveryToken token, String reason);            // 소비자가 명시적으로 reject(requeue=false)

    // --- 유지 관리 ---
    long purge(String queue);
    QueueStats stats(String queue);                       // ready / in-flight / dead 수 (관측성, 선택)

    // --- 선택: 변경 알림 힌트 (가짜 깨움 허용, 코어는 결과와 무관하게 주기적으로 폴링) ---
    default boolean awaitChange(Set<String> queues, Duration timeout) throws InterruptedException {
        Thread.sleep(timeout); return true;
    }
}
```

경계에 대한 핵심 계약(백엔드를 바꿔도 같아야 하는 것):
1. 리스는 커밋된 데이터다. 처리하는 동안 백엔드 자원(트랜잭션, 락, 연결)을 점유하지 않는다.
2. `ack`/`release`/`extend`/`deadLetter`는 펜싱 토큰으로 조건부 처리하고, 오래된 클레임은 `STALE`을 반환한다(예외 아님).
3. 만료된 리스는 `claim`이 자동으로 회수한다(rescuer 연산 없음). 전달 보장은 at-least-once다.
4. 시도 횟수는 `claim` 안에서 증가하고 데드레터 판정도 같은 원자 구간에서 한다.
5. "now"는 코어가 주입한다(epoch 기준 `InstantSource`).
6. `claim` 결과의 순서는 SPI가 보장한다(SQLite 구현이 정렬).
7. 알림은 힌트일 뿐이다. 알림이 누락돼도 정확성은 유지된다.
8. capabilities로 다중 프로세스 지원 여부(인메모리는 false)와 앱 트랜잭션 연동 여부를 노출한다.

열린 질문(다른 티켓이나 ADR로 넘길 것):
- publish 경계를 (A)로 할지 (B)로 할지 (4절). ADR 후보.
- ack 시 삭제할지, 보존(archive, PGMQ식)할지. #1의 "보존/purge 정책"과 연결된다.
- 큐별 FIFO를 얼마나 엄격하게 할지. 재전달된 메시지가 원래 위치로 돌아가는가(visible_at 방식에서는 id 순서를 유지하면 원래 위치로 돌아간다)? #1의 "순서 보장 범위"와 연결된다.
- 스키마를 큐별 테이블(PGMQ)로 할지, 단일 테이블 + queue 컬럼(River, Oban, Hangfire)으로 할지. 동적 큐 선언과 마이그레이션 단순성을 보면 단일 테이블이 유리해 보이지만 결정하지 않았다.
- SQLite 경합 처리(`SQLITE_BUSY` 재시도, 연결 풀 크기, WAL)는 #4의 결론을 따른다.

## 주의 사항

- 소스 코드 인용은 조사 시점에 각 저장소 기본 브랜치의 최신 커밋 SHA로 고정한 permalink다. 파일은 기본 브랜치 raw에서 받았고 SHA는 같은 시각에 조회했다. 그 사이에 새 커밋이 있었다면 줄 번호가 약간 어긋날 수 있다.
- River SQLite 드라이버는 문서 스스로 "실험적"이라고 밝힌다.
- "(판단)"으로 표시한 항목은 1차 출처가 직접 말한 내용이 아니라 이 조사의 추론이다.
- PostgreSQL에서 `UPDATE ... FROM cte ... RETURNING`의 반환 순서가 CTE의 `ORDER BY`를 따르는지는 확인하지 않았다. 구현할 때는 SQLite와 마찬가지로 애플리케이션에서 정렬하는 편이 안전하다.
