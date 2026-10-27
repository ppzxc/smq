# smq

애플리케이션 프로세스 안에서 동작하는 Java 25 내구성 메시지 큐. SQLite 파일 하나를 저장소로 쓰고, 메시지를 청크 단위로 꺼내 ACK/NACK하며, 실패한 메시지는 재시도한다. 별도 브로커 서버 없이 라이브러리 의존성 하나로 끝난다.

## 개요

### 언제 쓰나

- 브로커(RabbitMQ, Kafka 등)를 따로 운영하지 않고, 프로세스가 죽거나 재시작해도 메시지를 잃지 않는 작업 큐가 필요할 때
- 처리에 실패하면 다시 시도하고, 계속 실패하는 메시지는 따로 모아 두고 싶을 때
- 같은 호스트의 여러 프로세스가 SQLite 파일 하나를 공유해 큐를 나눠 소비하고 싶을 때

### 특징

- **내구성**: 발행과 정산이 성공을 반환하면 그 결과는 남는다. 기본은 전원 장애까지 견딘다(`Durability.POWER_LOSS`).
- **청크 단위 소비**: 한 번에 여러 건을 꺼내 하나의 리스(기한부 점유)로 묶고, 메시지마다 ACK/NACK/reject를 고른다. 정산은 트랜잭션 한 번으로 반영된다.
- **at-least-once 전달**: 리스 기한 안에 정산하지 않으면 메시지를 다시 꺼낼 수 있다. 중복 전달은 일어날 수 있으니 처리는 멱등하게 만든다.
- **재시도와 dead letter**: 지연 NACK, 최대 전달 횟수, dead letter 큐를 지원한다.
- **지연 발행**: 메시지를 지정한 시간이 지난 뒤에야 꺼낼 수 있게 한다.
- **FIFO 순서**: 큐 안에서 메시지 id 순서로 꺼낸다. 단일 활성 컨슈머 큐는 한 번에 한 청크만 리스된다.
- **다중 프로세스**: SQLite 저장소는 같은 파일을 여러 프로세스가 공유할 수 있다.
- **교체 가능한 저장소**: SQLite와 인메모리 저장소를 제공한다. 같은 공개 API로 쓴다.

### 하지 않는 것

smq는 "청크로 꺼내 ACK/NACK하는 재시도 가능한 내구성 큐"까지만 한다. exchange와 라우팅, 메시지 TTL, priority, purge, 큐 길이 제한, 구독(push) 헬퍼, 사용자 트랜잭션, 다른 호스트 사이의 공유는 v1 범위 밖이다. 목록과 이유는 [추후 기능 요구사항](docs/future-requirements.md)에 있다. 프로듀서와 컨슈머를 식별하지도 않는다.

## 빠른 시작

아직 Maven Central에 배포하지 않았다. 소스를 받아 로컬 저장소에 설치해 쓴다. 좌표는 `io.github.ppzxc:smq:0.1.0-SNAPSHOT`이고, JDK 25가 필요하다.

```bash
./gradlew publishToMavenLocal
```

```java
try (Smq smq = Smq.sqlite(Path.of("smq.db")).open()) {   // 또는 Smq.inMemory().open()
    Queue q = smq.declare("orders", QueueOptions.defaults());
    q.publish(OutgoingMessage.of("hello".getBytes(UTF_8)));
    Chunk chunk = q.pull(100, Duration.ofSeconds(5));
    // ... 처리 ...
    q.settle(chunk.ackAll());
}
```

실행할 때는 아래 [실행 요건](#실행-요건과-지원-플랫폼)의 JVM 옵션이 필요하다.

## 핵심 개념

정확한 정의는 [GLOSSARY.md](GLOSSARY.md)에 있다.

| 개념 | 뜻 |
|---|---|
| 큐(Queue) | 이름으로 식별되는 FIFO 보관소. `declare`로 존재하게 한다. |
| 메시지(Message) | payload(byte 배열), 문자열 헤더, 시스템 필드(id, 전달 횟수, 발행 시각). 메시지 id는 큐 안에서만 유일하다. |
| 발행 배치(Publish batch) | 한 번에 발행하는 메시지 묶음. 전부 발행되거나 하나도 발행되지 않는다. |
| 청크(Chunk) | `pull`로 한 번에 꺼낸 메시지 묶음. 묶음 전체가 리스 하나를 공유한다. |
| 리스(Lease) | 청크에 대한 기한부 점유. 기한 안에 정산하지 않으면 다시 꺼낼 수 있게 된다. |
| 정산(Settle) | 리스 중인 메시지에 ACK, NACK, reject를 적용한다. 한 번의 정산에 서로 다른 결과를 섞을 수 있고 전부 함께 반영된다. |
| ACK | 처리를 끝낸 메시지를 큐에서 지운다. |
| NACK | 메시지를 다시 꺼낼 수 있게 돌려준다. 지연을 주면 그 시간이 지난 뒤에 돌아온다. |
| reject | 재시도해도 소용없는 메시지를 전달 횟수와 상관없이 dead letter 큐로 보낸다. |
| 낡은 연산(Stale) | 이미 다른 리스로 넘어갔거나 이미 정산된 메시지에 대한 정산·연장. 예외가 아니라 `Outcome.STALE` 결과로 돌아온다. |

메시지 상태는 세 가지다. **준비(ready)**는 지금 꺼낼 수 있고, **리스 중(leased)**은 어떤 청크에 묶여 있으며, **지연됨(delayed)**은 지정한 시각 전이라 꺼낼 수 없다. `queue.counts()`가 세 값을 돌려준다.

## 사용법

### 열기와 저장소 선택

```java
// SQLite: 파일 하나. 없으면 만들고, 있으면 열면서 스키마를 필요 시 업그레이드한다.
Smq smq = Smq.sqlite(Path.of("smq.db"))
        .durability(Durability.POWER_LOSS)      // 기본값. 생략 가능
        .leaseDuration(Duration.ofSeconds(30))  // 기본값. 생략 가능
        .open();

// 인메모리: 프로세스 안에서만, 종료하면 사라진다. 테스트에 알맞다.
Smq mem = Smq.inMemory().open();
```

`Smq`는 스레드 안전하다. 닫을 때 정산하지 않은 리스는 그대로 두고, 기한이 지나면 다시 꺼낼 수 있게 된다(프로세스가 죽은 경우와 같다). 테스트에서는 `.clock(InstantSource)`로 시계를 주입할 수 있다.

### 큐 선언

```java
smq.declare("orders.dlq", QueueOptions.defaults());
Queue orders = smq.declare("orders",
        QueueOptions.defaults().maxDeliveries(5, "orders.dlq"));
```

- 같은 옵션으로 다시 선언하면 아무 일도 없고, 옵션이 다르면 `QueueDeclarationConflictException`이다. 그래서 시작할 때마다 선언해도 된다.
- `maxDeliveries(limit, dlq)`: 전달 횟수를 `limit`번 다 쓴 메시지를 dead letter 큐로 옮긴다. dead letter 큐는 먼저 선언돼 있어야 한다. 기본은 **무제한**이다([ADR 0001](docs/adr/0001-no-default-delivery-limit.md)).
- `deadLetterQueue(dlq)`: 한도 없이 reject한 메시지만 받을 dead letter 큐를 지정한다.
- `singleActiveConsumer(true)`: 한 번에 한 청크만 리스되게 한다. 순서가 필요한 소비에 쓴다.
- `smq.queue(name)`으로 선언된 큐를 가져오고(없으면 `QueueNotFoundException`), `smq.delete(name)`으로 큐를 메시지째 지운다(다른 큐의 dead letter 큐로 쓰이는 중이면 `QueueInUseException`).

### 발행

```java
MessageId id = orders.publish(
        OutgoingMessage.of("hello".getBytes(UTF_8))
                .withHeaders(Map.of("type", "created")));

// 배치는 전부 발행되거나 하나도 발행되지 않는다. id는 입력 순서대로 돌아온다.
List<MessageId> ids = orders.publish(List.of(
        OutgoingMessage.of("a".getBytes(UTF_8)),
        OutgoingMessage.of("b".getBytes(UTF_8)).withDelay(Duration.ofSeconds(10))));  // 10초 뒤에 꺼낼 수 있음
```

발행은 성능을 위해 가능하면 배치로 한다([성능 참고](#성능-참고-배치-없이-쓰면-느리다)).

### 꺼내기

```java
Chunk chunk = orders.pull(100, Duration.ofSeconds(5));
if (chunk.isEmpty()) { /* 5초를 기다렸지만 메시지가 없었다 */ }
for (Message m : chunk.messages()) {
    // m.id(), m.payload(), m.headers(), m.deliveryCount(), m.publishedAt()
}
```

- 준비 상태의 메시지가 하나라도 있으면 바로, 없으면 `wait`까지 기다려 최대 `max`건을 id 순서로 돌려준다. 기다리지 않으려면 `Duration.ZERO`를 준다.
- 기다리는 동안 인터럽트되면 `InterruptedException`이다.
- 리스 시간은 `pull(max, wait, leaseDuration)`으로 호출마다 바꿀 수 있다. 기본은 빌더의 `leaseDuration`(30초)이다.
- 꺼낼 때마다 메시지의 전달 횟수가 1 늘고, 리스는 새로 만들어진다.

### 정산

```java
// 전부 ACK
orders.settle(chunk.ackAll());

// 메시지마다 다른 결과
Settlement.Builder s = chunk.settle();
for (Message m : chunk.messages()) {
    try {
        handle(m);
        s.ack(m.id());
    } catch (TransientException e) {
        s.nack(m.id(), Duration.ofSeconds(30));   // 30초 뒤에 다시 꺼낼 수 있게
    } catch (PoisonException e) {
        s.reject(m.id());                          // dead letter 큐로 바로 이동
    }
}
Outcomes outcomes = orders.settle(s.build());
if (!outcomes.allOk()) {
    List<MessageId> stale = outcomes.stale();      // 리스가 만료돼 다른 곳으로 넘어간 메시지
}
```

- `Chunk`는 정산 상태를 기억하지 않는 불변 값이다. `chunk.settle()...build()`로 `Settlement` 값을 만들고 `queue.settle(...)`로 반영한다. 정산 한 번이 저장소 트랜잭션 한 번이다([ADR 0003](docs/adr/0003-settlement-is-an-immutable-value.md)).
- 지연 없는 `nack(id)`는 메시지를 자기 id 자리로, 즉 맨 앞으로 돌려보낸다.
- `ackRest()`와 `nackRest(delay)`는 그 빌더에 아직 담지 않은 나머지를 한꺼번에 처리한다. "나머지"는 한 `Settlement` 값 안에서만 계산되므로, 정산을 나눠서 한 뒤 `ackRest()`를 쓰면 앞서 정산한 메시지가 `STALE`로 돌아온다.
- 같은 `Settlement`를 다시 반영해도 안전하다. 이미 반영됐다면 `STALE`이 돌아온다.
- dead letter 큐가 없는 큐에서 reject하면 `NoDeadLetterQueueException`이고, 그 정산 전체가 반영되지 않는다.

### 리스 연장

처리가 길어져 리스 기한을 넘길 것 같으면 연장한다. 기한은 "지금부터 `leaseDuration` 뒤"로 바뀐다.

```java
Extension ext = orders.extend(chunk, Duration.ofSeconds(60));
if (!ext.outcomes().allOk()) { /* 일부는 이미 다른 리스로 넘어갔거나 정산됐다 */ }
```

### 재시도와 dead letter

메시지가 한 번 꺼내질 때마다 전달 횟수가 늘어난다. ACK도, NACK도, 리스 만료도 한 번의 전달이다. `maxDeliveries`를 다 쓴 메시지와 reject된 메시지는 원래 큐에서 지워지고 dead letter 큐에 **새 메시지**로 들어간다. 그래서 메시지 id와 전달 횟수가 새로 시작된다. 옮겨진 메시지에는 다음 헤더가 붙는다.

| 헤더 | 값 |
|---|---|
| `smq-original-queue` | 원래 큐 이름 |
| `smq-original-message-id` | 원래 메시지 id |
| `smq-last-delivery-count` | 옮겨질 때까지의 전달 횟수 |
| `smq-moved-at` | 옮긴 시각 |
| `smq-reason` | `rejected` 또는 `max-deliveries` |

`smq-` 접두사는 시스템 예약이라 같은 이름의 사용자 헤더는 시스템 값으로 덮어써진다. dead letter 큐는 그 자체로 평범한 큐라서 같은 방법으로 꺼내 확인한다.

### 예외

모두 `SmqException`(unchecked)의 하위 클래스다. 인자 오류는 `IllegalArgumentException`이다.

| 예외 | 언제 |
|---|---|
| `QueueNotFoundException` | 선언하지 않은 큐를 가져왔다. |
| `QueueDeclarationConflictException` | 이미 있는 큐를 다른 옵션으로 선언했다. |
| `QueueInUseException` | 다른 큐가 dead letter 큐로 쓰는 큐를 지우려 했다. |
| `NoDeadLetterQueueException` | dead letter 큐가 없는 큐에서 reject했다. |
| `IncompatibleStorageException` | 파일이 smq 파일이 아니거나 스키마 버전이 맞지 않는다. |
| `SmqStorageException` | 저장소 연산의 효력을 알 수 없다. 반영됐을 수도, 안 됐을 수도 있다. |

`SmqStorageException` 뒤에는 발행을 다시 해도 된다(앞선 발행이 반영됐다면 중복되지만 at-least-once 범위 안이다). 정산은 같은 `Settlement` 값으로 다시 하면 된다.

## 내구성과 동시성

- **내구성 수준**: SQLite 저장소의 `Durability`는 두 가지다. `POWER_LOSS`(기본)는 커밋마다 fsync해서 전원 장애나 OS 멈춤 뒤에도 성공한 발행·정산이 남는다. `PROCESS_CRASH`는 프로세스가 죽어도 남지만 전원 장애 때는 마지막 커밋 몇 개를 잃을 수 있고, 대신 처리량이 높다([ADR 0005](docs/adr/0005-default-durability-survives-power-loss.md)). 인메모리 저장소에는 해당 없다.
- **다중 프로세스**: 여러 프로세스가 같은 SQLite 파일을 열어 큐를 공유할 수 있다. 쓰기 경합(`SQLITE_BUSY`)은 내부에서 트랜잭션 단위로 다시 시도한다. 컨슈머를 서로 구별하지 않으며, 리스가 같은 메시지를 두 컨슈머에게 동시에 주지 않는다.
- **스키마 업그레이드**: 파일을 열 때 스키마 버전이 낮으면 자동으로 올린다. 업그레이드 뒤에도 남아 있는 옛 버전 프로세스는 다음 트랜잭션에서 예외로 멈춘다. 이때 그 프로세스가 쥔 리스는 만료되어 재전달되므로 전달이 유실되지 않는다([ADR 0004](docs/adr/0004-schema-auto-upgrade-with-per-transaction-version-check.md)).
- **스레드 안전**: `Smq`와 `Queue`, 불변 값인 `Chunk`와 `Settlement`는 여러 스레드에서 써도 된다.

## 실행 요건과 지원 플랫폼

- **네이티브 접근 허용**: SQLite 저장소가 쓰는 sqlite-jdbc는 네이티브 라이브러리를 읽는다. JVM 옵션으로
  `--enable-native-access=ALL-UNNAMED`를 준다. 모듈 경로에서는 `--enable-native-access=org.xerial.sqlitejdbc`다.
- **지원 플랫폼**: Linux와 Windows다. 둘 다 DB 파일을 로컬 파일시스템(Linux 로컬 FS, Windows 로컬 NTFS)에 둔다. Windows는 `./gradlew check`(CI)와 다중 프로세스 안전성을 검증했다.
- **DB 파일 위치**: SMB 공유, 네트워크 드라이브, NFS, OneDrive 같은 동기화 폴더, ReFS, WSL의 `/mnt/c`는 지원하지 않는다.
- **Windows 성능**: Linux 기준 환경보다 처리량이 낮다. 특히 `POWER_LOSS` 단건은 fsync 비용에 묶여 약 80~90 msg/s다. 배치로 발행하면 격차가 크게 줄어든다. 근거는 [ADR 0008](docs/adr/0008-windows-has-no-absolute-throughput-floor.md)이다.
- **인메모리 저장소**: `Smq.inMemory()`는 한 프로세스 안에서만 쓴다. 다중 프로세스는 지원하지 않는다.

## 성능 참고: 배치 없이 쓰면 느리다

기본 내구성(`POWER_LOSS`)은 쓰기 트랜잭션마다 fsync를 한다. 그래서 메시지를 하나씩 발행하고 청크 크기 1로 꺼내 ACK하면 처리량이 크게 떨어진다. 아래는 베어메탈 서버(RAID 컨트롤러의 배터리 백업 쓰기 캐시가 있는 HDD, xfs)에서 잰 1프로세스, 빈 큐의 값(msg/s)이다. 발행·꺼내기·ACK를 반복하는 조건이고, "묶음"은 발행 배치와 청크를 같은 크기로 키운 경우다(256B 100건, 2KB 300건, 4KB 400건, 6KB 500건).

| 메시지 크기 | POWER_LOSS 단건 | POWER_LOSS 묶음 | PROCESS_CRASH 단건 | PROCESS_CRASH 묶음 |
|---|---|---|---|---|
| 256B | 722 | 16,612 (23배) | 1,805 | 36,789 (20배) |
| 2KB | 719 | 9,797 (14배) | 1,886 | 11,099 (6배) |
| 4KB | 632 | 10,521 (17배) | 1,322 | 10,535 (8배) |
| 6KB | 632 | 5,583 (9배) | 1,327 | 5,282 (4배) |

`POWER_LOSS`의 단건은 약 630~720 msg/s로, 쓰기 트랜잭션마다 fsync가 한 번씩 있어서 묶음보다 훨씬 낮다. 가능하면 발행을 묶고 청크를 크게 꺼낸다.

절대값은 저장 장치와 캐시에 크게 좌우된다. 쓰기 캐시가 없는 디스크나 가상 디스크에서는 `POWER_LOSS`가 훨씬 느리다(WSL2에서는 256B 단건 29 msg/s, 묶음 2,999 msg/s). 기준선과 측정 환경은 `benchmark/README.md`를 본다.

## 빌드와 테스트

JDK 25가 필요하다(Gradle toolchain이 맞춰 준다).

```bash
./gradlew build            # 컴파일, 테스트, 문서 jar 검증
./gradlew test             # 테스트만
./gradlew benchmarkSmoke   # 벤치 하네스가 도는지만 확인하는 짧은 실행
./gradlew benchmark        # 전체 벤치. 기준 환경에서 수동으로만 돌린다
```

벤치마크 하네스는 `benchmark` 소스셋에 있고 배포 jar에는 들어가지 않는다. 형식과 절차는 [docs/benchmark.md](docs/benchmark.md), 기준선은 [benchmark/README.md](benchmark/README.md)를 본다.

## 문서 지도

| 문서 | 내용 |
|---|---|
| [GLOSSARY.md](GLOSSARY.md) | 도메인 용어 정의. 코드와 문서가 이 용어를 쓴다. |
| [docs/adr/](docs/adr) | 설계 결정 기록(최대 전달 횟수 기본값, 저장소 SPI, 정산 값, 스키마 업그레이드, 기본 내구성) |
| [docs/future-requirements.md](docs/future-requirements.md) | v1에서 뺀 기능 목록 |
| [docs/benchmark.md](docs/benchmark.md), [benchmark/README.md](benchmark/README.md) | 벤치 하네스와 기준선 |
| [research/](research) | RabbitMQ 핵심 기능, SQLite 다중 프로세스 동시성 등 조사 자료 |

## 라이선스

[MIT License](LICENSE)
