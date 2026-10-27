# 사용법

smq 공개 API의 사용법 레퍼런스다. 개요와 빠른 시작은 [README](../README.md), 용어 정의는 [GLOSSARY.md](../GLOSSARY.md)에 있다.

## 열기와 저장소 선택

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

## 큐 선언

```java
smq.declare("orders.dlq", QueueOptions.defaults());
Queue orders = smq.declare("orders",
        QueueOptions.defaults().maxDeliveries(5, "orders.dlq"));
```

- 같은 옵션으로 다시 선언하면 아무 일도 없고, 옵션이 다르면 `QueueDeclarationConflictException`이다. 그래서 시작할 때마다 선언해도 된다.
- `maxDeliveries(limit, dlq)`: 전달 횟수를 `limit`번 다 쓴 메시지를 dead letter 큐로 옮긴다. dead letter 큐는 먼저 선언돼 있어야 한다. 기본은 **무제한**이다([ADR 0001](adr/0001-no-default-delivery-limit.md)).
- `deadLetterQueue(dlq)`: 한도 없이 reject한 메시지만 받을 dead letter 큐를 지정한다.
- `singleActiveConsumer(true)`: 한 번에 한 청크만 리스되게 한다. 순서가 필요한 소비에 쓴다.
- `smq.queue(name)`으로 선언된 큐를 가져오고(없으면 `QueueNotFoundException`), `smq.delete(name)`으로 큐를 메시지째 지운다(다른 큐의 dead letter 큐로 쓰이는 중이면 `QueueInUseException`).

## 발행

```java
MessageId id = orders.publish(
        OutgoingMessage.of("hello".getBytes(UTF_8))
                .withHeaders(Map.of("type", "created")));

// 배치는 전부 발행되거나 하나도 발행되지 않는다. id는 입력 순서대로 돌아온다.
List<MessageId> ids = orders.publish(List.of(
        OutgoingMessage.of("a".getBytes(UTF_8)),
        OutgoingMessage.of("b".getBytes(UTF_8)).withDelay(Duration.ofSeconds(10))));  // 10초 뒤에 꺼낼 수 있음
```

발행은 성능을 위해 가능하면 배치로 한다([성능 참고](../README.md#성능-단건은-느리다-배치로-쓴다)).

## 꺼내기

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

## 정산

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

- `Chunk`는 정산 상태를 기억하지 않는 불변 값이다. `chunk.settle()...build()`로 `Settlement` 값을 만들고 `queue.settle(...)`로 반영한다. 정산 한 번이 저장소 트랜잭션 한 번이다([ADR 0003](adr/0003-settlement-is-an-immutable-value.md)).
- 지연 없는 `nack(id)`는 메시지를 자기 id 자리로, 즉 맨 앞으로 돌려보낸다.
- `ackRest()`와 `nackRest(delay)`는 그 빌더에 아직 담지 않은 나머지를 한꺼번에 처리한다. "나머지"는 한 `Settlement` 값 안에서만 계산되므로, 정산을 나눠서 한 뒤 `ackRest()`를 쓰면 앞서 정산한 메시지가 `STALE`로 돌아온다.
- 같은 `Settlement`를 다시 반영해도 안전하다. 이미 반영됐다면 `STALE`이 돌아온다.
- dead letter 큐가 없는 큐에서 reject하면 `NoDeadLetterQueueException`이고, 그 정산 전체가 반영되지 않는다.

## 리스 연장

처리가 길어져 리스 기한을 넘길 것 같으면 연장한다. 기한은 "지금부터 `leaseDuration` 뒤"로 바뀐다.

```java
Extension ext = orders.extend(chunk, Duration.ofSeconds(60));
if (!ext.outcomes().allOk()) { /* 일부는 이미 다른 리스로 넘어갔거나 정산됐다 */ }
```

## 재시도와 dead letter

메시지가 한 번 꺼내질 때마다 전달 횟수가 늘어난다. ACK도, NACK도, 리스 만료도 한 번의 전달이다. `maxDeliveries`를 다 쓴 메시지와 reject된 메시지는 원래 큐에서 지워지고 dead letter 큐에 **새 메시지**로 들어간다. 그래서 메시지 id와 전달 횟수가 새로 시작된다. 옮겨진 메시지에는 다음 헤더가 붙는다.

| 헤더 | 값 |
|---|---|
| `smq-original-queue` | 원래 큐 이름 |
| `smq-original-message-id` | 원래 메시지 id |
| `smq-last-delivery-count` | 옮겨질 때까지의 전달 횟수 |
| `smq-moved-at` | 옮긴 시각 |
| `smq-reason` | `rejected` 또는 `max-deliveries` |

`smq-` 접두사는 시스템 예약이라 같은 이름의 사용자 헤더는 시스템 값으로 덮어써진다. dead letter 큐는 그 자체로 평범한 큐라서 같은 방법으로 꺼내 확인한다.

## 예외

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

- **내구성 수준**: SQLite 저장소의 `Durability`는 두 가지다. `POWER_LOSS`(기본)는 커밋마다 fsync해서 전원 장애나 OS 멈춤 뒤에도 성공한 발행·정산이 남는다. `PROCESS_CRASH`는 프로세스가 죽어도 남지만 전원 장애 때는 마지막 커밋 몇 개를 잃을 수 있고, 대신 처리량이 높다([ADR 0005](adr/0005-default-durability-survives-power-loss.md)). 인메모리 저장소에는 해당 없다.
- **다중 프로세스**: 여러 프로세스가 같은 SQLite 파일을 열어 큐를 공유할 수 있다. 쓰기 경합(`SQLITE_BUSY`)은 내부에서 트랜잭션 단위로 다시 시도한다. 컨슈머를 서로 구별하지 않으며, 리스가 같은 메시지를 두 컨슈머에게 동시에 주지 않는다.
- **스키마 업그레이드**: 파일을 열 때 스키마 버전이 낮으면 자동으로 올린다. 업그레이드 뒤에도 남아 있는 옛 버전 프로세스는 다음 트랜잭션에서 예외로 멈춘다. 이때 그 프로세스가 쥔 리스는 만료되어 재전달되므로 전달이 유실되지 않는다([ADR 0004](adr/0004-schema-auto-upgrade-with-per-transaction-version-check.md)).
- **스레드 안전**: `Smq`와 `Queue`, 불변 값인 `Chunk`와 `Settlement`는 여러 스레드에서 써도 된다.
