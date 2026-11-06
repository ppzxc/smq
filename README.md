# smq

애플리케이션 프로세스 안에서 동작하는 Java 25 내구성 메시지 큐. SQLite 파일 하나를 저장소로 쓰고, 메시지를 청크 단위로 꺼내 ACK/NACK하며, 실패한 메시지는 재시도한다. 별도 브로커 서버 없이 라이브러리 의존성 하나로 끝난다.

> **상태**: Maven Central에 `0.0.1` 공개. 0.x 동안은 API 호환을 약속하지 않는다.

## 개요

### 언제 쓰나

- 브로커(RabbitMQ, Kafka 등)를 따로 운영하지 않고, 프로세스가 죽거나 재시작해도 메시지를 잃지 않는 작업 큐가 필요할 때
- 처리에 실패하면 다시 시도하고, 계속 실패하는 메시지는 따로 모아 두고 싶을 때
- 한 애플리케이션(한 JVM) 안에서 SQLite 파일 하나를 저장소로 쓰는 로컬 큐가 필요할 때. 여러 프로세스가 같은 파일을 공유하는 용도는 아니다([ADR 0012](docs/adr/0012-single-jvm-only-enforced-by-lock-file-and-exclusive-mode.md))

### 특징

- **내구성**: 발행과 정산이 성공을 반환하면 그 결과는 애플리케이션이 죽어도 남는다(기본 `Durability.PROCESS_CRASH`). 전원·OS 장애까지 견뎌야 하면 `Durability.POWER_LOSS`를 고른다.
- **청크 단위 소비**: 한 번에 여러 건을 꺼내 하나의 리스(기한부 점유)로 묶고, 메시지마다 ACK/NACK/reject를 고른다. 정산은 트랜잭션 한 번으로 반영된다.
- **at-least-once 전달**: 리스 기한 안에 정산하지 않으면 메시지를 다시 꺼낼 수 있다. 중복 전달은 일어날 수 있으니 처리는 멱등하게 만든다.
- **재시도와 dead letter**: 지연 NACK, 최대 전달 횟수, dead letter 큐를 지원한다.
- **지연 발행**: 메시지를 지정한 시간이 지난 뒤에야 꺼낼 수 있게 한다.
- **FIFO 순서**: 큐 안에서 메시지 id 순서로 꺼낸다. 단일 활성 컨슈머 큐는 한 번에 한 청크만 리스된다.
- **한 JVM 전용**: SQLite 파일은 한 JVM만 연다. 같은 파일을 두 번째로 열면(다른 프로세스, 같은 JVM의 중복 열기, 외부 SQLite 도구가 쥔 경우) 기다리지 않고 `StorageLockedException`이다.
- **교체 가능한 저장소**: SQLite와 인메모리 저장소를 제공한다. 같은 공개 API로 쓴다.

### 하지 않는 것

smq는 "청크로 꺼내 ACK/NACK하는 재시도 가능한 내구성 큐"까지만 한다. exchange와 라우팅, 메시지 TTL, priority, purge, 큐 길이 제한, 구독(push) 헬퍼, 사용자 트랜잭션, 다른 호스트 사이의 공유는 v1 범위 밖이다. 목록과 이유는 [추후 기능 요구사항](docs/future-requirements.md)에 있다. 프로듀서와 컨슈머를 식별하지도 않는다.

## 빠른 시작

JDK 25가 필요하다. Maven Central의 `io.github.ppzxc:smq`를 의존성으로 건다.

Gradle(Kotlin DSL):

```kotlin
dependencies {
    implementation("io.github.ppzxc:smq:0.0.1")
}
```

Maven:

```xml
<dependency>
    <groupId>io.github.ppzxc</groupId>
    <artifactId>smq</artifactId>
    <version>0.0.1</version>
</dependency>
```

SQLite 저장소가 쓰는 sqlite-jdbc는 네이티브 라이브러리를 읽으므로 JVM 옵션 `--enable-native-access=ALL-UNNAMED`가 필요하다. 모듈 경로에서는 `--enable-native-access=org.xerial.sqlitejdbc`다.

```java
import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.ppzxc.smq.*;
import java.nio.file.Path;
import java.time.Duration;

try (Smq smq = Smq.sqlite(Path.of("smq.db")).open()) {   // 또는 Smq.inMemory().open()
    Queue q = smq.declare("orders", QueueOptions.defaults());
    q.publish(OutgoingMessage.of("hello".getBytes(UTF_8)));

    Chunk chunk = q.pull(100, Duration.ofSeconds(5));
    for (Message m : chunk.messages()) {
        System.out.println(new String(m.payload(), UTF_8));   // 처리는 멱등하게 만든다
    }
    q.settle(chunk.ackAll());
}
```

## 핵심 개념

전체 정의는 [GLOSSARY.md](GLOSSARY.md)에 있다. 흐름을 이해하는 데 필요한 개념만 추린다.

- **큐**는 이름으로 식별되는 FIFO 보관소다. `declare`로 존재하게 한다.
- **청크**는 `pull`로 한 번에 꺼낸 메시지 묶음이고, 묶음 전체가 **리스** 하나를 공유한다. 리스는 기한부 점유라서, 기한 안에 정산하지 않으면 메시지를 다시 꺼낼 수 있다.
- **정산**은 리스 중인 메시지에 ACK(지움), NACK(돌려줌, 지연 가능), reject(dead letter 큐로 보냄)를 적용한다. 서로 다른 결과를 섞어도 한 번에 전부 반영된다.
- 이미 다른 리스로 넘어갔거나 정산된 메시지에 대한 정산은 예외가 아니라 `Outcome.STALE` 결과로 돌아온다.
- 메시지 상태는 **준비**(지금 꺼낼 수 있음), **리스 중**, **지연됨**(지정 시각 전)이고, `queue.counts()`가 세 값을 돌려준다. 이 값은 기록된 상태 전환을 따라 센 근사값이라서 락 없이 큐 깊이와 무관한 비용으로 읽힌다. 기한이 지난 리스와 끝난 지연은 다음에 꺼낼 때 반영된다([ADR 0018](docs/adr/0018-counts-is-an-approximation-from-recorded-transitions.md)).

사용법의 전체 레퍼런스(열기, 큐 선언, 발행, 꺼내기, 정산, 리스 연장, dead letter, 예외)는 [docs/usage.md](docs/usage.md)에 있다.

## 내구성과 동시성

- 기본 내구성은 `Durability.PROCESS_CRASH`다. 애플리케이션 프로세스가 죽어도 성공한 발행과 정산은 남는다. **전원 장애나 OS 크래시 때는 마지막 커밋 몇 개를 잃을 수 있다**(발행한 메시지가 사라지거나 ACK한 메시지가 되살아난다). 기본값은 smq가 스스로 지킬 수 있는 장애까지만 보장한다.
- 전원·OS 장애까지 견뎌야 하면 `.durability(Durability.POWER_LOSS)`를 명시한다. 커밋마다 fsync하므로 처리량이 낮아지고(아래 표), 저장 장치가 fsync를 지킬 때만 보장이 성립한다([ADR 0014](docs/adr/0014-default-durability-survives-process-crash.md)).
- SQLite 파일은 한 JVM만 열 수 있다. DB 옆의 락 파일(`<DB 파일명>.lock`)과 `locking_mode=EXCLUSIVE`로 강제하며, 둘째 열기는 `StorageLockedException`이다. 같은 JVM의 여러 컨슈머에게는 리스가 같은 메시지를 동시에 주지 않는다.
- `Smq`, `Queue`, `Chunk`, `Settlement`는 여러 스레드에서 써도 된다.

상세는 [docs/usage.md](docs/usage.md#내구성과-동시성)를 본다.

## 실행 요건과 지원 플랫폼

- **지원 플랫폼**: Linux와 Windows다. 둘 다 DB 파일을 로컬 파일시스템(Linux 로컬 FS, Windows 로컬 NTFS)에 둔다. Windows는 `./gradlew check`(CI)로 검증했고, NTFS에서도 둘째 열기가 거부된다.
- **DB 파일 위치**: SMB 공유, 네트워크 드라이브, NFS, OneDrive 같은 동기화 폴더, ReFS, WSL의 `/mnt/c`는 지원하지 않는다.
- **Windows 성능**: Linux 기준 환경보다 처리량이 낮다. 특히 `POWER_LOSS` 단건은 fsync 비용에 묶여 약 80~90 msg/s다. 배치로 발행하면 격차가 크게 줄어든다. 근거는 [ADR 0008](docs/adr/0008-windows-has-no-absolute-throughput-floor.md)이다.
- **인메모리 저장소**: `Smq.inMemory()`는 한 프로세스 안에서만 쓴다.

## 성능: 내구성 수준과 배치

두 내구성 수준의 처리량(msg/s)이다. 발행·꺼내기·ACK를 반복하는 조건이고, "단건"은 한 건씩, "묶음"은 발행 배치와 청크를 키운 경우다(256B 100건, 2KB 300건, 4KB 400건, 6KB 500건). 베어메탈 서버(배터리 백업 쓰기 캐시가 있는 HDD, xfs)에서 1회 잰 값이다.

| 메시지 크기 | `PROCESS_CRASH`(기본) 단건 | `PROCESS_CRASH`(기본) 묶음 | `POWER_LOSS` 단건 | `POWER_LOSS` 묶음 |
|---|---|---|---|---|
| 256B | 2,276 | 49,925 | 648 | 16,836 |
| 2KB | 2,152 | 15,530 | 685 | 9,806 |
| 4KB | 2,535 | 13,695 | 595 | 9,006 |
| 6KB | 2,580 | 6,842 | 567 | 5,734 |

`POWER_LOSS`는 쓰기 트랜잭션마다 fsync를 하므로 저장 장치의 fsync 속도에 묶인다. 이 서버는 쓰기 캐시 덕분에 fsync가 빠르다. 쓰기 캐시가 없는 디스크나 가상 디스크에서는 `POWER_LOSS` 단건이 수십 msg/s까지 떨어진다(WSL2에서 256B 단건 29~67 msg/s). 어느 수준이든 가능하면 발행을 묶고 청크를 크게 꺼낸다. 측정 환경은 [docs/benchmark.md](docs/benchmark.md#성능-참고-배치-없이-쓰면-느리다)에 있다.

## 빌드와 테스트

```bash
./gradlew build            # 컴파일, 테스트, 문서 jar 검증
./gradlew test             # 테스트만
./gradlew benchmarkSmoke   # 벤치 하네스가 도는지만 확인하는 짧은 실행
```

벤치마크 절차는 [docs/benchmark.md](docs/benchmark.md), 기준선은 [benchmark/README.md](benchmark/README.md)를 본다.

## 문서 지도

| 문서 | 내용 |
|---|---|
| [docs/usage.md](docs/usage.md) | 사용법 레퍼런스(발행, 꺼내기, 정산, 예외 등) |
| [GLOSSARY.md](GLOSSARY.md) | 도메인 용어 정의. 코드와 문서가 이 용어를 쓴다. |
| [docs/adr/](docs/adr) | 설계 결정 기록(0001~0018) |
| [docs/future-requirements.md](docs/future-requirements.md) | v1에서 뺀 기능 목록 |
| [docs/release.md](docs/release.md), [CHANGELOG.md](CHANGELOG.md) | 릴리스 런북, 변경 이력 |
| [docs/benchmark.md](docs/benchmark.md), [benchmark/README.md](benchmark/README.md) | 벤치 하네스, 성능 수치, 기준선 |
| [docs/research/](docs/research) | RabbitMQ 핵심 기능, SQLite 동시성 등 조사 자료(다중 프로세스 조사는 ADR 0012 이전의 기록) |

## 라이선스

[MIT License](LICENSE)
