# smq

Java 25 임베디드 내구성 큐. SQLite 파일 하나를 저장소로 쓰는 청크 단위 ACK/NACK 재시도 큐다.

```java
try (Smq smq = Smq.sqlite(Path.of("smq.db")).open()) {   // 또는 Smq.inMemory().open()
    Queue q = smq.declare("orders", QueueOptions.defaults());
    q.publish(OutgoingMessage.of("hello".getBytes(UTF_8)));
    Chunk chunk = q.pull(100, Duration.ofSeconds(5));
    // ... 처리 ...
    q.settle(chunk.ackAll());
}
```

## 실행 요건

- **네이티브 접근 허용**: SQLite 저장소가 쓰는 sqlite-jdbc는 네이티브 라이브러리를 읽는다. JVM 옵션으로
  `--enable-native-access=ALL-UNNAMED`를 준다. 모듈 경로에서는 `--enable-native-access=org.xerial.sqlitejdbc`다.
- **DB 파일 위치**: Linux 로컬 파일시스템에 둔다. WSL의 `/mnt/c`, NFS, SMB는 지원하지 않는다.
- **인메모리 저장소**: `Smq.inMemory()`는 한 프로세스 안에서만 쓴다. 다중 프로세스는 지원하지 않는다.

## 성능 참고: 배치 없이 쓰면 느리다

기본 내구성(`POWER_LOSS`)은 쓰기 트랜잭션마다 fsync를 한다. 그래서 메시지를 하나씩 발행하고 청크 크기 1로 꺼내 ACK하면(P0) 처리량이 크게 떨어진다. WSL2 ext4에서 잰 값은 아래와 같다(256B, 1프로세스, 빈 큐).

| 조건 | POWER_LOSS | PROCESS_CRASH |
|---|---|---|
| P0: 청크 1 | 29 msg/s | 714 msg/s |
| P1: 발행 배치 100, 청크 100 | 2,999 msg/s | 11,314 msg/s |

청크 1은 같은 조건의 청크 100보다 `POWER_LOSS`에서 약 100배, `PROCESS_CRASH`에서 약 16배 느리다. 가능하면 발행을 묶고 청크를 크게 꺼낸다. 위 값은 WSL2 임시 측정이라 fsync 비용을 과소평가했을 수 있고, 기준 환경 측정으로 바뀔 수 있다. 자세한 내용은 `benchmark/README.md`를 본다.
