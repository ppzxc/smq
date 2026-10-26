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

기본 내구성(`POWER_LOSS`)은 쓰기 트랜잭션마다 fsync를 한다. 그래서 메시지를 하나씩 발행하고 청크 크기 1로 꺼내 ACK하면 처리량이 크게 떨어진다. 아래는 베어메탈 서버(RAID 컨트롤러의 배터리 백업 쓰기 캐시가 있는 HDD, xfs)에서 잰 1프로세스, 빈 큐의 값(msg/s)이다. 발행·꺼내기·ACK를 반복하는 조건이고, "묶음"은 발행 배치와 청크를 같은 크기로 키운 경우다(256B 100건, 2KB 300건, 4KB 400건, 6KB 500건).

| 메시지 크기 | POWER_LOSS 단건 | POWER_LOSS 묶음 | PROCESS_CRASH 단건 | PROCESS_CRASH 묶음 |
|---|---|---|---|---|
| 256B | 472 | 15,600 (33배) | 1,222 | 36,240 (30배) |
| 2KB | 473 | 9,789 (21배) | 1,330 | 11,404 (9배) |
| 4KB | 453 | 10,277 (23배) | 1,030 | 10,117 (10배) |
| 6KB | 445 | 5,743 (13배) | 937 | 4,888 (5배) |

`POWER_LOSS`의 단건은 메시지 크기와 상관없이 약 450~470 msg/s다. 건당 fsync가 비용을 정하기 때문이다. 가능하면 발행을 묶고 청크를 크게 꺼낸다.

절대값은 저장 장치와 캐시에 크게 좌우된다. 쓰기 캐시가 없는 디스크나 가상 디스크에서는 `POWER_LOSS`가 훨씬 느리다(WSL2에서는 256B 단건 29 msg/s, 묶음 2,999 msg/s). 기준선과 측정 환경은 `benchmark/README.md`를 본다.
