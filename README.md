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
