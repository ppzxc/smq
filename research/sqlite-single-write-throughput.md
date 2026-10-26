# SQLite 단건 쓰기 처리량(커밋당 fsync) 향상: 업계 표준과 선도 사례

- 조사일: 2026-10-26
- 관련 문서: [ADR 0005](../docs/adr/0005-default-durability-survives-power-loss.md), [ADR 0002](../docs/adr/0002-storage-spi-at-queue-operation-level.md), [sqlite-multiprocess-concurrency.md](sqlite-multiprocess-concurrency.md)
- 범위: 저장소 코드는 건드리지 않는다. 이 문서는 조사 결과만 담는다. 모든 주장 옆에 1차 출처를 붙였고, 확인하지 못한 것은 "확인 못함"으로 적었다.

## 요약(결론 5줄 이내)

1. WAL + `synchronous=FULL`에서 쓰기 트랜잭션 하나는 커밋마다 WAL을 한 번 sync한다. 체크포인트 때의 DB sync와 WAL 재시작 뒤 첫 쓰기의 헤더 sync가 가끔 추가될 뿐이다. `wal_autocheckpoint`, `journal_size_limit`, `mmap_size`, `cache_size`, `temp_store`, 체크포인트 모드는 커밋당 sync 횟수를 줄이지 못한다. 많아야 체크포인트가 분산되는 방식이 달라질 뿐이다.
2. 업계 표준 해법은 여러 쓰기를 한 트랜잭션으로 묶어 커밋 수 자체를 줄이는 것이다(SQLite FAQ가 같은 조언을 한다). 이 방법은 동시에 들어오는 호출자가 있을 때만 효과가 있다. 호출자 한 명이 순차로 단건을 호출하고 매번 성공 반환을 기다리면 커밋 수가 그대로여서 그룹 커밋은 효과가 없다(맞다).
3. 내구성(FULL)을 유지하고 공개 API와 SPI 단위를 바꾸지 않는 조건에서 "1프로세스 단일 호출자 순차 단건" 벤치를 소프트웨어만으로 의미 있게 올리는 방법은 찾지 못했다. 남는 변수는 커밋당 fsync 비용(저장 장치, 파일시스템, 마운트 옵션)과 커밋 사이의 CPU 오버헤드뿐이다.
4. `synchronous=NORMAL`은 단건 처리량을 크게 올리지만 ADR 0005와 충돌한다. WAL2, BEGIN CONCURRENT는 공식 문서상 fsync 횟수 감소를 주장하지 않고 별도 SQLite 빌드가 필요하다. F2FS batch atomic write는 롤백 저널 모드용으로 설명되어 있다.
5. 현실적으로 채택할 만한 것은 (가) 다중 호출자일 때 SPI 안쪽의 쓰기 합치기(리더가 SAVEPOINT로 연산을 격리해 한 번에 커밋)와 (나) 단건 벤치 대신 병렬 호출자 벤치를 별도 지표로 추가하는 것이다. 단건 450 msg/s 하한은 장치의 fsync 지연이 결정하므로, 하한을 올리려면 장치/파일시스템 기준선을 다시 측정해야 한다.

---

## 1. WAL + synchronous=FULL의 커밋당 fsync

### 1.1 확인된 동작

| 동작 | 빈도 | 출처 |
|---|---|---|
| 커밋 시 WAL 파일 sync | FULL이면 트랜잭션 커밋마다 1회, NORMAL이면 생략 | [wal.html](https://www.sqlite.org/wal.html): "Writers sync the WAL on every transaction commit if PRAGMA synchronous is set to FULL but omit this sync if PRAGMA synchronous is set to NORMAL." |
| 같은 내용, 다른 표현 | FULL이면 "각 트랜잭션 커밋 뒤 WAL 파일 sync 한 번 추가" | [pragma.html#pragma_synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous): "With synchronous=FULL in WAL mode, an additional sync operation of the WAL file happens after each transaction commit." |
| 체크포인트 때 WAL sync, DB sync | 체크포인트마다. WAL을 먼저 sync한 뒤 DB로 옮기고, WAL 전체를 옮겼을 때만 DB를 sync | [wal.html](https://www.sqlite.org/wal.html): "The WAL must be synced to persistent storage prior to moving content from the WAL into the database and the database file must be synced prior to resetting the WAL." 소스 [wal.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/wal.c): `walCheckpoint`에서 `sqlite3OsSync(pWal->pWalFd, ...)` 뒤 `sqlite3OsSync(pWal->pDbFd, ...)` |
| WAL 재시작 뒤 헤더 sync | 체크포인트가 WAL을 리셋하고 첫 쓰기가 WAL 앞부분을 덮어쓸 때 1회 | [pragma.html](https://www.sqlite.org/pragma.html#pragma_synchronous): "the WAL file header is synchronized when a WAL file begins to be reused after a checkpoint". 소스 [wal.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/wal.c) `walFrames()`: `if( pWal->syncHeader ){ rc = sqlite3OsSync(...)}`, 주석 "Sync the header (unless SQLITE_IOCAP_SEQUENTIAL is true or unless all syncing is turned off by PRAGMA synchronous=OFF)." |
| 커밋 sync 조건 | 트랜잭션을 끝내는 프레임 묶음이면서 FULL일 때만 | [wal.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/wal.c) 주석: "Padding and syncing only occur if this set of frames complete a transaction and if PRAGMA synchronous=FULL." |

정리하면 FULL에서 평상시 쓰기 트랜잭션 1개당 sync는 WAL 1회다. 체크포인트(기본 1000페이지마다)가 돌 때 WAL sync 1회와 DB sync 1회, 그리고 그 뒤 첫 쓰기에서 헤더 sync 1회가 얹힌다. 이 추가분은 "페이지 1000개마다"로 분산되므로 단건 트랜잭션이 작을수록 평균에서는 미미하다. 다만 한 트랜잭션이 몇 페이지를 쓰는지와 체크포인트 간격이 얼마나 되는지의 실측 분포는 이 조사에서 확인하지 못했다(확인 못함). 단건 트랜잭션 하나가 쓰는 페이지 수가 소수라는 점은 추정이다.

### 1.2 롤백 저널과의 대비 (참고)

롤백 저널 모드는 FULL에서 저널 sync 2회와 DB sync 1회가 필요하다([atomiccommit.html](https://www.sqlite.org/atomiccommit.html) 3.7, 3.10, 6.2절: "the rollback journal is flushed to disk twice", "Another flush must occur to make sure that all the database changes are written into nonvolatile storage"). WAL이 sync 횟수가 적다는 점은 [wal.html](https://www.sqlite.org/wal.html)에도 있다: "WAL uses many fewer fsync() operations". smq는 이미 WAL을 쓰므로 이 방향의 추가 개선 여지는 없다.

### 1.3 SQLITE_ENABLE_ATOMIC_WRITE, IOCAP, BATCH_ATOMIC

- `SQLITE_ENABLE_ATOMIC_WRITE`: 파일시스템이 원자적 쓰기를 보고하고 트랜잭션이 DB 한 페이지만 바꾸면 롤백 저널 없이 한 번의 페이지 쓰기로 커밋한다. 단, "few filesystems support this capability ... so this feature is disabled by default"([compile.html](https://www.sqlite.org/compile.html)). 이 설명은 저널을 쓰지 않는다는 얘기여서 롤백 저널 모드의 최적화로 읽힌다. WAL에 적용된다는 문장은 못 찾았다(확인 못함).
- `SQLITE_ENABLE_BATCH_ATOMIC_WRITE`: "As of SQLite version 3.21.0 ... only supported on F2FS", "avoids writing the rollback journal", "can make transactions over twice as fast"([compile.html](https://www.sqlite.org/compile.html)). `SQLITE_IOCAP_BATCH_ATOMIC` 정의는 [c_iocap_atomic.html](https://www.sqlite.org/c3ref/c_iocap_atomic.html). 위 두 문서 모두 롤백 저널을 없애는 최적화로 설명한다. pager.c 발췌에서도 "A pager is never in WRITER_DBMOD or WRITER_FINISHED state if the connection is open in WAL mode"라고 되어 있어([pager.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/pager.c)), WAL 모드에서 이 경로가 쓰인다는 근거는 없다. 단 pager.c는 앞부분 100KB만 읽었다(확인 못함: 호출 지점 전체).
- `SQLITE_IOCAP_SEQUENTIAL`: 보고되면 WAL 헤더 sync가 생략된다(위 wal.c). `SQLITE_IOCAP_POWERSAFE_OVERWRITE`는 WAL 섹터 패딩을 끈다([wal.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/wal.c) `sqlite3WalOpen`). 어느 쪽도 커밋당 WAL sync 1회를 없애지는 않는다.

### 1.4 내구성에 영향이 없는 튜닝이 "커밋당" fsync 수를 바꾸는가

| 설정 | 공식 설명 | 커밋당 fsync 횟수 | 비고 |
|---|---|---|---|
| `wal_autocheckpoint` | 기본 1000페이지, 0 이하면 끔. 자동 체크포인트는 모두 PASSIVE ([pragma.html](https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint)) | 아니오. 체크포인트 횟수만 바뀐다 | 값을 키우면 체크포인트 sync와 헤더 sync가 덜 자주 일어난다. 대신 WAL이 커지고 읽기가 느려진다 ([wal.html](https://www.sqlite.org/wal.html): "read performance deteriorates as the WAL file grows"). 끄면 누군가 직접 체크포인트해야 한다 |
| `wal_checkpoint` 모드 | PASSIVE, FULL, RESTART, TRUNCATE ([pragma.html](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint)) | 아니오 | FULL 이상은 쓰기를 막는다 |
| `journal_size_limit` | WAL/저널 파일을 체크포인트 뒤 남길 최대 크기, 기본 무제한 ([pragma.html](https://www.sqlite.org/pragma.html#pragma_journal_size_limit)) | 아니오 | 파일 크기를 유지하면 sync 비용이 달라질 가능성은 있으나, 공식 문서에서 그 효과를 확인하지 못했다(확인 못함) |
| `mmap_size` | 메모리 맵 I/O 상한 ([pragma.html](https://www.sqlite.org/pragma.html#pragma_mmap_size)) | 아니오 | 읽기 경로 |
| `cache_size`, `temp_store` | 캐시 크기, 임시 테이블 위치 | 아니오 | 읽기와 임시 테이블 경로. 큐 연산의 WAL sync 횟수와 무관 |
| `page_size` | DB 생성 시점에 정해짐 ([pragma.html](https://www.sqlite.org/pragma.html#pragma_page_size)) | 아니오 | 페이지 크기는 쓰는 바이트 양에 영향을 줄 뿐 sync 횟수와는 무관. 기존 DB에는 VACUUM이 필요하고 WAL 모드에서는 바로 적용되지 않는다 |
| `fullfsync`, `checkpoint_fullfsync` | macOS에서만 F_FULLFSYNC 사용 ([pragma.html](https://www.sqlite.org/pragma.html#pragma_fullfsync)) | 횟수 변화 없음 | macOS에서는 오히려 더 느려진다 |
| `SQLITE_FCNTL_CHUNK_SIZE`, `SIZE_HINT` | 파일을 청크 단위로 확장해 단편화를 줄이고 "improve performance on some systems"([c_fcntl](https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html)) | 횟수 변화 없음 | 파일 크기 변경 메타데이터 sync 비용을 줄일 수 있다는 것은 추정이다. 공식 문서에 그런 문장은 없다(확인 못함). JDBC로는 호출 불가 |

fdatasync에 관해서는 소스에 `# define fdatasync fsync`가 `#if !defined(fdatasync) && !HAVE_FDATASYNC`일 때만 적용되고 이유는 "We do not trust systems to provide a working fdatasync()."라고 되어 있다([os_unix.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/os_unix.c)). 또 `unixSync()`는 "If dataOnly==0 then both the file itself and its metadata (file size, access time, etc) are synced."라고 설명한다. 즉 빌드에 따라 fdatasync가 쓰일 수 있다. xerial sqlite-jdbc가 내장한 네이티브 SQLite가 `HAVE_FDATASYNC`로 빌드되었는지는 확인하지 못했다(확인 못함).

결론: 내구성에 영향이 없는 PRAGMA 튜닝은 "커밋마다 WAL sync 1회"를 줄이지 못한다. 줄이는 것은 체크포인트 쪽 sync 몇 개의 빈도뿐이고, 단건 벤치에서 효과가 보인다면 체크포인트가 가끔 끼는 구간의 꼬리 지연 정도다.

---

## 2. 단일 writer 큐와 그룹 커밋

### 2.1 원리

- SQLite 공식 FAQ: "SQLite will easily do 50,000 or more INSERT statements per second on an average desktop computer. But it will only do a few dozen transactions per second. Transaction speed is limited by the rotational speed of your disk drive." 그리고 여러 INSERT를 `BEGIN`...`COMMIT`으로 묶으라고 권한다([faq.html](https://www.sqlite.org/faq.html)). 2024-11-19 갱신 문구도 "batching operations in a single transaction improves performance dramatically"라는 핵심 조언은 유효하다고 한다. 회전 디스크 수치는 낡았다.
- WAL은 한 번에 한 writer만 허용한다: "since there is only one WAL file, there can only be one writer at a time"([wal.html](https://www.sqlite.org/wal.html)). 락 계층도 RESERVED는 하나만 활성화된다([lockingv3.html](https://www.sqlite.org/lockingv3.html)).
- SQLite 자체에 group commit(여러 호출자의 커밋을 하나의 fsync로 합치는 내부 기능)이 있다는 공식 문서는 찾지 못했다(확인 못함). 커밋 sync 조건은 위 wal.c 주석대로 "트랜잭션이 완결될 때 FULL이면 sync"로, 트랜잭션 단위다. 합치려면 호출자(애플리케이션)가 한 트랜잭션으로 묶어야 한다.

### 2.2 호출자별 오류 격리: SAVEPOINT

- 한 트랜잭션 안에서 연산별로 `SAVEPOINT`를 열고, 실패하면 `ROLLBACK TO`로 그 연산만 되돌릴 수 있다. `ROLLBACK TO`는 "does not cancel the transaction"이고, 커밋은 바깥쪽 트랜잭션에서 일어난다: "Content is not actually committed on the disk until the outermost transaction commits."([lang_savepoint.html](https://www.sqlite.org/lang_savepoint.html))
- 이미 `BEGIN` 안에 있으면 SAVEPOINT는 중첩된다. "Transactions created using BEGIN...COMMIT do not nest. For nested transactions, use the SAVEPOINT and RELEASE commands."([lang_transaction.html](https://www.sqlite.org/lang_transaction.html))
- 따라서 합쳐 쓰는 리더는 연산마다 `SAVEPOINT s; ...; RELEASE s;`로 감싸고, 연산이 예외를 던지면 `ROLLBACK TO s; RELEASE s;` 후 그 호출자에게만 오류를 돌려주면 된다. 마지막에 한 번 `COMMIT`한다. 커밋 자체가 실패(디스크 오류 등)하면 묶인 모든 호출자가 실패해야 한다는 점은 이 방식의 본질적 성질이다(원자성 단위가 커진다).
- 호출자는 커밋이 끝난 뒤에만 성공을 반환해야 ADR 0005를 지킨다. 이 대기는 "그룹 커밋에서 호출자가 커밋 완료를 기다리는" 일반 구조와 같다.

### 2.3 Expensify Bedrock

확인된 것:

- Bedrock은 SQLite 위의 분산 트랜잭션 프레임워크다([README](https://raw.githubusercontent.com/Expensify/Bedrock/main/README.md)).
- 소스에서 `BEGIN CONCURRENT`를 쓰고 `PRAGMA journal_mode = wal2`를 설정한다(`SQLite::beginTransaction`, `SQLite::commonConstructorInitialization`, [SQLite.cpp](https://raw.githubusercontent.com/Expensify/Bedrock/main/sqlitecluster/SQLite.cpp)). 커밋 순서를 보장하려고 `commitLock`을 쥔다: "We lock this here, so that we can guarantee the order in which commits show up in the database."(같은 파일, `SQLite::prepare`) 이는 SQLite 쪽 문서가 BEGIN CONCURRENT도 "still serializes COMMIT commands"라고 하는 것과 맞다([begin_concurrent.md](https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md)).
- 리더가 팔로어의 ACK를 기다리지 않고 먼저 커밋하는 비동기 복제 모델이다: "The leader does not wait for followers to acknowledge a transaction before committing it", "A cluster is therefore only as durable as the leader's ability to hand its commits off before it dies."([synchronization.md](https://raw.githubusercontent.com/Expensify/Bedrock/main/docs/synchronization.md))
- Expensify의 4M QPS 글은 읽기 전용 측정이다: "No writes are done during this test, only reads."([블로그](https://use.expensify.com/blog/scaling-sqlite-to-4m-qps-on-a-single-server)).

확인하지 못한 것:

- Bedrock이 "여러 호출자 커밋을 한 fsync로 묶는 group commit"을 한다는 근거는 소스에서 찾지 못했다. SQLite.cpp 검색에서 group commit 로직과 `PRAGMA synchronous` 설정이 모두 나오지 않았다(확인 못함. 파일 전체를 정독한 것은 아니다). 그러므로 "Bedrock = group commit 사례"라고 쓰면 안 된다. 확인된 것은 "BEGIN CONCURRENT + wal2 + 커밋 직렬화 락" 구조뿐이다.
- Bedrock이 단건 쓰기에서 얻은 TPS 수치도 1차 출처에서 확인하지 못했다(확인 못함).

### 2.4 그 밖의 선도 사례

| 사례 | 확인된 내용 | 출처 |
|---|---|---|
| Litestream | `synchronous=NORMAL`을 권장하며 "In WAL mode, fsync runs only when the WAL checkpoints" 취지. 대용량 쓰기 서버에서는 `wal_autocheckpoint=0`을 권장(체크포인트를 Litestream이 제어). 복제는 WAL 기반 | [litestream.io/tips](https://litestream.io/tips/) |
| LiteFS | 복제, 리스 기반 단일 primary 암시. 쓰기 성능과 내구성 보증 수치는 문서에 없음. pre-1.0이고 Fly.io가 지원하지 않는다고 명시 | [docs.fly.io/litefs](https://docs.fly.io/litefs) |
| Cloudflare D1 | DB 하나는 단일 스레드로 쿼리를 한 번에 하나씩 처리: "Each individual D1 database is inherently single-threaded, and processes queries one at a time." 처리량은 쿼리 시간에 반비례. `batch()`로 요청당 오버헤드를 한 번만 지불하라고 안내. 쓰기는 "durably persisted across several locations" | [limits](https://developers.cloudflare.com/d1/platform/limits/) |
| Turso | `BEGIN CONCURRENT`는 MVCC 모드(`PRAGMA journal_mode = mvcc`)에서 낙관적 동시 쓰기. 충돌하면 COMMIT 실패 후 재시도. fsync/synchronous 설명 없음 | [transactions](https://docs.turso.tech/sql-reference/statements/transactions.md) |
| libSQL | 확인 못함(공식 문서 직접 확인 안 함) | - |
| Rails 8 (Active Record sqlite3 adapter) | 기본 PRAGMA: `journal_mode=wal`, `synchronous=normal`, `mmap_size=134217728`(128MB), `journal_size_limit=67108864`(64MB), `cache_size=2000`, `foreign_keys=true`. 트랜잭션 모드 기본값 `default_transaction_mode: :immediate`. 소스는 동기화 정책 근거를 주석으로 설명하지 않는다 | [sqlite3_adapter.rb](https://raw.githubusercontent.com/rails/rails/main/activerecord/lib/active_record/connection_adapters/sqlite3_adapter.rb) |

관찰: 이 사례들이 커밋당 fsync를 줄이는 방식은 거의 모두 (1) `synchronous=NORMAL`로 내구성을 낮추거나(Litestream, Rails), (2) 복제로 내구성을 다른 곳에 두거나(Bedrock, LiteFS, D1), (3) 호출자가 배치로 묶는 것(D1 `batch()`, FAQ)이다. 로컬 디스크 전원 장애 내구성을 FULL로 유지하면서 단건 처리량을 올린 사례는 이 조사 범위에서 찾지 못했다.

---

## 3. BEGIN IMMEDIATE, 트랜잭션 작업량, prepared statement, user_version, 카운터

### 3.1 BEGIN IMMEDIATE

- "IMMEDIATE causes the database connection to start a new write immediately, without waiting for a write statement. The BEGIN IMMEDIATE might fail with SQLITE_BUSY if another write transaction is already active on another database connection."([lang_transaction.html](https://www.sqlite.org/lang_transaction.html))
- DEFERRED로 읽기 뒤 쓰기로 올리면 락 업그레이드 시점에 SQLITE_BUSY가 날 수 있다([lockingv3.html](https://www.sqlite.org/lockingv3.html)). 그래서 쓰기 의도가 확실한 트랜잭션은 IMMEDIATE가 표준이다. Rails도 기본을 immediate로 둔다(위 adapter 소스).
- fsync 횟수와의 관계: BEGIN IMMEDIATE 자체는 sync를 만들지 않는다. 커밋의 WAL sync 1회는 그대로다. 락을 일찍 잡으므로 다중 프로세스 경합 때의 BUSY 처리에 영향이 있을 뿐이다. (sync 미발생은 공식 문장이 아니라 위 wal.c 조건 "isCommit이고 FULL일 때만 sync"에서 도출한 추론이다.)

### 3.2 트랜잭션 내 작업량 줄이기

- WAL 커밋 비용은 대략 "WAL에 프레임 쓰기 + 커밋당 sync 1회"다([wal.html](https://www.sqlite.org/wal.html): "Write transactions are very fast since they only involve writing the content once ... and because the writes are all sequential"). 단건 트랜잭션에서 sync 지연이 지배적이면 트랜잭션 안의 SQL 작업량을 줄여도 효과가 작다. 수치로 어느 쪽이 지배적인지는 smq 기준선 프로파일로 확인해야 한다(이 조사에서는 확인 못함).
- 건드린 페이지가 적을수록 쓰는 WAL 프레임이 줄어 체크포인트가 덜 자주 돈다. 이것도 sync 횟수를 줄이는 것은 아니다.

### 3.3 prepared statement 재사용

- 공식 생명주기: prepare 1회 후 bind, step, `sqlite3_reset()`를 반복해 재사용한다([c3ref/stmt.html](https://www.sqlite.org/c3ref/stmt.html)). 효과는 SQL 컴파일 비용 절감이다. fsync 횟수에는 영향이 없다.
- xerial sqlite-jdbc에서 `PreparedStatement` 재사용 시 내부 캐시가 어떻게 되는지는 이 조사에서 확인하지 못했다(확인 못함).

### 3.4 `PRAGMA user_version` 읽기 비용

- 공식 설명은 "user_version ... integer at offset 60 in the database header"뿐이다([pragma.html](https://www.sqlite.org/pragma.html#pragma_user_version)). 읽기 비용이나 sync 여부에 대한 공식 문장은 없다(확인 못함).
- 이 값은 트랜잭션 안에서 읽는 읽기이므로 WAL sync를 일으키지 않는다. 이는 위 wal.c 조건에서 도출한 추론이다. 따라서 ADR 0004의 연산마다 버전 확인은 fsync 비용이 아니라 CPU와 페이지 1 조회 비용이다. 버전을 바꿔 쓰는 쪽은 업그레이드 때만 발생한다.
- 같은 연결 하나만 쓴다면 다른 프로세스가 스키마를 바꾼 경우만 감지하면 되는데, 이 비용을 줄이는 대안으로 `PRAGMA data_version`이 있다: "values returned by two invocations ... will be different if changes were committed to the database by any other connection in the interim."([pragma.html](https://www.sqlite.org/pragma.html#pragma_data_version)) 다만 이것은 "다른 연결이 커밋했다"는 신호이지 스키마 버전이 바뀌었다는 신호가 아니다. ADR 0004의 판단은 이 문서의 범위 밖이다.

### 3.5 단일 행 카운터 `UPDATE ... RETURNING` 경합

- 한 행을 모든 연산이 갱신하면 쓰기가 한 페이지에 모여, 직렬화된 writer에서는 문제가 안 되지만 BEGIN CONCURRENT에서는 페이지 충돌이 된다. 공식 문서: "The system uses optimistic page-level-locking"과 증가하는 rowid가 "all but ensures that all new rows are added to the same database page"([begin_concurrent.md](https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md)). 커밋 시 충돌하면 `SQLITE_BUSY_SNAPSHOT`이고 클라이언트는 `ROLLBACK`만 할 수 있다.
- 현재 smq 구조(연결 1개, ReentrantLock 직렬화)에서는 카운터 행 경합 자체가 없다. 경합은 다중 프로세스 또는 BEGIN CONCURRENT 도입 시에만 생긴다.

---

## 4. synchronous=NORMAL과 대안

### 4.1 NORMAL(WAL)이 전원 장애 때 잃는 것 (공식 문장)

- "WAL mode is always consistent with synchronous=NORMAL, but WAL mode does lose durability. A transaction committed in WAL mode with synchronous=NORMAL might roll back following a power loss or system crash."([pragma.html](https://www.sqlite.org/pragma.html#pragma_synchronous))
- "The downside to this configuration is that transactions are no longer durable and might rollback following a power failure or hard reset."([wal.html](https://www.sqlite.org/wal.html))
- NORMAL이어도 체크포인트는 sync를 한다: "with PRAGMA synchronous set to NORMAL, the checkpoint is the only operation to issue an I/O barrier or sync operation"([wal.html](https://www.sqlite.org/wal.html)). 무결성(corruption 없음)은 유지된다. 잃는 것은 마지막 커밋 몇 개의 내구성이다. 이것이 ADR 0005가 기본값으로 거부한 이유와 정확히 같다("발행한 메시지가 사라지거나 ACK한 메시지가 되살아날 수 있다").
- 참고로 `synchronous=OFF`는 전원 장애 때 DB 손상까지 가능하다([howtocorrupt.html](https://www.sqlite.org/howtocorrupt.html): "Setting PRAGMA synchronous=OFF can cause the database to go corrupt if there is an operating-system crash or power failure"). 고려 대상이 아니다.
- 하드웨어가 sync를 거짓 보고하는 문제(USB 메모리, 소비자용 디스크)는 FULL이어도 내구성을 깨뜨린다([howtocorrupt.html](https://www.sqlite.org/howtocorrupt.html) 3.1절). WAL은 "the only time that a failed sync operation can cause database corruption is during a checkpoint operation"이라고 한다.

### 4.2 대안

| 대안 | 공식 문서 내용 | 평가 |
|---|---|---|
| WAL2 | 두 WAL 파일을 번갈아 써서 체크포인트가 끝나지 못해도 WAL이 무한히 커지지 않게 하는 것이 목적. "A wal2 database can only be opened by SQLite builds compiled from this branch." fsync 횟수 언급 없음 ([wal2.md](https://www.sqlite.org/src/doc/wal2/doc/wal2.md)) | 체크포인트 정체 해결책이지 커밋당 sync 해결책이 아니다. 별도 브랜치 빌드 필요 |
| BEGIN CONCURRENT | 여러 writer가 동시에 트랜잭션을 진행하되 "still serializes COMMIT commands". 충돌 시 `SQLITE_BUSY_SNAPSHOT`. fsync 언급 없음 ([begin_concurrent.md](https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md)) | 동시 호출자의 락 대기를 줄이지만 커밋마다 sync하는 구조는 그대로(공식 문서가 반대로 주장하지 않음). 별도 브랜치 빌드 필요 |
| F2FS batch atomic write | 롤백 저널을 쓰지 않고 "can make transactions over twice as fast", F2FS만 지원 ([compile.html](https://www.sqlite.org/compile.html)) | 롤백 저널 모드 최적화로 설명됨. WAL에서의 효과 확인 못함 |
| 전용 파일시스템, 마운트 옵션, fdatasync | 공식 문서에서 단건 커밋 sync 비용을 줄이는 설정으로 안내된 것은 찾지 못함(확인 못함). 소스에서 fdatasync 사용은 빌드 매크로에 달려 있다 | 장치 쪽(전원 손실 보호 캐시가 있는 SSD 등) 조치는 SQLite 문서 범위 밖. 이 조사에서는 1차 출처 확인 못함 |
| Turso MVCC | BEGIN CONCURRENT를 MVCC 저널 모드에서 지원. 내구성 구현 세부 문서 없음 ([transactions](https://docs.turso.tech/sql-reference/statements/transactions.md)) | 별도 엔진. smq 현재 의존성(sqlite-jdbc)과 다름 |

### 4.3 sqlite-jdbc(xerial)에서 가능한 설정 범위

[SQLiteConfig.java](https://raw.githubusercontent.com/xerial/sqlite-jdbc/master/src/main/java/org/sqlite/SQLiteConfig.java) 기준(master):

- `SynchronousMode`: OFF, NORMAL, FULL (EXTRA 없음). `JournalMode`: DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF (WAL2 없음). `TransactionMode`: DEFERRED, IMMEDIATE, EXCLUSIVE. `TempStore`: DEFAULT, FILE, MEMORY.
- 열거형에 있는 PRAGMA: `wal_autocheckpoint`, `journal_size_limit`, `mmap_size`, `page_size`, `cache_size`, `temp_store`, `busy_timeout` 등. `setPragma(Pragma, String)`은 열거형 키만 받고, 모르는 pragma 이름은 `apply()`에서 건너뛴다.
- 따라서 `journal_mode=wal2`, `BEGIN CONCURRENT`, `SQLITE_FCNTL_*` 호출은 JDBC 설정만으로는 불가능하다. 일반 SQL 실행(`Statement.execute("PRAGMA ...")`)로 우회할 수는 있지만, 내장 네이티브 라이브러리가 wal2/BEGIN CONCURRENT 브랜치로 빌드되어 있지 않으므로 지원되지 않을 것이다. 내장 SQLite의 컴파일 옵션(HAVE_FDATASYNC 등)은 확인 못함.

---

## 5. 단일 호출자 순차 단건에서 그룹 커밋은 효과가 없는가

결론: 맞다. 근거는 다음과 같다.

1. 커밋당 sync는 트랜잭션 단위다. FULL이고 트랜잭션을 끝내는 프레임 묶음일 때만 WAL을 sync한다([wal.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/wal.c) 주석, [wal.html](https://www.sqlite.org/wal.html)). 그룹 커밋의 이득은 N개의 커밋을 1개로 줄이는 데서 나온다. FAQ도 같은 원리다: INSERT 자체는 빠르고 트랜잭션 수가 병목이다([faq.html](https://www.sqlite.org/faq.html)).
2. 호출자가 하나이고 각 호출이 성공 반환(= 내구성 확정)을 기다린 뒤에야 다음 호출을 낸다면, 같은 시점에 묶을 대기자가 없다. 커밋 수는 호출 수와 같고 sync 수도 같다.
3. 묶으려면 둘 중 하나를 포기해야 한다. 호출이 커밋 전에 먼저 성공을 반환하거나(ADR 0005 위반), 호출 단위를 늘려 한 호출이 여러 메시지를 받거나(공개 API 변경 위반, D1 `batch()`와 같은 방식).
4. 같은 이유로 그룹 커밋의 대기 시간 창(지연을 두고 모으기)을 도입하면 단일 호출자의 단건 지연만 늘어난다.

단, smq의 "한 바퀴"(발행, 꺼내기, 정산)는 쓰기 트랜잭션 3개다. 이 3개는 서로 다른 API 호출이므로 합칠 수 없다. 합치려면 `pull`과 `settle`을 결합한 새 연산(예: "정산하고 다음 것을 꺼내기")이 필요하다. 이는 공개 API와 SPI 단위(ADR 0002)를 바꾼다.

### 효과 있는 방법이 존재하는가

정직한 결론: 제약 (a) FULL 유지, (b) 공개 API 불변, (c) 큐 연산 단위 SPI, (d) 단일 연결과 호출자 직렬화 아래에서, 단일 호출자 순차 단건 벤치의 커밋당 fsync 횟수를 줄이는 소프트웨어 방법은 이 조사에서 찾지 못했다. 가능한 개선은 다음 범위로 제한된다.

- 커밋당 sync 횟수 감소 없이 sync 외 오버헤드를 줄이는 것(prepared statement 재사용, 불필요한 조회 제거). 기대 이득은 sync 지연 대비 작을 가능성이 높다. 정량 근거는 smq 프로파일에서 확인해야 한다(이 조사에서는 확인 못함).
- 체크포인트 빈도 조정(`wal_autocheckpoint`)으로 간헐적 추가 sync(WAL, DB, 헤더)의 빈도를 줄이기. 평균은 거의 안 바뀌고 꼬리 지연이 줄 수 있다.
- 장치와 파일시스템의 sync 지연 자체를 낮추는 환경 선택. 이는 SQLite 문서 범위 밖이며 기준선(ns0270 기준선)이 그 환경에 종속된다는 점을 문서화해야 한다.

효과가 확실한 것은 "여러 호출자가 동시에 쓰는" 경우뿐이다.

---

## 6. smq 제약에서의 채택 가능성 평가표

제약: (a) ADR 0005(FULL 유지, 성공 반환한 발행/정산은 전원 장애 뒤에도 남음), (b) 공개 API 불변, (c) SPI는 큐 연산 단위(ADR 0002), (d) 현재 구조(연결 1개, ReentrantLock 직렬화, 연산마다 BEGIN IMMEDIATE + `PRAGMA user_version` 확인 + COMMIT, 단건 한 바퀴 = 쓰기 트랜잭션 3개, 기준선 Power loss 단건 약 450 msg/s).

ADR 원문 확인: ADR 0005는 `Durability.POWER_LOSS`(기본, `synchronous=FULL`)와 `PROCESS_CRASH`(`synchronous=NORMAL`, 명시 선택)로 나눈다. ADR 0002는 "SQLite에서는 트랜잭션이 SPI 경계 밖으로 늘어나 모든 프로세스의 쓰기를 오래 막을 위험"을 이유로 저수준 트랜잭션 SPI를 기각했다.

| 방법 | 근거 출처 | fsync 영향 | 내구성 영향 | 단일 호출자 단건 벤치 효과 | 채택 가능성 |
|---|---|---|---|---|---|
| `synchronous=NORMAL` (`PROCESS_CRASH`) | [pragma.html](https://www.sqlite.org/pragma.html#pragma_synchronous), [wal.html](https://www.sqlite.org/wal.html) | 커밋당 sync 제거(체크포인트만 sync) | 전원 장애 시 커밋 롤백 가능 | 큼(ADR 0005의 조사 측정 약 1.7배) | 기본값으로는 불가(ADR 0005 위반). 이미 옵트인 옵션으로 존재 |
| 그룹 커밋(여러 호출자 연산을 한 COMMIT으로, 호출자는 커밋 후 반환, 연산별 SAVEPOINT) | [faq.html](https://www.sqlite.org/faq.html), [lang_savepoint.html](https://www.sqlite.org/lang_savepoint.html) | 동시 호출자 N명이면 sync가 최대 1/N | 변화 없음(커밋 후 반환) | 없음(커밋 수 불변). 다중 호출자 벤치에서만 효과 | 가능. SPI 안쪽 구현이면 (b), (c) 유지. 다만 ADR 0002가 우려한 "쓰기 락을 오래 쥠"과 합치기 창 지연, 커밋 실패 시 전원 실패 처리를 설계해야 함. 단일 호출자 벤치 하한은 못 올림 |
| 한 바퀴 합치기(`pull`+`settle` 결합 연산) | 출처 없음(설계 판단) | 한 바퀴 3회에서 2회 이하로 | 변화 없음 | 있음(커밋 수가 준다) | 불가. 공개 API와 SPI 연산 단위 변경(제약 b, c 위반) |
| 대량 발행 API(`publishBatch` 등) | [faq.html](https://www.sqlite.org/faq.html), D1 `batch()` ([limits](https://developers.cloudflare.com/d1/platform/limits/)) | 호출당 sync 1회로 감소 | 변화 없음 | 단건 벤치에는 해당 없음 | 불가(공개 API 추가는 제약 b가 "불변"이면 위반. 추가가 허용되는지는 별도 판단) |
| `wal_autocheckpoint` 증가/조정 | [pragma.html](https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint), [wal.html](https://www.sqlite.org/wal.html) | 커밋당 불변. 체크포인트 sync 빈도만 감소 | 변화 없음 | 거의 없음(꼬리 지연 일부) | 가능, 저위험. 효과는 작음. WAL 증가에 따른 읽기 저하 주의 |
| `journal_size_limit`, `mmap_size`, `cache_size`, `temp_store`, `page_size` | [pragma.html](https://www.sqlite.org/pragma.html) | 불변 | 변화 없음 | 없음 | 가능하나 sync 병목에는 무효. Rails 기본값이 같은 계열을 설정함([adapter](https://raw.githubusercontent.com/rails/rails/main/activerecord/lib/active_record/connection_adapters/sqlite3_adapter.rb)) |
| prepared statement 재사용 | [stmt.html](https://www.sqlite.org/c3ref/stmt.html) | 불변 | 변화 없음 | 작음(CPU만) | 가능, 저위험. 이미 적용 여부는 코드에서 확인 필요 |
| `user_version` 확인 제거/완화 (`data_version` 등) | [pragma.html](https://www.sqlite.org/pragma.html#pragma_data_version) | 불변(읽기라 sync 없음, 추론) | ADR 0004의 스키마 안전성에 영향 | 작음 | ADR 0004 재검토가 필요해 이 조사의 범위 밖 |
| BEGIN CONCURRENT | [begin_concurrent.md](https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md) | 커밋 직렬화 유지, 공식 문서에 sync 감소 언급 없음 | 변화 없음 | 없음(단일 호출자) | 불가에 가까움. sqlite-jdbc 지원 없음, 별도 빌드 필요 |
| WAL2 | [wal2.md](https://www.sqlite.org/src/doc/wal2/doc/wal2.md) | 커밋당 불변(언급 없음) | 변화 없음 | 없음 | 불가에 가까움. 같은 이유 |
| F2FS batch atomic write | [compile.html](https://www.sqlite.org/compile.html) | 롤백 저널 sync 제거(롤백 모드) | 장치/FS 보장에 의존 | WAL에서는 확인 못함 | 불가(F2FS 한정, 빌드 옵션 필요) |
| 저장 장치/파일시스템 교체(PLP SSD 등) | SQLite 문서 범위 밖(확인 못함) | 횟수 불변, sync 지연 감소 | 변화 없음 | 큼(추정) | 환경 문제. 기준선은 장치별로 따로 명시해야 함 |
| 복제로 내구성 대체(Bedrock, LiteFS, D1 방식) | [synchronization.md](https://raw.githubusercontent.com/Expensify/Bedrock/main/docs/synchronization.md), [limits](https://developers.cloudflare.com/d1/platform/limits/) | 로컬 sync 제거 가능 | 로컬 전원 장애 내구성 기준이 달라짐 | 큼 | 불가(ADR 0005의 로컬 전원 장애 약속과 다른 모델) |

---

## 7. 확인하지 못한 것 모음

- SQLite 자체의 group commit 존재 여부(공식 문서에서 못 찾음).
- Bedrock의 group commit 구현과 쓰기 TPS 수치.
- libSQL 공식 문서 내용.
- `journal_size_limit`, 파일 청크 크기, fdatasync가 단건 커밋의 sync 비용에 미치는 영향(공식 문서 문장 없음).
- xerial sqlite-jdbc 내장 SQLite의 `HAVE_FDATASYNC` 및 컴파일 옵션, `PreparedStatement` 내부 캐시.
- WAL 모드에서 `SQLITE_ENABLE_ATOMIC_WRITE`/batch atomic이 동작하는지(pager.c 앞 100KB만 확인).
- smq 단건 트랜잭션 하나가 쓰는 페이지 수와 체크포인트 빈도의 실측치, 450 msg/s 중 sync 지연이 차지하는 비율.
- SQLite 포럼의 Richard Hipp 발언("a single transaction with PRAGMA synchronous=FULL causes an fsync() ... which makes all prior transactions durable")은 검색 결과 요약에서만 보았고 원문을 직접 읽지 않아 근거로 쓰지 않았다.
