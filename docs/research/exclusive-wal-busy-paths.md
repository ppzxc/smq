# EXCLUSIVE WAL 연결이 열린 뒤 외부 프로세스가 BUSY를 일으킬 수 있는 경로

- 티켓: ppzxc/smq#137 (상위 맵: #136)
- 조사일: 2026-11-03
- 질문: `PRAGMA locking_mode = EXCLUSIVE`인 WAL 연결이 smq 열기를 마친 뒤, 외부 프로세스가 같은 DB 파일에 하는 어떤 행동이 smq 연결의 `BEGIN IMMEDIATE`, 읽기, `wal_checkpoint(RESTART)`에 `SQLITE_BUSY`(확장 코드 포함)를 일으키는가.
- 대상: `org.xerial:sqlite-jdbc:3.53.4.0`(내장 SQLite 3.53.4, `build.gradle.kts`), Linux(WSL2) ext4·tmpfs. Windows에서는 실험하지 않았다.
- 출처 표기: [S]는 sqlite.org 문서, [C]는 SQLite 3.53.4 소스(`https://sqlite.org/src/raw?filename=<경로>&ci=version-3.53.4`)나 sqlite-jdbc 3.53.4.0 소스, [M]은 이 조사에서 직접 돌린 실험, [추론]은 1차 출처가 직접 말하지 않는 내용을 근거와 함께 이은 것이다.

## 답

**smq 연결에 BUSY를 일으키는 외부 경로는 찾지 못했다. 근거는 소스와 실험이다.** 열기가 끝나면 smq 연결은 DB 파일의 EXCLUSIVE 락을 닫을 때까지 쥔다. 그 뒤로는 smq 쪽 SQLite가 OS 락도 WAL 락도 다시 요청하지 않으므로, 외부 프로세스가 무엇을 해도 smq 연결에서 락 획득이 실패할 일이 없다. 외부 프로세스가 하는 행동은 세 갈래로 끝난다.

1. **락을 지키는 SQLite 접근**(일반 읽기·쓰기, `BEGIN EXCLUSIVE`, `mode=ro`): BUSY는 외부 프로세스 쪽에서만 난다. smq는 영향이 없다. [S][C][M]
2. **락을 우회하는 접근**(`immutable=1` 읽기, 같은 JVM의 다른 fd를 닫아 POSIX 락이 풀린 뒤의 접근): smq에는 BUSY가 나지 않는다. 대신 외부가 낡은 내용을 읽거나, 외부의 쓰기가 조용히 사라진다. [S][M]
3. **파일 조작**(DB나 `-wal` 삭제·교체·자르기, `-shm` 조작): smq에는 BUSY가 나지 않는다. 결과는 데이터 유실이고, `-shm`을 자르면 smq JVM이 SIGBUS로 죽는다. [S][M]

`SQLITE_BUSY_SNAPSHOT`에 이르는 코드 경로는 이론상 하나 남는다. smq가 열 때 이미 WAL인 파일을 다시 열어 `-shm`을 쓰는 경우, 그 `-shm` 헤더 바이트를 누군가 `BEGIN IMMEDIATE`의 읽기 시작과 쓰기 시작 사이에 고쳐 쓰는 경우다. SQLite로는 그렇게 할 방법이 없고 파일을 직접 고쳐 써야 한다. 실험에서는 재현되지 않았다(5절).

## 1. 열기 순서와 락 상태

smq의 열기(`SqliteStorage.open`)는 락 파일 → sqlite-jdbc 연결(설정 pragma 적용) → `TransactionExecutor.execute(SCHEMA::establish)`(`BEGIN IMMEDIATE` … `COMMIT`) → `wal_autocheckpoint=0`, `journal_size_limit`, `journal_mode=WAL` 순서다. `busy_timeout`은 0이다.

- "When the locking-mode is set to EXCLUSIVE, the database connection never releases file-locks. The first time the database is read in EXCLUSIVE mode, a shared lock is obtained and held. The first time the database is written, an exclusive lock is obtained and held." [S] https://www.sqlite.org/pragma.html#pragma_locking_mode
- WAL 모드에서 쓰기 트랜잭션을 시작할 때, EXCLUSIVE 모드인 pager는 DB 파일의 EXCLUSIVE 락을 먼저 잡는다(`sqlite3PagerBegin`, `pager.c` 5978–5986행). 주석: "If the pager is configured to use locking_mode=exclusive, and an exclusive lock on the database is not already held, obtain it now." 이어서 WAL을 exclusive 모드로 바꾼다(`sqlite3WalExclusiveMode(pWal, 1)`). [C]
- WAL을 처음 열 때 이미 EXCLUSIVE 모드면 EXCLUSIVE 락을 먼저 잡고 heap memory wal-index를 쓴다(`pagerOpenWal`, `pager.c` 7638행). [C][S] https://www.sqlite.org/wal.html#noshm
- 따라서 열기의 `BEGIN IMMEDIATE`(스키마 판정)가 끝난 시점에는 새 파일이든 다시 여는 파일이든 smq 연결이 DB 파일의 EXCLUSIVE 락을 쥐고 있다. 이를 놓는 길은 연결을 닫거나 `locking_mode=NORMAL`로 바꾼 뒤 접근하는 것뿐인데(pragma.html), smq는 둘 다 하지 않는다. [추론, 아래 [M]으로 확인]

### 1.1 다시 열 때는 `-shm`이 생긴다(ADR-0012의 서술과 다름)

ADR-0012와 `SqliteStorage.open`의 주석에는 "WAL로 바꾸기 전에 켜야 -shm이 생기지 않는다"고 적혀 있다. 이것은 **새 파일에만 맞는다.**

- sqlite-jdbc는 설정 pragma를 `Properties`의 키 순회 순서로 하나씩 `pragma k=v`로 실행한다(`SQLiteConfig.apply`). [C] https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/SQLiteConfig.java
- smq 설정의 실제 순회 순서는 `[open_mode, transaction_mode, jdbc.get_generated_keys, synchronous, date_precision, busy_timeout, date_string_format, jdbc.explicit_readonly, date_class, locking_mode, shared_cache]`이다. `synchronous`가 `locking_mode`보다 먼저다. [M]
- `synchronous` pragma의 플래그에는 `NeedSchema`("Force schema load before running")가 있다(`tool/mkpragmatab.tcl`). 그래서 `locking_mode=EXCLUSIVE`가 걸리기 전에 NORMAL 모드로 스키마를 읽는다. 파일이 이미 WAL이면 이때 shared-memory wal-index(`-shm`)로 WAL이 열린다. [C]
- 실측(`ShmCause`): 설정에 `synchronous`가 있으면 다시 열기의 연결 직후 `-shm`이 생기고 `locking_mode=NORMAL`이 `normal`로 바뀐다(EXCLUSIVE에 갇히지 않음). `synchronous`를 빼면 `-shm`이 생기지 않고 `locking_mode=NORMAL`이 `exclusive`로 남는다. [M]
- 실제 `SqliteStorage.open`으로도 같다. 새 파일 열기에서는 `[smq.db, smq.db.lock]`, 닫았다가 다시 열면 `[smq.db-shm, smq.db-wal, smq.db, smq.db.lock]`. [M]

BUSY 관점에서는 두 경우가 다르지 않다. 다시 열기에서도 열기의 `BEGIN IMMEDIATE`가 DB 파일 EXCLUSIVE 락을 잡고 WAL을 `WAL_EXCLUSIVE_MODE`로 바꾼다(위 `sqlite3PagerBegin`). 그 뒤의 WAL 락 함수는 모두 OS를 부르지 않고 성공한다(2절). 다른 점은 wal-index가 다른 프로세스도 열 수 있는 파일(`-shm`)에 있다는 것이고, 이 차이는 5절의 파일 조작에서만 드러난다.

## 2. 열린 뒤 smq 연결이 BUSY를 받을 수 있는 자리 (소스)

`SQLITE_BUSY`는 락을 얻지 못했을 때 난다. "The SQLITE_BUSY result code indicates that the database file could not be written (or in some cases read) because of concurrent activity by some other database connection" [S] https://www.sqlite.org/rescode.html#busy. smq 연결이 락을 요청하는 자리는 두 층에 있다.

- **DB 파일 락(pager → VFS)**: `pagerLockDb`는 이미 쥔 수준 이하를 요청하면 VFS를 부르지 않는다(`if( pPager->eLock<eLock || ... )`, `pager.c` 1165행). unix VFS도 이미 쥔 수준 이하면 바로 성공한다("ok (already held)", `os_unix.c` 1923행). EXCLUSIVE는 최고 수준이므로 이후의 모든 요청이 OS 호출 없이 성공한다. [C]
- **WAL 락(read mark, write, checkpoint, recover)**: `walLockShared`, `walLockExclusive`는 `if( pWal->exclusiveMode ) return SQLITE_OK;`로 시작한다(`wal.c` 1087, 1109행). heap memory 모드든 `WAL_EXCLUSIVE_MODE`든 0이 아니다. [C]

질문의 세 동작에 대입하면 다음과 같다.

| smq의 동작 | BUSY가 나는 코드 자리 | EXCLUSIVE WAL에서 |
|---|---|---|
| `BEGIN IMMEDIATE` | `sqlite3WalBeginWriteTransaction`의 `walLockExclusive(WAL_WRITE_LOCK)`, 이어서 `SQLITE_BUSY_SNAPSHOT` 검사(`memcmp(&pWal->hdr, walIndexHdr(pWal))`, `wal.c` 3724–3736행) | 락은 OS 호출 없이 성공. `BUSY_SNAPSHOT`은 다른 연결이 그 사이 WAL에 썼을 때 난다([S] rescode.html#busy_snapshot). heap memory면 wal-index가 연결 자신의 메모리라 다른 연결이 바꿀 수 없다. `-shm`이면 5절 참고 |
| autocommit 읽기(`anyOrUncertain`) | read mark 락, `SQLITE_BUSY_RECOVERY`(다른 프로세스가 WAL 복구 중, [S] rescode.html#busy_recovery) | 락은 OS 호출 없이 성공. 복구 락도 `walLockExclusive`라 같다 |
| `PRAGMA wal_checkpoint(RESTART)` | `WAL_CKPT_LOCK`, `WAL_WRITE_LOCK`, reader 대기(`walBusyLock`), `wal.c` 4340–4365행 | 락은 OS 호출 없이 성공. **또한 이 pragma는 BUSY를 예외로 올리지 않는다**(아래) |

**`wal_checkpoint` pragma는 BUSY를 결과 행으로 돌려준다.** `OP_Checkpoint`는 `sqlite3Checkpoint`가 `SQLITE_BUSY`를 돌려주면 `rc = SQLITE_OK; aRes[0] = 1;`로 바꾸고 계속한다(`vdbe.c` 8030–8034행). [C] 문서: "The first column is usually 0 but will be 1 if a RESTART or FULL or TRUNCATE checkpoint was blocked from completing ... the first column is 0 if the equivalent call to sqlite3_wal_checkpoint_v2() would have returned SQLITE_OK or 1 if the equivalent call would have returned SQLITE_BUSY." [S] https://www.sqlite.org/pragma.html#pragma_wal_checkpoint. 단, 비교가 `rc!=SQLITE_BUSY`라서 기본 코드 5만 행으로 바뀐다. 확장 BUSY 코드(예: 261)는 예외로 올라간다. EXCLUSIVE WAL에서는 이 확장 코드가 나는 경로(다른 프로세스의 복구)도 위의 락 단락으로 막힌다. [C][추론]

같은 연결 자신이 원인이 되는 경우도 BUSY가 아니다. 열린 읽기 statement가 있는 채 체크포인트를 부르면 `SQLITE_LOCKED`(6)다(`sqlite3BtreeCheckpoint`의 `p->inTrans!=TRANS_NONE`). [C] smq는 statement를 닫고 lock 아래에서 체크포인트하므로 해당하지 않는다.

## 3. 공식 문서가 말하는 WAL의 BUSY 경우

wal.html §9 "Sometimes Queries Return SQLITE_BUSY In WAL Mode"가 드는 경우는 셋이다. [S] https://www.sqlite.org/wal.html §9

1. "If another database connection has the database mode open in exclusive locking mode then all queries against the database will return SQLITE_BUSY." 여기서 EXCLUSIVE를 쥔 쪽은 smq다. BUSY는 **외부 쪽**이 받는다.
2. 마지막 연결이 닫히며 WAL과 `-shm`을 정리하는 동안 다른 연결이 열면 BUSY가 날 수 있다. smq가 닫히는 중이거나 다른 연결이 닫히는 중인 경우다. 한 JVM 전용에서는 smq 열기 중에만 해당하며, 이때는 `StorageLockedException`이다(ADR-0012).
3. 마지막 연결이 죽은 뒤 첫 연결이 복구하는 동안 셋째 연결이 끼어들면 BUSY가 난다. 이것도 열기 시점의 일이다.

셋 다 "열린 뒤"의 smq 연결에는 해당하지 않는다.

## 4. 실험 설정 [M]

- smq 쪽: 실제 열기 순서를 흉내 낸 JDBC 연결(`busy_timeout=0`, `locking_mode=EXCLUSIVE`, `synchronous=FULL`, `BEGIN IMMEDIATE`로 스키마 작성, `wal_autocheckpoint=0`, `journal_size_limit`, `journal_mode=WAL`). sqlite-jdbc 3.53.4.0, `sqlite_version()` = 3.53.4. 1.1절과 같은 JVM 두 번째 연결 확인에는 실제 `SqliteStorage.open`을 썼다.
- 외부 프로세스: Python 3.14 `sqlite3` 모듈(내장 SQLite 3.46.1, `timeout=0`). sqlite3 CLI는 설치돼 있지 않았다.
- 경우마다 새 디렉터리에서 "새 파일"(heap memory wal-index)과 "다시 열기"(`-shm` wal-index, 1.1절) 두 가지로 돌렸다. 외부 행동을 한 뒤 smq 연결에서 autocommit 읽기, `BEGIN IMMEDIATE`+`INSERT`+`COMMIT`, `wal_checkpoint(RESTART)`를 차례로 하고, 외부 행동과 smq 동작을 한 번 더 반복했다. 끝으로 smq를 닫고 새 연결로 `integrity_check`와 행 수를 봤다(smq는 3행을 넣는다).
- 파일시스템: ext4(`/home`)와 tmpfs(`/tmp`). 결과는 같았다.
- 코드: 이 브랜치에 넣지 않았다. 핵심은 아래 표와 같다.

## 5. 실험 결과 [M]

**smq 연결의 읽기, `BEGIN IMMEDIATE`, `wal_checkpoint(RESTART)`는 모든 경우에 성공했다. BUSY나 다른 오류는 한 번도 없었다.** 체크포인트의 첫 열(busy)은 매번 0이었다. 아래 표의 "smq" 열은 그래서 SIGBUS 한 칸을 빼고 모두 "성공"이다.

| 외부 행동 | 외부 프로세스 결과 | smq 연결 | 닫은 뒤 |
|---|---|---|---|
| 일반 읽기 | `SQLITE_BUSY`(5) | 성공 | 정상, 3행 |
| 일반 쓰기 | `SQLITE_BUSY` | 성공 | 정상, 3행 |
| `BEGIN EXCLUSIVE` | `SQLITE_BUSY` | 성공 | 정상, 3행 |
| `?mode=ro` 읽기 | `SQLITE_BUSY` | 성공 | 정상, 3행 |
| `?immutable=1` 읽기 | 성공. 단 WAL에만 있는 내용을 못 본다(새 파일 첫 읽기: WAL에 1행이 있는데 0. 체크포인트 직후의 읽기는 맞았다) | 성공 | 정상, 3행 |
| `?nolock=1` 읽기·쓰기 | `SQLITE_CANTOPEN`(14). `nolock`이면 pager가 WAL을 지원하지 않는다고 답한다(`sqlite3PagerWalSupported`의 `if( pPager->noLock ) return 0;`, `pager.c` 7597행) | 성공 | 정상, 3행 |
| `-wal` 삭제 | 성공 | 성공(열린 fd로 계속 씀) | 정상, 3행. `-wal`은 사라진 채 |
| `-wal`을 0바이트로 자르기 | 성공 | 성공 | 정상, 3행 |
| DB 파일 삭제 | 성공. 이후 외부 읽기는 `no such table`(빈 새 파일을 만든다) | 성공(지워진 inode에 씀) | **데이터 전부 유실**(새 빈 파일) |
| DB 파일 교체(복사 후 `rename`) | 성공 | 성공(옛 inode에 씀) | **교체 이후의 쓰기 유실**(새 파일 3행 중 2행 또는 1행) |
| `-shm` 헤더 0으로 덮기(다시 열기만) | 성공 | 성공 | 정상, 3행 |
| `-shm` 헤더의 `mxFrame` 고치기(다시 열기만) | 성공 | 성공 | 정상, 3행 |
| `-shm`을 0바이트로 자르기(다시 열기만) | 성공 | **smq JVM이 SIGBUS로 죽음**(libsqlitejdbc.so 안, mmap한 wal-index 접근). ext4와 tmpfs 모두 재현 | - |
| 같은 JVM이 `smq.db`의 다른 fd를 열고 닫은 뒤 일반 읽기 | 성공(락이 풀림) | 성공 | 정상, 3행 |
| 같은 JVM fd 닫기 뒤 `BEGIN EXCLUSIVE` | 성공(외부가 EXCLUSIVE를 얻음) | 성공 | 정상, 3행 |
| 같은 JVM fd 닫기 뒤 일반 쓰기(2회) | 성공 | 성공 | **외부 쓰기 유실**. 새 파일: 2건 모두 사라져 3행. 다시 열기: 1건만 남아 4행(5행이어야 함). 이때도 `-wal`, `-shm`이 외부 연결이 닫히며 지워졌다 |

같은 JVM의 두 번째 JDBC 연결(락 파일을 거치지 않음)도 외부 프로세스와 같았다. smq가 트랜잭션 밖에 있을 때도 그 연결의 `BEGIN IMMEDIATE`와 `SELECT`가 모두 `SQLITE_BUSY`(5)였고, smq 쪽은 영향이 없었다(새 파일, 다시 열기 모두). [M]

관련 문서:

- 같은 프로세스의 `close()`가 POSIX 락을 모두 푼다: "the close() system call will cancel all POSIX advisory locks on the same file for all threads and all file descriptors in the process." [S] https://www.sqlite.org/howtocorrupt.html §2.2
- 열린 DB 파일 삭제·이름 변경은 정의되지 않은 동작이다: "unlinking or renaming an open database file results in behavior that is undefined and probably undesirable." Windows는 열린 파일 삭제를 허용하지 않는다. [S] howtocorrupt.html §2.5
- `-wal`을 지우면 크래시 뒤 복구가 안 된다: "If the hot journal files are moved, deleted, or renamed after a crash or power failure, then automatic recovery will not work and the database may go corrupt." [S] howtocorrupt.html §1.3
- `immutable=1`은 "skips all file locking and change detection ... that file changes anyhow, then SQLite might return incorrect query results and/or SQLITE_CORRUPT errors." [S] https://www.sqlite.org/uri.html#uriimmutable

### 5.1 남는 이론상 경로: `-shm` 직접 조작에 의한 `SQLITE_BUSY_SNAPSHOT`

다시 열기(`-shm` 사용)에서는 `walIndexHdr(pWal)`가 mmap한 `-shm`을 가리킨다. `BEGIN IMMEDIATE`는 읽기 트랜잭션을 시작하며 헤더를 복사하고, 곧이어 쓰기 시작에서 그 복사본과 `-shm` 헤더를 `memcmp`해 다르면 `SQLITE_BUSY_SNAPSHOT`(517)을 돌려준다(`wal.c` 3734–3736행). [C] 그러니 이 짧은 틈에 `-shm` 헤더 바이트를 고쳐 쓰는 프로세스가 있으면 이론상 517이 난다. 다만 다음과 같다.

- SQLite를 거치는 외부 프로세스는 `-shm`에 닿기 전에 DB 파일 SHARED 락에서 BUSY로 멈춘다(위 표의 일반 읽기·쓰기). `nolock=1`은 WAL 파일을 열지 못한다. 파일 바이트를 직접 쓰는 경우만 남는다.
- 실험에서 헤더를 0으로 덮거나 `mxFrame`을 바꿔도 smq 동작 사이의 틈을 노리지 않았으므로 517은 나지 않았다.
- 새 파일(heap memory)에서는 wal-index가 smq 프로세스의 메모리에 있어 이 경로가 없다.
- 같은 조작이 SIGBUS(자르기)나 내용 오염으로 이어지는 것이 더 직접적인 결과다. [추론]

## 6. smq 코드와 테스트에 대한 함의 (사실만)

**`TransactionExecutor.anyOrUncertain`의 BUSY 분기** ("방어 코드: 락 파일은 권고라서 이를 우회해 파일을 쥔 외부 도구가 있으면 BUSY가 날 수 있다")

- 열린 뒤 smq 연결의 autocommit 읽기에 BUSY가 나는 경로는 소스(2절)에서도 실험(5절)에서도 나오지 않았다. 락 파일을 모르는 외부 도구는 smq 연결이 쥔 EXCLUSIVE 락에 막혀 BUSY를 **자기가** 받는다.
- 락을 실제로 우회하는 경우(같은 JVM의 fd close로 POSIX 락 상실, `immutable=1`, 파일 조작)에도 smq 쪽은 BUSY가 아니었다. 결과는 외부 쓰기 유실, 낡은 읽기, 데이터 유실, SIGBUS였다.
- 남는 경로는 다시 열기에서 `-shm` 바이트를 직접 고쳐 쓰는 경우의 `BUSY_SNAPSHOT`(5.1)뿐이다. 이것도 autocommit 읽기가 아니라 `BEGIN IMMEDIATE` 쪽 자리다.

**`checkpointIfWalIsLarge`의 BUSY catch와 `CHECKPOINT_RETRY_AFTER_COMMITS`**

- `PRAGMA wal_checkpoint(RESTART)`는 기본 코드 `SQLITE_BUSY`를 `SQLException`으로 올리지 않고 결과 행 첫 열 1로 돌려준다(`vdbe.c` `OP_Checkpoint`, pragma.html). 따라서 `catch (SQLException e) { if (isBusy(e)) ... }` 분기에 닿을 수 있는 것은 확장 BUSY 코드뿐이고, EXCLUSIVE WAL에서는 그 경로도 2절의 락 단락에 걸린다. 이것은 프로세스 수와 무관한 사실이다.
- 재시도를 실제로 일으키는 다른 자리는 `rs.getInt(1) != 0`(결과 행 busy=1 또는 행 없음)이다. EXCLUSIVE WAL에서는 체크포인트의 writer 락과 reader 대기가 OS 호출 없이 성공하므로(`walBusyLock` → `walLockExclusive`) busy=1이 될 원인이 없다. 실험에서 첫 열은 매번 0이었다.
- 이 저장소의 테스트 중 체크포인트 재시도(`CHECKPOINT_RETRY_AFTER_COMMITS`, `walOverLimit`가 되감기 실패로 남는 경우)를 다루는 것은 없다. `TransactionExecutorTest`는 `wal_checkpoint(PASSIVE)`로 남은 프레임 수만 본다.

**`TransactionExecutorReadTest`**

- 픽스처가 `locking_mode`를 설정하지 않은 NORMAL 모드 연결로 `TransactionExecutor`를 만든다. smq의 실제 연결 설정과 다르다.
- `BUSY가_나면_재시도_없이_있다고_답한다`는 probe 안에서 인위로 `SQLException(..., 517)`을 던진다. 분기 자체를 시험할 뿐, 실제 연결에서 그 분기에 닿는지는 시험하지 않는다.
- `읽는_동안과_읽은_뒤에_다른_연결의_쓰기가_막히지_않는다`는 다른 연결의 쓰기가 성공한다는 전제에 기대는데, smq의 EXCLUSIVE 연결에서는 다른 연결의 쓰기가 늘 BUSY다(5절). NORMAL 픽스처에서만 성립하는 성질이다.

**`SqliteStorageTest.쓰기_트랜잭션은_BEGIN_IMMEDIATE로_시작한다`**

- 테스트는 smq 트랜잭션 안에서 다른 연결의 `BEGIN IMMEDIATE`가 BUSY인지 본다. 그런데 같은 JVM의 다른 연결은 smq가 **트랜잭션 밖에** 있을 때도 `BEGIN IMMEDIATE`와 `SELECT` 모두 BUSY다(5절 끝). 따라서 이 테스트는 smq가 `BEGIN IMMEDIATE`를 쓰든 `BEGIN DEFERRED`를 쓰든 통과한다. 이름이 말하는 것을 구분하지 못한다. [M][추론]

**곁가지로 드러난 사실(BUSY와 무관)**

- ADR-0012와 `SqliteStorage.open` 주석의 "WAL로 바꾸기 전에 켜야 -shm이 생기지 않는다"는 새 파일에서만 맞다. 다시 열기에서는 sqlite-jdbc가 `synchronous`를 `locking_mode`보다 먼저 실행해 `-shm`이 생기고, 연결이 heap memory 모드가 아니다(1.1절).
- 이 상태에서 외부가 `-shm`을 자르면 smq JVM이 SIGBUS로 죽는다(5절).
- Windows: 2절의 근거(`pagerLockDb`의 조기 반환, `walLock*`의 `exclusiveMode` 검사)는 VFS 위층이라 Windows에도 그대로 적용된다고 본다. 실험은 하지 않았다. [추론]
