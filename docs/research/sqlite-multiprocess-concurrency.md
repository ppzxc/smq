# SQLite 다중 프로세스 동시성과 sqlite-jdbc Java 25 지원

- 티켓: ppzxc/smq#4 (상위 맵: #1)
- 조사일: 2026-10-23
- 범위: 같은 호스트의 여러 프로세스가 SQLite 파일 하나를 큐 저장소로 공유할 때의 한계와 권장 설정, 그리고 Java 25에서 쓸 JDBC 드라이버. 다른 백엔드와의 비교는 #3에서 다루므로 여기서는 다루지 않는다.
- 출처 표기: [S]는 1차 문서 인용, [M]은 이 저장소 개발 환경에서 직접 측정한 결과, [추론]은 1차 출처가 직접 말하지 않은 내용을 근거와 함께 추론한 것이다.

## 요약

1. **쓰기는 DB 전체에서 한 번에 하나뿐이다.** WAL을 써도 writer는 하나이고, 생산자(enqueue)와 소비자(claim/ack의 UPDATE/DELETE)가 같은 쓰기 락 하나를 나눠 쓴다. 프로세스를 늘려도 전체 쓰기 처리량은 늘지 않고 오히려 줄며, 늘리려면 배치로 묶어야 한다. [S][M]
2. **권장 설정**: `journal_mode=WAL`, `synchronous=NORMAL`(내구성이 중요하면 `FULL`), `busy_timeout` 5000ms 이상, 모든 쓰기 트랜잭션은 `BEGIN IMMEDIATE`(sqlite-jdbc에서는 `transaction_mode=IMMEDIATE`). 그리고 `SQLITE_BUSY`가 나면 트랜잭션 전체를 재시도하는 루프를 애플리케이션에 둔다.
3. **드라이버**: `org.xerial:sqlite-jdbc:3.53.4.0`(2026-08-26 릴리스, SQLite 3.53.4 내장). CI 매트릭스에 JDK 25가 있고, 로컬 Corretto 25.0.3에서도 동작을 확인했다. 아직 JNI 기반이라 JDK 24+에서는 `--enable-native-access` 경고가 나온다. [S][M]
4. **파일 위치**: 반드시 Linux 로컬 파일시스템(WSL2라면 ext4 쪽 `~/...`)에 둔다. `/mnt/c`(9P)는 측정상 약 200배 느렸고, Windows 프로세스와 함께 쓰면 WAL의 전제(같은 호스트, 공유 메모리)가 깨진다. NFS/SMB 같은 네트워크 FS는 금지. [S][M]
5. **프로세스 간 알림 기능은 SQLite에 없다.** `update_hook`/`commit_hook`은 연결 단위이고, 다른 연결에서 커밋해도 호출되지 않는다(측정). 대신 `PRAGMA data_version`이나 인덱스 기반 `SELECT ... LIMIT 1`을 적응형 간격으로 폴링한다. 한 번 호출에 약 3µs다. [S][M]

## 1. 락 모델과 쓰기 직렬화

- SQLite는 동시에 여러 read 트랜잭션을 허용하지만 write 트랜잭션은 하나만 허용한다. 이는 별도 프로세스 사이에서도 마찬가지다. [S] https://www.sqlite.org/lang_transaction.html
- "SQLite supports an unlimited number of simultaneous readers, but it will only allow one writer at any instant in time." 각 쓰기 작업은 "a few dozen milliseconds" 이내로 끝나는 것을 전제로 한다. [S] https://www.sqlite.org/whentouse.html
- WAL에서는 reader와 writer가 서로를 막지 않지만, WAL 파일이 하나뿐이라 writer는 여전히 하나다. [S] https://www.sqlite.org/wal.html
- `BEGIN CONCURRENT`(다중 writer, 낙관적 페이지 락)는 SQLite 본선이 아니라 별도 브랜치 기능이다. 공식 배포판과 sqlite-jdbc에서는 쓸 수 없다. [S] https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md

**큐 설계에 주는 의미**: 소비자의 claim(예: `UPDATE ... SET state='inflight' ... RETURNING`)과 ack(`DELETE`)도 쓰기다. 따라서 "초당 쓰기 트랜잭션 수"라는 예산 하나를 생산자와 소비자가 나눠 쓴다. 메시지 한 건의 생애가 enqueue, claim, ack로 이루어진다면, 같은 예산에서 메시지 처리량은 enqueue만 할 때의 약 1/3이다. [추론]

## 2. `BEGIN IMMEDIATE`와 `busy_timeout`

- 기본 트랜잭션 모드는 DEFERRED다. SELECT로 시작한 read 트랜잭션이 나중에 쓰기를 시도할 때 다른 연결이 이미 쓰고 있으면, 업그레이드가 불가능해 곧바로 `SQLITE_BUSY`가 난다. [S] https://www.sqlite.org/lang_transaction.html
- busy handler(=`busy_timeout`)가 있어도, SQLite가 데드락 가능성(read→write 승격 경합)을 감지하면 핸들러를 호출하지 않고 즉시 `SQLITE_BUSY`를 돌려준다. [S] https://www.sqlite.org/c3ref/busy_handler.html
- `BEGIN IMMEDIATE`는 시작 시점에 write 락을 잡는다. 실패하더라도 이 시점에는 busy handler가 정상적으로 대기, 재시도한다. WAL에서는 IMMEDIATE와 EXCLUSIVE가 같다. [S] https://www.sqlite.org/lang_transaction.html
- `sqlite3_busy_timeout(ms)`은 누적 sleep이 ms에 도달할 때까지 재시도한 뒤 `SQLITE_BUSY`를 반환한다. [S] https://www.sqlite.org/c3ref/busy_timeout.html
- sqlite-jdbc의 기본 `busy_timeout`은 3000ms다(`SQLiteConfig`에서 `"3000"`이 기본값). [S] https://github.com/xerial/sqlite-jdbc/blob/master/src/main/java/org/sqlite/SQLiteConfig.java
- sqlite-jdbc는 `transaction_mode=IMMEDIATE`를 설정하면 `setAutoCommit(false)` 이후 트랜잭션 시작문으로 `begin immediate;`를 쓴다. [S] https://github.com/xerial/sqlite-jdbc/blob/master/src/main/java/org/sqlite/SQLiteConnectionConfig.java , https://github.com/xerial/sqlite-jdbc/blob/master/USAGE.md

`busy_timeout`이 있어도 `SQLITE_BUSY`가 날 수 있는 경우 [S] https://www.sqlite.org/wal.html (9절):
- 다른 연결이 `locking_mode=EXCLUSIVE`로 열고 있는 경우
- 마지막 연결이 닫히면서 WAL/shm을 정리하는 짧은 순간
- 크래시 뒤 첫 연결이 recovery를 하는 동안

따라서 **애플리케이션 레벨에서 트랜잭션 전체를 재시도**해야 한다(지수 백오프와 지터, 상한 설정).

## 3. WAL, synchronous, checkpoint

- WAL 모드는 DB 파일에 영구 기록되므로 한 번만 설정하면 된다. 다만 연결마다 `PRAGMA journal_mode=WAL`을 실행해도 무해하다. [S] https://www.sqlite.org/wal.html (3.3절)
- 모든 프로세스가 같은 호스트에 있어야 한다. WAL은 wal-index를 공유 메모리(`-shm` 파일 mmap)로 유지하기 때문에 네트워크 FS에서 동작하지 않는다. 또 `-shm` 파일에 대한 쓰기 권한이 필요하다. [S] https://www.sqlite.org/wal.html
- `synchronous=NORMAL` + WAL: 일관성(손상 없음)은 보장된다. 다만 전원이 꺼지거나 OS가 크래시하면 마지막 커밋이 롤백될 수 있다. 애플리케이션 크래시에는 설정과 관계없이 내구성이 유지된다. `FULL`이면 커밋마다 WAL을 fsync한다. [S] https://www.sqlite.org/pragma.html#pragma_synchronous
- 자동 checkpoint는 WAL이 1000페이지(기본 4KB 페이지 기준 약 4MB)에 도달하면 커밋 끝에 PASSIVE 모드로 실행된다. `wal_autocheckpoint=N`으로 조정할 수 있다. [S] https://www.sqlite.org/wal.html (6절), https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint
- sqlite-jdbc 3.53.0.0부터 `SQLiteConfig`에서 `wal_autocheckpoint`를 설정할 수 있다. [S] https://github.com/xerial/sqlite-jdbc/releases/tag/3.53.0.0
- **Checkpoint starvation**: 읽기 트랜잭션이 하나라도 열려 있으면 checkpoint가 WAL을 리셋하지 못한다. reader가 계속 겹치면 WAL이 무한히 커진다. 해결책은 reader가 없는 순간("reader gaps")을 보장하거나, `wal_checkpoint(RESTART|TRUNCATE)`를 수동으로 실행하는 것이다. [S] https://www.sqlite.org/wal.html (6절)
- checkpoint 모드: PASSIVE는 대기하지 않고 busy handler도 호출하지 않는다. FULL은 writer를 막고 모든 reader가 최신 스냅샷을 볼 때까지 대기한다. RESTART는 FULL에 더해 reader가 WAL을 다 쓸 때까지 대기한다. TRUNCATE는 RESTART에 더해 WAL 파일을 0바이트로 자른다. [S] https://www.sqlite.org/pragma.html#pragma_wal_checkpoint , https://www.sqlite.org/c3ref/wal_checkpoint_v2.html
- checkpoint를 해도 WAL 파일은 잘리지 않고 재사용된다. 남는 크기를 제한하려면 `journal_size_limit`을 쓴다. [S] https://www.sqlite.org/pragma.html#pragma_journal_size_limit

**큐 관점의 주의점** [추론]: 폴링 소비자가 read 트랜잭션을 열어 둔 채 대기하면(ResultSet 미종료, autocommit 해제 상태의 SELECT 등) 그 자체로 checkpoint starvation의 원인이 된다. 폴링은 autocommit 상태에서 하고, ResultSet과 Statement는 반드시 닫는다. 실제로 측정용 코드에서 PRAGMA의 Statement를 닫지 않았더니 같은 연결의 다음 쓰기가 `SQLITE_BUSY ... SQL statements in progress`로 실패했다. [M]

## 4. 처리량 감각치 (측정)

환경 [M]: WSL2(커널 6.18, ext4 on VHDX), 32 vCPU, Corretto 25.0.3, sqlite-jdbc 3.53.4.0. 각 프로세스는 별도 JVM이고, 트랜잭션마다 `BEGIN IMMEDIATE`, 256B 메시지 INSERT n건, 공유 카운터 read-modify-write 1회, `COMMIT`을 수행한다. 설정은 `busy_timeout=10000`.

| 위치 | 프로세스 | 배치 | synchronous | 총 메시지 | msg/s |
|---|---|---|---|---|---|
| ext4 | 1 | 1 | NORMAL | 20,000 | 약 25,000 |
| ext4 | 4 | 1 | NORMAL | 40,000 | 약 24,000 |
| ext4 | 16 | 1 | NORMAL | 40,000 | 약 14,000 |
| ext4 | 4 | 1 | FULL | 20,000 | 약 14,500 |
| ext4 | 16 | 1 | FULL | 20,000 | 약 10,000 |
| ext4 | 8 | 100 | NORMAL | 400,000 | 약 118,000 |
| /mnt/c (9P) | 1 | 1 | NORMAL | 500 | 약 66 |
| /mnt/c (9P) | 4 | 1 | NORMAL | 1,000 | 약 115 |

- 모든 실행에서 행 수와 카운터가 일치했고(lost update 없음), `integrity_check=ok`, 애플리케이션 레벨 BUSY 재시도는 0건이었다.
- 해석: 프로세스 수를 늘려도 처리량은 늘지 않고 줄었다(1개 25k에서 16개 14k). 배치로 묶으면 약 5배로 늘었다. 이는 1절의 "writer는 하나"와 FAQ의 "INSERT는 초당 5만 건 이상이지만 트랜잭션은 디스크 fsync가 상한"과 일치한다. [S] https://www.sqlite.org/faq.html#q19
- 주의: wall time에 JVM 기동 시간이 포함되어 있어 값은 하한에 가깝다. WSL2 VHDX의 fsync 비용은 베어메탈과 다를 수 있다(FULL이 예상보다 빠름). 따라서 이 숫자는 이 호스트에서의 자릿수 감각으로만 쓴다. 실제 하드웨어에서는 smq 벤치로 다시 측정해야 한다.

## 5. 파일시스템: 네트워크 FS와 WSL

- NFS 등에서는 fcntl 락이 깨져 있는 경우가 많으므로 여러 프로세스가 접근한다면 피하라고 한다. [S] https://www.sqlite.org/faq.html#q5 , https://www.sqlite.org/useovernet.html
- WAL은 네트워크 FS에서 동작하지 않는다. [S] https://www.sqlite.org/wal.html
- POSIX advisory lock은 같은 프로세스 안에서 누군가 같은 파일을 `close()`하기만 해도 모든 락이 풀린다. SQLite는 내부 전역 목록으로 이를 우회하지만, 이 우회는 SQLite 라이브러리 사본 하나 안에서만 유효하다. [S] https://www.sqlite.org/howtocorrupt.html (2.2절, 2.3절). smq에 주는 함의는 다음과 같다.
  - 같은 JVM에서 DB 파일을 SQLite 밖에서 열고 닫지 말 것(예: `Files.copy` 백업, 헤더 검사). 백업이 필요하면 `VACUUM INTO`나 백업 API를 쓴다. [추론]
  - 같은 JVM에 sqlite-jdbc 사본이 두 개(셰이딩, 다른 classloader) 올라가지 않게 할 것. [추론]
- WSL:
  - Microsoft는 Linux 도구로 작업한다면 파일을 WSL 파일시스템에 두라고 권고하며, `/mnt/<drive>`는 OS 경계를 넘어 느리다고 설명한다. [S] https://learn.microsoft.com/en-us/windows/wsl/filesystems
  - 이 환경에서 `/mnt/c`는 `type 9p (aname=drvfs ... cache=0x5)`로 마운트되어 있다. 측정 결과 ext4보다 약 200배 느렸다(66~115 msg/s). WSL2 안의 프로세스끼리는 이번 실행에서 lost update가 없었지만, 실행 한 번으로 락이 올바르다는 증명은 되지 않는다. [M]
  - 9P는 파일 프로토콜이고 실제 파일은 Windows 쪽에 있다. Windows 네이티브 프로세스가 같은 파일을 열면 서로 다른 커널 사이에서 `-shm` 공유 메모리와 락을 공유해야 하는데, 이는 SQLite가 지원하지 않는 "네트워크 FS 위 WAL"과 같은 상황이다. [추론, 근거: wal.html]
  - WSL의 fcntl 락 미구현 버그(microsoft/WSL#1927)는 **WSL1** 문제다. WSL2 ext4에서는 해결되어 `fixed-in-wsl2`로 닫혔다. `/mnt/c` 9P에서의 락 의미는 이 이슈가 다루지 않으므로 검증되지 않은 상태다. [S] https://github.com/microsoft/WSL/issues/1927
  - 결론: smq의 DB 파일, 테스트용 임시 DB, sqlite-jdbc의 네이티브 라이브러리 추출 경로는 모두 Linux FS에 둔다. 프로젝트를 `/mnt/c`에서 체크아웃해 테스트하지 않는다.

## 6. JDBC 드라이버: xerial sqlite-jdbc

- 최신 버전은 **3.53.4.0**이다(Maven Central `maven-metadata.xml`의 `lastUpdated=20260826100422`, GitHub 릴리스 2026-08-26, SQLite 3.53.4로 업그레이드). [S] https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/maven-metadata.xml , https://github.com/xerial/sqlite-jdbc/releases/tag/3.53.4.0
  - 최근 릴리스: 3.53.2.1(07-27, DATETIME→TIMESTAMP 매핑, 메타데이터 쿼리 이스케이프 수정), 3.53.2.0(06-04), 3.53.1.0(05-06), 3.53.0.0(04-14, `wal_autocheckpoint` 노출, 분류자 jar 도입).
- **Java 25 지원**:
  - 클래스는 `release 8`로 빌드되고, `module-info`는 java9 멀티릴리스로 들어간다(모듈명 `org.xerial.sqlitejdbc`). [S] https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.53.4.0/sqlite-jdbc-3.53.4.0.pom , https://github.com/xerial/sqlite-jdbc/blob/master/src/main/java9/module-info.java
  - CI 테스트 매트릭스는 JDK `[11, 21, 25]`이고, GraalVM native-image 테스트는 `[17, 21, 25]`다. 3.51.3.0 릴리스 노트에는 "use jdk 25 where possible"가 있다. [S] https://github.com/xerial/sqlite-jdbc/blob/master/.github/workflows/ci.yml , https://github.com/xerial/sqlite-jdbc/releases/tag/3.51.3.0
  - 로컬 확인: Corretto 25.0.3에서 Maven Central jar(sha1 `47e21dd5...` 일치)로 연결, WAL, 다중 프로세스 쓰기, `integrity_check`가 모두 정상이었다. [M]
- **네이티브 로딩(JNI)**:
  - 아직 JNI다. 드라이버를 로드할 때 OS에 맞는 `.so`를 `java.io.tmpdir`(또는 `-Dorg.sqlite.tmpdir`)에 추출한 뒤 `System.load`한다. 추출 파일명에 버전과 UUID가 들어가고 `.lck` 파일로 정리되므로, 여러 프로세스가 동시에 추출해도 충돌하지 않는다. [S] https://github.com/xerial/sqlite-jdbc/blob/master/src/main/java/org/sqlite/SQLiteJDBCLoader.java , https://github.com/xerial/sqlite-jdbc/blob/master/USAGE.md
  - `/tmp`가 `noexec`인 환경에서는 `-Dorg.sqlite.tmpdir`을 지정하거나, `-Dorg.sqlite.lib.path`/`org.sqlite.lib.name`으로 미리 배치한 라이브러리를 쓴다. [S] USAGE.md
  - 3.53.0.0부터 `natives-linux` 등 분류자 jar가 있어 배포 크기를 줄일 수 있다(기본 jar에는 모든 OS 네이티브 포함). [S] https://github.com/xerial/sqlite-jdbc/blob/master/README.adoc
  - FFM(Panama) 전환 요청 이슈 #717은 열려 있으며 2024-01 이후 진척이 없다. [S] https://github.com/xerial/sqlite-jdbc/issues/717
- **`--enable-native-access` 경고 (JEP 472, JDK 24+)**:
  - JDK 25.0.3에서 플래그 없이 실행하면 다음 경고가 나온다. [M]
    ```
    WARNING: java.lang.System::load has been called by org.sqlite.SQLiteJDBCLoader in an unnamed module (...sqlite-jdbc.jar)
    WARNING: Use --enable-native-access=ALL-UNNAMED to avoid a warning for callers in this module
    WARNING: Restricted methods will be blocked in a future release unless native access is enabled
    ```
  - 클래스패스에서는 `--enable-native-access=ALL-UNNAMED`, 모듈패스에서는 `--enable-native-access=org.xerial.sqlitejdbc`를 쓴다. smq 모듈 이름이 아니라 sqlite-jdbc 모듈 이름이어야 한다. [S] https://github.com/xerial/sqlite-jdbc/issues/1289 (메인테이너 답변, 고정됨)
  - jar 매니페스트의 `Enable-Native-Access`는 `java -jar`로 실행하는 실행 jar에서만 유효하고 값은 `ALL-UNNAMED`만 허용된다. 따라서 **라이브러리인 smq가 대신 설정해 줄 수 없고**, smq 사용자(애플리케이션)가 런처 옵션이나 매니페스트로 지정해야 한다. 기본값은 `--illegal-native-access=warn`이며, 앞으로 기본이 deny로 바뀌면 플래그 없이는 로드에 실패한다. [S] https://openjdk.org/jeps/472
  - smq 쪽 조치 [추론]: README에 필요한 JVM 옵션을 명시하고, Gradle `tasks.test { jvmArgs("--enable-native-access=ALL-UNNAMED") }`를 추가한다.

## 7. 프로세스 간 새 메시지 알림

- `sqlite3_update_hook`은 **콜백을 등록한 그 연결**에서 일어난 변경에만 호출된다. [S] https://www.sqlite.org/c3ref/update_hook.html
- 측정 [M]: 같은 JVM 안에서 연결 A와 B에 `SQLiteConnection.addUpdateListener`를 등록하고 A에서 INSERT했더니, A의 리스너는 1회 호출되고 B의 리스너는 0회였다. 연결이 다르면 같은 프로세스 안에서도 알림이 없으니, 프로세스가 다르면 당연히 불가능하다.
- 따라서 SQLite만으로는 **프로세스 간 푸시 알림이 불가능하다.** 가능한 대안은 다음과 같다.
  1. **폴링 (기본 권장)**
     - `PRAGMA data_version`: 다른 연결(다른 프로세스 포함)이 커밋하면 값이 바뀐다. 같은 연결에서 두 시점의 값을 비교할 때만 의미가 있다. [S] https://www.sqlite.org/pragma.html#pragma_data_version
     - 또는 `SELECT id FROM msg WHERE queue=? AND id>? ORDER BY id LIMIT 1`(인덱스 필요).
     - 측정 비용은 둘 다 호출당 약 3µs였다(Python sqlite3, ext4). [M] 폴링 자체의 CPU 비용은 무시할 만하고, 실제 비용은 지연과 wake-up 빈도다.
     - 권장 간격 [추론]: 메시지가 있으면 즉시 연속 처리하고, 비어 있으면 적응형 백오프(예: 1ms → 2 → 4 ... 상한 100~250ms)를 쓴다. 지연에 민감하지 않은 큐는 상한을 1s로 둔다. 고정 간격이라면 10~50ms가 무난하다. 폴링 쿼리는 autocommit으로 실행하고 ResultSet을 즉시 닫아 checkpoint를 막지 않게 한다(3절).
  2. **같은 프로세스 안**: 생산자와 소비자가 같은 JVM에 있으면 smq가 커밋 직후 JVM 내부 신호(Condition, LockSupport 등)로 깨운다. 이는 SQLite 기능이 아니라 smq의 기능이다. [추론]
  3. **선택적 가속 힌트** [추론, 미검증]: Linux 로컬 FS라면 `-wal` 파일 수정을 `WatchService`(inotify)로 감시하거나 Unix domain socket 사이드채널로 깨울 수 있다. 신호가 유실될 수 있으므로 폴링을 대체하지 말고 폴링 주기를 줄이는 힌트로만 쓴다. WSL `/mnt/c`의 9P에서는 inotify가 신뢰할 수 없다.

## 8. smq 권장 설정 (초안)

```text
jdbc:sqlite:/path/on/linux/fs/smq.db
  ?journal_mode=WAL
  &synchronous=NORMAL        # 전원 장애 시 마지막 커밋 유실을 허용하지 않으면 FULL
  &busy_timeout=5000         # 드라이버 기본값 3000. 쓰기 경합이 크면 상향
  &transaction_mode=IMMEDIATE
  &foreign_keys=ON           # 스키마에 따라
```

- 쓰기 트랜잭션은 짧게 유지하고(락은 수 ms 이내), 가능하면 배치로 묶는다(enqueue batch, 여러 건 ack).
- `SQLITE_BUSY`/`SQLITE_LOCKED`가 나면 트랜잭션 전체를 지수 백오프로 재시도한다.
- 장시간 read 트랜잭션을 금지한다. 유휴 시간에 주기적으로 `PRAGMA wal_checkpoint(TRUNCATE)`를 실행하거나 `journal_size_limit`을 설정해 WAL 크기를 관리한다.
- JVM 옵션은 `--enable-native-access=ALL-UNNAMED`(클래스패스) 또는 `=org.xerial.sqlitejdbc`(모듈패스)다.
- 의존성은 `org.xerial:sqlite-jdbc:3.53.4.0`이다(Linux 전용 배포라면 `natives-linux` 분류자 검토).

## 9. 미해결 및 후속 과제

- 실제 큐 패턴(enqueue + claim + ack 혼합, 여러 소비자)에서의 처리량과 꼬리 지연은 측정하지 않았다. smq 프로토타입에서 벤치로 다시 측정해야 한다.
- 베어메탈 Linux와 SSD에서의 `synchronous=FULL` 비용은 미측정이다(WSL2 VHDX의 fsync 의미는 불확실).
- `/mnt/c` 9P에서 Windows 프로세스와 WSL 프로세스가 함께 쓸 때의 락 정합성은 검증하지 않았다. 지원 범위에서 제외할 것을 권장한다.

## 부록: 측정 재현

- 드라이버: `https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.53.4.0/sqlite-jdbc-3.53.4.0.jar` (sha1 `47e21dd53efe1524fe0df6965cb65003ffe17172`)
- 프로그램: 단일 파일 Java로 `init`(테이블 `msg(id INTEGER PRIMARY KEY, body BLOB)`와 `ctr(k,v)` 생성), `enqueue <count> <batch>`(위 4절의 트랜잭션 반복, 실패하면 ROLLBACK 후 재시도 횟수 집계), `check`(행 수, 카운터, `integrity_check`)를 구현했다. 셸 스크립트는 P개의 JVM을 동시에 띄워 wall time을 측정한다. 저장소에는 커밋하지 않았다.
- 리스너 검증: 같은 DB에 연결 A와 B를 열고, 둘 다 `addUpdateListener`를 등록한 뒤 A에서 INSERT 1건을 실행했다. 결과는 `A fired=1, B fired=0`이었다.
