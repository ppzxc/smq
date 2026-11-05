# Changelog

이 프로젝트의 주요 변경을 이 파일에 적는다. 형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따르고, 0.x 동안은 API 호환을 약속하지 않는다.

## [Unreleased]

### Added

- 벤치마크 하네스 `randomBench`에 `--durability <POWER_LOSS|PROCESS_CRASH>` 옵션을 추가했다. 주지 않으면 시드에서 무작위로 고르던 대로이고, 주면 그 수준으로 고정한다. (#156)
- 실행 중인 SQLite 저장소의 일관된 사본을 만드는 `Smq.snapshot(Path)`를 추가했다. `VACUUM INTO`를 자바 락 아래에서 돌리므로 그동안 다른 연산이 DB 크기에 비례해 멈춘다. 대상이 이미 있으면 덮어쓰지 않고 예외를 던진다. 인메모리 저장소는 지원하지 않는다. (#127, ADR-0013)

### Changed

- 벤치마크 하네스가 자식 JVM을 종료할 때 정상 종료 신호와 유예 시간 없이 곧바로 강제 종료한다. 실패한 실행의 JFR 덤프가 종료 훅에 기대던 근거는 이미 없어졌고(단일 JVM), 이제 자식 JVM의 JFR은 종료 시점에 덤프되지 않는다. (#156)
- **호환 파괴**: SQLite 저장소를 한 JVM만 열 수 있게 했다. 열 때 DB 옆의 락 파일(`<DB 파일명>.lock`)에 OS 배타 락을 잡고 연결을 `locking_mode=EXCLUSIVE`로 연다. 다른 프로세스나 같은 JVM의 중복 열기, 외부 도구가 파일을 쥔 경우에는 기다리지 않고 `StorageLockedException`이 올라온다. 같은 파일을 여러 프로세스가 공유하던 사용은 둘째 프로세스의 `open`이 실패한다. 실행 중에는 sqlite3 CLI 같은 외부 도구로 DB 파일을 읽을 수 없고, 대신 `Smq.snapshot(Path)`로 만든 스냅샷을 읽는다. (#128, ADR-0012)
- **호환 파괴**: `Smq.SqliteBuilder.lockWaitFloor(Duration)`을 공개 API에서 뺐다. lock 대기 하한은 기본 100ms 그대로이고, `wait`가 0인 꺼내기도 정상적인 짧은 직렬화는 기다린다. 이 옵션을 쓰던 빌드는 컴파일 오류가 난다. (#155)
- **호환 파괴**: SQLite 저장소의 기본 내구성 수준을 `Durability.POWER_LOSS`에서 `Durability.PROCESS_CRASH`로 바꿨다. 기본값은 애플리케이션 크래시까지 견디고, 전원 장애나 OS 크래시 때는 마지막 커밋 몇 개를 잃을 수 있다. 전원·OS 장애까지 견뎌야 하면 빌더에서 `.durability(Durability.POWER_LOSS)`를 명시한다. 그 보장은 저장 장치가 fsync를 지킬 때만 성립한다. v1 성능 하한은 계속 `POWER_LOSS`로 잰다. (#146, ADR-0014)
- 벤치마크 결과와 기준선의 항목 키에서 형태 구성요소(`proc1`, `same-process`)를 뺐다. 예: `sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec`는 `sqlite/S1/P1/POWER_LOSS/empty:msgPerSec`가 된다. `--filter`에 옛 키를 쓰던 스크립트는 고쳐야 한다. (#129)
- 벤치마크 프로파일 번호를 데이터 크기 순으로 다시 매겼다. 같은 크기에서 짝수가 단건(청크 1), 홀수가 묶음이다: P0 256B/1, P1 256B/100, P2 2KB/1, P3 2KB/300, P4 4KB/1, P5 4KB/400, P6 6KB/1, P7 6KB/500. 옛 이름과의 대응은 P2S→P2, P2→P3, P4S→P4, P4→P5, P3S→P6, P3→P7이다(P0, P1은 그대로). 결과와 기준선의 항목 키도 바뀌므로 `--filter`에 옛 이름을 쓰던 스크립트는 고쳐야 한다. (#144)

### Fixed

- 체크포인트 확인에서 WAL 파일 크기를 읽지 못하면 "한도 아래"로 보고 연속 실패 횟수를 지우던 문제를 고쳤다. 이제 체크포인트 실패와 구분되는 경고를 남기고(연속 실패와 같은 간격으로 억제) 횟수를 지우지 않는다. 파일이 없는 데이터베이스에서 WAL 위치를 8커밋마다 다시 묻던 것도 한 번만 묻게 했다. (ADR-0017)
- 커밋 뒤 WAL 체크포인트의 SQL 오류를 더 이상 `SmqStorageException`(효력 불명)으로 올리지 않는다. 이미 커밋된 발행이나 정산이 정상 결과를 돌려주고, 오류는 경고 로그(`System.Logger`)만 남기며 다음 정규 확인(커밋 8번 뒤)이 되감기를 다시 시도한다. 호출자가 커밋된 발행을 실패로 보고 재시도해 중복이 생기던 문제를 없앤다. (ADR-0017)
- 다시 열 때도 `-shm` 없이 `locking_mode=EXCLUSIVE`로 연다. sqlite-jdbc는 설정 pragma의 실행 순서를 보장하지 않아 `synchronous`가 EXCLUSIVE보다 먼저 돌았고, 이미 WAL인 파일을 다시 열면 `-shm`이 생겼다. 이제 연결 직후 `locking_mode`, `synchronous` 순서로 직접 실행한다. (#153, #148)

### Removed

- 쓰지 않던 `ChildJvm`의 오버로드와 접근자(표준 오류 파일·JVM 옵션을 받는 `start`, `exitCode`, `requestStop`, 유예 시간을 받는 `terminate`)를 걷어냈다. (#156)
- 한 JVM 전용(ADR-0012)이 되어 쓸 곳이 없는 다중 프로세스용 저장소 런타임 코드(BUSY 재시도, 트랜잭션마다 하는 `user_version` 확인, WAL salt 감지)를 걷어냈다. (#126)
- 벤치마크 하네스의 `randomBench --heap-dump` 옵션을 없앴다. 자식 JVM에만 적용되던 옵션이라 단일 JVM에서는 쓸모가 없었다. 이 옵션을 넘기던 스크립트는 오류가 난다. (#126)

## [0.0.1] - 2026-10-28

### Added

- 첫 공개. Maven Central에 `io.github.ppzxc:smq`로 배포한다.
- 애플리케이션 프로세스 안에서 동작하는 Java 25 내구성 메시지 큐. 별도 브로커 서버 없이 라이브러리 의존성 하나로 쓴다.
- SQLite 파일 저장소와 인메모리 저장소. 같은 공개 API로 쓴다. SQLite 저장소는 같은 호스트의 여러 프로세스가 파일 하나를 공유해 큐를 나눠 소비할 수 있다.
- 내구성 수준 `Durability.POWER_LOSS`(기본, 전원 장애까지 견딤)와 `PROCESS_CRASH`.
- 청크 단위 소비: `pull`로 여러 건을 하나의 리스로 꺼내고, 메시지마다 ACK/NACK/reject를 골라 한 번에 정산한다.
- at-least-once 전달, 지연 NACK, 최대 전달 횟수, dead letter 큐, 지연 발행, 큐 안 FIFO 순서, 단일 활성 컨슈머 큐.
- Linux와 Windows 지원(DB 파일은 로컬 파일시스템에 둔다).

[Unreleased]: https://github.com/ppzxc/smq/compare/0.0.1...HEAD
[0.0.1]: https://github.com/ppzxc/smq/releases/tag/0.0.1
