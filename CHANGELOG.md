# Changelog

이 프로젝트의 주요 변경을 이 파일에 적는다. 형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따르고, 0.x 동안은 API 호환을 약속하지 않는다.

## [Unreleased]

### Added

- 실행 중인 SQLite 저장소의 일관된 사본을 만드는 `Smq.snapshot(Path)`를 추가했다. `VACUUM INTO`를 자바 락 아래에서 돌리므로 그동안 다른 연산이 DB 크기에 비례해 멈춘다. 대상이 이미 있으면 덮어쓰지 않고 예외를 던진다. 인메모리 저장소는 지원하지 않는다. (#127, ADR-0013)

### Removed

- 한 JVM 전용(ADR-0012)이 되어 쓸 곳이 없는 다중 프로세스용 저장소 런타임 코드(BUSY 재시도, 트랜잭션마다 하는 `user_version` 확인, WAL salt 감지)를 걷어냈다. (#126)

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
