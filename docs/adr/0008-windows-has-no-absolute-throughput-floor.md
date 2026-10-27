# Windows에는 절대 처리량 하한을 두지 않고, 기준선과 릴리스 판정은 Linux 기준 환경에만 적용한다

Windows(로컬 NTFS)의 처리량은 Linux 기준 환경보다 크게 낮고, 원인은 smq 코드가 아니라 플랫폼의 파일 I/O 비용이다. 그래서 Linux 하한(P1 10,000, P2 8,000, P3 4,000 msg/s)과 `benchmark/baseline.json`은 그대로 두고 Windows에는 절대 하한을 두지 않는다. Windows 결과는 기록만 하고 릴리스를 막는 데 쓰지 않는다. [ADR 0006](0006-windows-support-is-conditional-on-verification.md)이 미룬 하한 결정이 이것이다. 설정으로 푸는 방안은 택하지 않는다. 격차가 내구성 수준이나 배치 크기로 조절되는 값이 아니라 장비와 OS의 속성이기 때문이다.

## 원인 조사 (2026-10-27)

장비는 Windows 11, i9-14900HX, 삼성 NVMe(소비자용), 로컬 NTFS, JDK 25.0.3이다. 안티바이러스는 AhnLab V3가 실시간으로 켜져 있고 Defender는 꺼져 있다. 같은 장비의 WSL2 ext4와 견줬다.

- **fsync 비용**: 파일에 4KB를 쓰고 `force`하면 초당 약 480회다(회당 약 2ms). 쓰기 트랜잭션마다 fsync가 하나이고 단건은 사이클당 3회라서 POWER_LOSS 단건이 약 80~90 msg/s(Linux 기준 환경 720)로 떨어진다. 기준 환경은 배터리 백업 쓰기 캐시라 fsync가 훨씬 싸다. 소비자용 NVMe에는 전원 손실 보호 캐시가 없어 fsync가 매번 매체까지 간다.
- **fsync가 아닌 쓰기 비용**: `synchronous=OFF`인 단독 SQLite WAL 삽입(6KB, 500건 트랜잭션)이 Windows 약 8.4k msg/s, WSL2 약 104k msg/s다. 원시 4KB 파일 쓰기도 Windows 약 52k/s, WSL2 약 400k/s다. fsync를 끈 상태에서도 큰 격차가 있다. 하네스의 PROCESS_CRASH(fsync 없음)에서도 256B는 Linux와 같거나 더 빠르지만(41k 대 37k) 2KB 이상은 Linux의 40~60%다. 바이트가 늘수록 비용이 커진다.
- **격리하지 못한 것**: 이 쓰기 비용에 AhnLab V3의 파일 시스템 필터가 얼마나 기여하는지는 확인하지 못했다. 제외 설정과 `fltmc`에 관리자 권한이 필요했다. 같은 이유로 Defender 영향도 보지 않았다. 안티바이러스 제외 경로에서 다시 재면 격차가 줄어드는지 알 수 있다.
- **smq 코드 문제는 아니다**: 격차는 smq 없이 sqlite-jdbc만으로도 같은 모양으로 재현된다. 체크포인트는 주원인이 아니다(`wal_autocheckpoint=0`이어도 약 12k).

## 하네스 측정 (Windows, 1회, 기준선 대비)

| 항목 | Windows | 기준선(ns0270) |
|---|---|---|
| S1 P1 POWER_LOSS 1프로세스 | 7,676 | 16,612 |
| S1 P2 POWER_LOSS | 5,204 | 9,797 |
| S1 P3 POWER_LOSS | 2,361 | 5,583 |
| S1 P1 PROCESS_CRASH | 41,390 | 36,789 |
| S1 P0/P2S/P3S/P4S POWER_LOSS(단건) | 82~93 | 632~722 |
| 4프로세스 ÷ 1프로세스 | 대부분 70% 이상 (P1 PROCESS_CRASH만 68%) | |
| 100만 건 깊이 ÷ 빈 큐 | POWER_LOSS 86%, PROCESS_CRASH 43% | |
| S3 p99 같은 프로세스 | 29.7ms | 7.0ms |

이전에 기록한 891 msg/s(P3)는 우회 패치한 하네스와 짧은 측정이라 흔들렸다. 이번 측정은 1,142~2,361 사이다. 같은 조건에서도 회차 편차가 크므로 Windows 수치는 한 자리 정확도로 읽지 않는다.

## Considered Options

- Linux 하한을 Windows에도 적용한다: 이 장비에서는 항상 FAIL이라 Windows 지원을 선언할 수 없게 된다. 하한은 기준 환경(배터리 백업 캐시)의 하드웨어 성질을 전제로 정한 값인데 그 전제가 Windows 소비자용 장비에는 없다.
- Windows 전용 절대 하한을 둔다: 장비마다 fsync 비용이 달라서 한 장비에서 잰 값을 하한으로 못 박으면 다른 Windows 장비에서 의미가 없다. 안티바이러스 구성에 따라서도 달라진다.
- 설정으로 푼다(하한 값을 사용자가 정한다): 하네스는 개발자 도구이고 릴리스 판정은 기준 환경에서만 하므로(`benchmark/README.md` 릴리스 절차 6) 설정할 대상이 없다.

## 결과

- Windows 사용자에게는 README에서 처리량이 Linux 기준 환경보다 낮고 특히 POWER_LOSS 단건이 fsync 비용에 묶인다고 알린다. 배치로 발행하면 격차가 크게 줄어든다.
- Windows에서 성능이 중요하면 PROCESS_CRASH를 쓰거나 배치를 키우고, 안티바이러스 제외 경로에 DB 폴더를 두는 것이 효과가 있을 수 있다(검증 전이다).
