# Potato Pyramid #210 검증 증거

- 독립 기준 모델과 고정 가상 시계 명령열은 `StorageContractTest`에서 인메모리와 SQLite 저장소에 각각 적용한다. 만료 정각에 같은 메시지가 재전달되고, 이전 리스 정산은 `STALE`, 새 리스 정산은 `OK`이며 기록된 상태 수가 모델과 일치한다.
- 동시에 시작한 만료 정각의 `pull`과 이전 리스 `ACK`는 두 직렬 순서 중 하나만 허용한다. `ACK`가 먼저면 메시지가 없고, 재꺼내기가 먼저면 이전 `ACK`가 `STALE`이며 새 리스만 남는다.
- 축소 테스트의 재현 명령열은 `publish → pull → advance to lease deadline → pull`이다. strict expiry 변이(`deadline < now`)와 정상 비교(`deadline <= now`)의 전달 수가 다르며, 한 단계를 제거하면 차이가 사라진다.
- 표적 코드 변이 확인: 메모리 저장소 만료 비교를 `<`로 바꿨을 때 계약 테스트 13건이 실패했다. SQLite 만료 SQL을 `<`로 바꿨을 때 26건이 실패했다. 두 변경을 원복한 뒤 모델·계약 테스트가 통과했다.
- Linux Java 25에서 `StorageContractMemoryTest`, `StorageContractSqliteTest`, `LeaseCounterexampleTest`가 통과했고, 실행 시간은 28.04초였다.
- 모델 테스트를 포함한 현재 통합 SHA `ec5715d`에서 PR #219의 전체 검사 잡 시간을 기록했다: [linux-build 2m37s](https://github.com/ppzxc/smq/actions/runs/45531669122/job/138928337591), [windows-build 4m58s](https://github.com/ppzxc/smq/actions/runs/45531669116/job/138928226039). 이는 모델 테스트만의 시간이 아니라 두 OS의 전체 `./gradlew check` 잡 시간이다. Linux에서 모델·계약 테스트 자체는 28.04초였다.
