# 벤치 하네스

smq v1 성능 목표(스펙 #23의 "성능 목표")를 재고, 기준선과 견주어 회귀를 막는 하네스다. 코드는 같은 모듈의 `benchmark` 소스셋(`src/benchmark/java`)에 있고 배포 jar와 소스 jar에는 들어가지 않는다(`./gradlew check`가 `verifyJarExcludesBenchmark`로 jar 내용을 검사한다). JMH는 쓰지 않는다.

## 동작 방식

부모 JVM(`BenchMain`)이 조건마다 새 SQLite 파일을 만들고 큐를 선언해(100만 건 깊이면 미리 채워) 둔 뒤, 자식 JVM(`ChildMain`)을 프로세스 수만큼 띄운다. 자식들은 같은 파일을 열고, 부모가 정한 같은 시각까지 JIT를 데운 뒤 그 시각부터 같은 길이의 구간을 잰다. 자식은 결과를 JSON 한 줄로 표준 출력에 내고, 부모가 합산한다.

- S1: 발행 배치, 꺼내기, 청크 전체 ACK를 반복한다. 처리량은 모든 자식이 구간 안에 ACK한 메시지 수의 합을 가장 느린 자식의 구간 길이로 나눈 값(msg/s)이다.
- S2: 발행만 반복한다. 처리량 계산은 S1과 같다.
- S3: 프로듀서가 50ms마다 발행 배치(P1은 100건)를 내고, 각 메시지에 발행 직전 시각을 적는다. 컨슈머가 청크를 꺼낸 직후 시각과의 차이를 지연으로 모아 p50/p99(ms, nearest-rank)를 낸다. 같은 프로세스는 한 JVM의 두 스레드, 다른 프로세스는 프로듀서 JVM과 컨슈머 JVM을 따로 띄운다.
- 발행 배치 크기와 청크 크기는 프로파일이 정한다(P0 256B/1, P1 256B/100, P2 2KB/300, P3 6KB/500).

측정 조건은 스펙 표를 그대로 따른다.

| 시나리오 | 조건 |
|---|---|
| S1 | P1~P3 x 프로세스 {1, 4} x {POWER_LOSS, PROCESS_CRASH}, 빈 큐. P0은 프로세스 1, 빈 큐. P1은 100만 건 깊이도 같은 조합으로 잰다 |
| S2 | P1~P3, 프로세스 1, 빈 큐 |
| S3 | P1만, 같은 프로세스와 다른 프로세스 (POWER_LOSS) |

## 태스크

- `./gradlew benchmarkSmoke`: 짧은 스모크(조건당 약 1.5초). 다중 JVM, 다른 프로세스 S3, 미리 채운 깊이 경로를 한 번씩 지나가는지만 본다. 임계값은 없고 자식이 실패하지 않으면 통과한다. 수치는 의미가 없다.
- `./gradlew benchmark`: 전체 측정(23개 조건, 조건마다 측정 10초와 예열 3초에 준비 시간이 더해진다. 100만 건 깊이는 준비가 더 걸린다). 수동으로만 돌린다. 결과를 `build/benchmark/result.json`에 쓰고, `benchmark/baseline.json`이 있으면 기준선과 견준다. FAIL이 하나라도 있으면 비정상 종료한다.

`benchmark`에는 `-Pbenchmark.args="..."`로 옵션을 더할 수 있다.

| 옵션 | 뜻 |
|---|---|
| `--filter <문자열>` | 키에 이 문자열이 든 조건만 돈다. 예: `--filter S1/P1/proc1` |
| `--storage memory` | 인메모리로 돈다. 코어 오버헤드를 보는 진단값이고 임계값은 적용하지 않는다. 한 JVM에서 도는 조건만 있다 |
| `--work-dir <디렉터리>` | SQLite 파일을 만들 곳. 기본은 `build/benchmark/work` |
| `--output <파일>` | 결과 파일 위치 |

## 기준 환경에서 돌리기

1. 베어메탈 Linux에서, 로컬 NVMe의 ext4 위 체크아웃을 쓴다. 작업 디렉터리가 tmpfs나 네트워크 파일 시스템이면 fsync 비용이 맞지 않으므로 `--work-dir`로 NVMe ext4 경로를 준다. 결과 파일의 `environment.workDirFs`에 파일 시스템 종류가 남는다.
2. 다른 부하가 없는 상태에서 `./gradlew benchmark`를 돌린다. 인메모리 진단값이 필요하면 `./gradlew benchmark -Pbenchmark.args="--storage memory"`를 따로 돌린다(JVM 힙은 100만 건 깊이를 담을 만큼 필요하다).
3. 그런 장비가 없으면 WSL2 ext4에서 재도 된다. 이때 결과 파일의 `environment.caveat`에 "fsync 비용을 과소평가할 수 있다"는 경고가 자동으로 들어간다. 결과를 공유할 때 이 경고를 함께 적는다.
4. 첫 기준선을 정할 때는 `build/benchmark/result.json`을 `benchmark/baseline.json`으로 복사해 커밋한다. 하한이 터무니없으면 그때 한 번만 조정한다. 릴리스 전에는 같은 환경에서 다시 돌려 PASS를 확인한다.

## 결과와 기준선 파일 형식

결과 파일과 기준선 파일은 같은 형식이라 결과를 그대로 기준선으로 쓸 수 있다.

```json
{
  "environment": {"os": "Linux 6.x", "cpus": "16", "workDirFs": "ext4", "caveat": "..."},
  "entries": {
    "sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec": 12345.6,
    "sqlite/S3/P1/cross-process/POWER_LOSS/empty:p99Ms": 41.2
  }
}
```

항목 id는 `<저장소>/<시나리오>/<프로파일>/<형태>/<내구성>/<깊이>:<지표>`다. 형태는 `proc1`, `proc4`, `same-process`, `cross-process`, 깊이는 `empty` 또는 `1m`이다. 지표는 `msgPerSec`(높을수록 좋음), `p50Ms`, `p99Ms`(낮을수록 좋음)다.

## 비교 보고

비교기는 SQLite 항목만 검사하고 항목마다 한 줄로 보고한다.

```
PASS  floor S1 P1 proc1 POWER_LOSS >= 10000 msg/s  [12345 msg/s]
FAIL  scaling S1 P2 PROCESS_CRASH proc4 >= 70% of proc1  [5200 / 9000 = 57.8%]
SKIP  regression sqlite/S2/P1/proc1/POWER_LOSS/empty:msgPerSec  [기준선에 없다]
합계: 통과 20, 실패 1, 건너뜀 1
```

| 검사 | 기준 |
|---|---|
| `floor` | 1프로세스 빈 큐 POWER_LOSS의 S1이 P1 10,000, P2 8,000, P3 4,000 msg/s 이상. P0은 기록만 한다 |
| `scaling` | 4프로세스 S1이 1프로세스 값의 70% 이상(P1~P3, 내구성 수준마다) |
| `depth` | P1의 100만 건 깊이 S1이 같은 조건 빈 큐 값의 70% 이상 |
| `latency` | S3 p99가 같은 프로세스 10ms 이하, 다른 프로세스 260ms 이하 |
| `regression` | 기준선에 있는 항목이 20% 넘게 나빠지면 FAIL(처리량은 줄어듦, 지연은 늘어남). 정확히 20%는 통과 |

측정하지 않았거나 기준선에 없는 항목은 SKIP이고 실패가 아니다. `--filter`로 일부만 돌리면 나머지 검사는 SKIP이 된다. FAIL이 하나라도 있으면 종료 코드가 1이다.

## CI

CI에서는 임계값 없는 `benchmarkSmoke`만 돌린다. 이 저장소에는 아직 CI 설정이 없어 연결하지 않았다.
