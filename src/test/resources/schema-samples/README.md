# 스키마 견본

출시한 스키마 버전마다 견본 파일 하나를 이 폴더에 둔다. 스키마를 올릴 때 옛 견본이 업그레이드되고 일을 잃지 않는지 `SchemaV1SampleTest`가 확인한다. 평소 테스트는 커밋된 바이너리만 열고, 견본을 새로 만들지 않는다.

| 파일 | `user_version` | 내용 |
| --- | --- | --- |
| `schema-v1.db` | 1 | 아래 표 |

## schema-v1.db 내용

시각 `2026-01-01T00:00:00Z`(T0)에 고정하고, 리스 시간은 1시간이다.

| 큐 | 선언 | 상태 |
| --- | --- | --- |
| `v1-dead` | 기본 옵션 | 준비 1: `poison` (v1-orders에서 reject됨) |
| `v1-orders` | 최대 전달 3회, dead letter 큐 `v1-dead` | 준비 2: `m3` `m4` / 리스 중 2: `m1`(전달 2회) `m2` / 지연됨 1: `m5` (T0+2h) |
| `v1-serial` | 단일 활성 컨슈머 | 리스 중 1: `s1` / 준비 1: `s2` |

리스 기한은 모두 T0+1h다.

## 다시 만드는 방법

스키마를 바꾸지 않았다면 다시 만들 필요가 없다. 옛 견본은 업그레이드 회귀 테스트의 입력이므로 새 코드로 덮어쓰지 않는다.

새 스키마 버전 N을 출시할 때는 견본 `schema-vN.db`를 더한다. 생성기 `src/test/java/io/github/ppzxc/smq/SchemaSampleGenerator.java`는 공개 API(`Smq.sqlite`)와 고정 시계만 쓰므로 그 버전의 코드에서 실행한 결과가 그 버전의 견본이다.

```
./gradlew generateSchemaSample
```

기본 출력은 `src/test/resources/schema-samples/schema-v1.db`이고, 다른 경로는 인자로 준다(`./gradlew generateSchemaSample --args="경로"`). 생성기를 바꿨다면 `SchemaV1SampleTest`의 기대값(큐 선언, 개수, 메시지 id, 전달 횟수, 헤더)도 함께 맞춘다.
