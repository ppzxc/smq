# 추후 기능 요구사항

smq v1 범위에서 뺀 기능 목록이다. v1 범위는 [v1 기능 범위 확정](https://github.com/ppzxc/smq/issues/5)에서 정했고, 각 기능의 RabbitMQ 쪽 배경은 [RabbitMQ 핵심 기능 조사](../research/rabbitmq-core-features.md)에 있다.

v1의 목표: **청크 단위로 꺼내 ACK/NACK하는, 재시도 가능한 내구성 큐.** 아래 기능은 그 목표 밖이다. 다음 버전의 지도를 그릴 때 출발점으로 쓴다.

## 이후 버전 후보

### 라우팅

- **Exchange와 binding**: 큐 대신 exchange에 발행하고 binding 규칙으로 큐에 라우팅한다. v1은 큐에 직접 발행한다.
- **Fanout**: 바인딩된 모든 큐에 복사본을 보낸다.
- **Topic**: `*`, `#` 패턴으로 routing key를 매칭한다.
- **Headers exchange**: 헤더 값으로 라우팅한다.
- **Exchange-to-exchange binding**: 다단계 라우팅. 순환 감지가 필요하다.
- **Alternate exchange**: 라우팅되지 않은 메시지를 받을 exchange. v1에는 라우팅이 없으므로 해당하지 않는다.

### 순서와 소비

- **키 단위 병렬 순서**: 순서 키(파티션 키)가 같은 메시지끼리만 순서를 지키고, 키가 다르면 병렬로 처리한다. RabbitMQ의 `x-modulus-hash`로 큐를 나누는 방식에 해당한다.
- **막힌 큐 정지 옵션**: 최대 전달 횟수를 넘긴 메시지를 dead letter 큐로 옮기는 대신 큐를 멈추고 운영자 개입을 기다린다.
- **구독 헬퍼**: 라이브러리가 폴링 스레드를 돌리며 청크 콜백을 호출하는 push 형태의 API. v1은 pull만 제공한다.
- **Priority 큐**: 높은 priority 메시지를 먼저 전달한다. 인덱스 설계와 기아 문제를 고려해야 한다.

### 보존과 용량

- **메시지 TTL**: 큐 단위 기본값과 메시지 단위 값으로 만료시키고, 만료되면 dead letter 큐로 보낸다.
- **ACK된 메시지 보관**: v1은 ACK하면 즉시 삭제한다.
- **Purge**: 큐의 메시지를 한꺼번에 지운다.
- **큐 길이 제한과 overflow 정책**: 개수나 바이트로 제한하고, 넘치면 앞쪽을 버리거나 발행을 거부한다.
- **큐 속성 변경**: 선언한 뒤 최대 전달 횟수나 dead letter 큐 같은 속성을 바꾼다.

### 트랜잭션

- **사용자 트랜잭션**: "ACK와 다음 발행"처럼 여러 연산을 하나로 묶는다. v1은 발행 배치만 원자적으로 처리한다.
- **Transactional outbox**: 애플리케이션의 DB 연결을 공유해 업무 데이터와 발행을 함께 커밋한다. SQLite와 PostgreSQL 백엔드에서만 성립한다.

### 백엔드와 연동

- **PostgreSQL 백엔드 구현**: v1에서는 SPI가 수용할 수 있는지만 검증한다.
- **CloudEvents binding**: smq 메시지와 CloudEvent를 binary mode(`ce_` 헤더)로 변환하는 선택적 모듈. v1 메시지 헤더는 이 변환이 정보 손실 없이 가능하도록 문자열 키-값으로 둔다.
- **Stream**: 소비해도 지우지 않는 append-only 로그와 컨슈머별 오프셋.

## 영구 제외

smq의 형태(임베디드, 단일 호스트, 연결 개념 없음)에서 성립하지 않거나 다른 수단으로 대체되는 기능이다.

- **Publisher confirms**: 커밋한 뒤 반환하는 동기 발행이 그 자체로 confirm이다.
- **Exclusive / auto-delete / server-named 큐**: 연결이나 컨슈머 세션의 수명에 묶인 기능이다.
- **Exclusive consumer, consumer priority**: 단일 활성 컨슈머로 충분하다.
- **큐 TTL** (안 쓰는 큐 자동 삭제): 컨슈머 생존을 판단할 연결이 없다.
- **Quorum queue 복제**: 다중 호스트 클러스터링은 범위 밖이다. delivery limit 같은 의미론은 v1에 이미 들어 있다.
- **Lazy queue**: 디스크 기반 백엔드에서는 의미가 없다.
- **메시지 단위 durable/transient 구분**: 영속성은 백엔드 선택(SQLite 또는 인메모리)으로 정한다.
- **AMQP 와이어 호환, 독립 브로커 서버, 클러스터링, 관리 UI**: 지도 수준에서 범위 밖이다.
