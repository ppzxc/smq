# RabbitMQ 핵심 기능 조사 및 중요도 순위

- 티켓: [#2](https://github.com/ppzxc/smq/issues/2) (지도: [#1](https://github.com/ppzxc/smq/issues/1))
- 조사일: 2026-10-23
- 기준 버전: RabbitMQ 4.4 문서 (rabbitmq.com/docs, 2026-10-19 출시된 v4.4.0 릴리스 노트 포함)
- 출처: rabbitmq.com 공식 문서와 AMQP 0-9-1 모델 설명, rabbitmq 공식 GitHub 저장소. 본문 인용은 원문을 한국어로 옮긴 것이고, 각 항목에 출처 URL을 달았다.

## 순위 기준

순위는 **범용 메시지 큐로서의 중요도**로 매겼다. smq 환경에 맞는지는 순위에 섞지 않고 별도 열로 표시한다. 그래서 "SQLite에서는 의미 없음"인 기능도 순위가 높을 수 있다.

1. **신뢰성 필수성**: 이 기능이 없으면 메시지를 잃지 않고 한 번 이상 전달할 수 없는가.
2. **의존도**: 다른 기능이 이 기능 위에 얹히는가. 예: DLX는 reject와 TTL 위에 있다.
3. **사용 빈도**: 공식 튜토리얼과 가이드가 기본으로 다루는가, 특수 상황용인가.

순위는 위 기준에 따른 판단이다. 문서가 순위를 정해 주지는 않는다.

### smq 관련도 표기

| 표기 | 뜻 |
|---|---|
| 높음 | v1에서 사실상 필요하다. 없으면 메시지 큐라고 부르기 어렵다. |
| 중간 | 의미는 있지만 v1 범위는 #5에서 정한다. |
| 낮음 | 개념만 빌리거나 다른 형태로 대체된다. |
| 해당없음 | 클러스터, 네트워크 연결, 와이어 프로토콜에 묶인 기능이라 임베디드 단일 호스트에서는 성립하지 않는다. |

smq 전제: 임베디드 Java 라이브러리, AMQP 와이어 비호환, 단일 호스트에서 여러 프로세스가 SQLite 파일 하나를 공유, 백엔드 교체 가능(인메모리, PostgreSQL).

## 요약 순위표

| 순위 | 기능 | smq 관련도 |
|---|---|---|
| 1 | Queue (FIFO 저장과 전달) | 높음 |
| 2 | Consumer ack / nack / reject와 재전달 (at-least-once) | 높음 |
| 3 | Durable / persistent 메시지 (재시작 후 생존) | 높음 (모델이 달라짐) |
| 4 | Exchange, Binding, Routing key (direct, default exchange) | 높음 |
| 5 | Fanout exchange | 높음 |
| 6 | Topic exchange | 중간~높음 |
| 7 | Prefetch (QoS) | 중간 |
| 8 | Dead Letter Exchange (DLX) | 높음 |
| 9 | TTL (메시지 TTL, 큐 TTL) | 중간 |
| 10 | Publisher confirms | 낮음 (커밋 후 반환으로 대체) |
| 11 | Delivery acknowledgement timeout (consumer_timeout) | 높음 (리스/가시성 타임아웃의 근거) |
| 12 | Delivery limit (poison message 처리) | 높음 |
| 13 | Queue length limit와 overflow 정책 | 중간 |
| 14 | 지연 메시지 (delayed messages) | 중간 |
| 15 | Priority queue | 중간 |
| 16 | Single Active Consumer, exclusive consumer, consumer priority | 중간 |
| 17 | Headers exchange | 낮음 |
| 18 | Alternate exchange와 mandatory 플래그 (unroutable 처리) | 중간 |
| 19 | Exchange-to-exchange binding | 낮음 |
| 20 | 트랜잭션 (tx.select / commit / rollback) | 중간 (RabbitMQ보다 강하게 제공 가능) |
| 21 | Exclusive / auto-delete 큐, server-named 큐 | 낮음 |
| 22 | Stream (append-only 로그, 오프셋 재생) | 낮음 (v1 이후 후보) |
| 23 | Quorum queue (Raft 복제) | 해당없음 (의미론 일부만 차용) |
| 24 | Lazy queue | 해당없음 (RabbitMQ에서 이미 폐지) |

## 기능별 상세

### 1. Queue

- **설명**: 메시지를 저장했다가 컨슈머에게 FIFO 순서로 전달하는 버퍼. 속성은 이름, durable, exclusive, auto-delete, arguments(x-arguments)다.
- **순위 근거**: 다른 모든 기능이 큐 위에 얹힌다. 문서는 큐가 FIFO이고 RabbitMQ가 "메시지 순서를 보존하려고 시도한다"고 쓴다. 다만 priority와 재전달은 순서를 바꿀 수 있다. "broker는 여전히 FIFO로 dequeue하지만 재전달은 순서를 바꿀 수 있다." 순서를 엄격하게 지키려면 stream이나 single active consumer를 쓰라고 권한다.
- **smq**: 높음. 지도 #1의 미정 항목 "메시지 순서 보장의 범위"에 대한 RabbitMQ의 답은 이렇다. 큐 단위 FIFO는 기본이지만 재전달이 일어나면 순서가 보장되지 않고, 엄격한 순서는 SAC로 얻는다. smq도 같은 경계를 그을 수 있다.
- 출처: https://www.rabbitmq.com/docs/queues , https://www.rabbitmq.com/tutorials/amqp-concepts

### 2. Consumer ack / nack / reject와 재전달

- **설명**: 컨슈머가 처리 결과를 broker에 알린다. `basic.ack`는 긍정, `basic.reject`는 단건 부정, `basic.nack`는 `multiple` 플래그를 지원하는 RabbitMQ 확장이다. `requeue=true`면 다시 큐에 넣고, `false`면 DLX로 보내거나 버린다. 재전달되는 메시지에는 `redelivered=true`가 붙는다.
- **순위 근거**: at-least-once 전달의 핵심이다. 문서는 자동 ack를 "안전하지 않으며 모든 워크로드에 맞지는 않는다"고 경고한다. ack되지 않은 메시지는 채널이나 연결이 닫히면 다시 큐에 들어가므로 컨슈머는 멱등해야 한다. 같은 컨슈머에게 즉시 requeue하면 비용이 큰 루프가 생길 수 있다는 경고도 있다.
- **세부**: delivery tag는 채널마다 단조 증가하는 정수이고, ack는 메시지를 받은 그 채널로 보내야 한다.
- **smq**: 높음. 가장 큰 차이는 **재전달 트리거**다. RabbitMQ는 연결이나 채널이 닫히는 것을 감지해서 재전달한다. 반면 SQLite 파일 하나를 공유하는 여러 프로세스 사이에는 끊김을 감지할 연결이 없다. 그래서 smq는 **리스(가시성 타임아웃)** 로 재전달을 판단해야 한다(11번 참고, #6에서 결정). delivery tag는 메시지 ID나 리스 토큰으로 바뀐다.
- 출처: https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/tutorials/amqp-concepts

### 3. Durable / persistent 메시지

- **설명**: 큐와 exchange의 durable 속성은 broker 재시작 후에도 그 정의가 남는지를 정한다. 메시지가 남는지는 메시지의 delivery mode(1=transient, 2=persistent)가 따로 정한다. durable 큐에 있어도 transient 메시지는 복구할 때 버려진다.
- **순위 근거**: 재시작이나 장애 뒤에도 메시지가 남는다는 보장이 없으면 큐는 메모리 버퍼에 불과하다. AMQP 개념 문서는 durable 큐만으로는 메시지가 영속되지 않고 "전부 메시지 자체의 persistence mode에 달려 있다"고 쓴다. 참고로 4.3.0부터 non-exclusive transient classic 큐는 기본적으로 선언할 수 없다(deprecated).
- **smq**: 높음이지만 모델이 다르다. SQLite 백엔드에서는 모든 것이 디스크에 저장되므로 메시지마다 durable/transient를 구분할 필요가 약하다. "transient"는 사실상 **인메모리 백엔드**를 고르는 것에 해당한다.
- 출처: https://www.rabbitmq.com/docs/queues , https://www.rabbitmq.com/tutorials/amqp-concepts , https://www.rabbitmq.com/docs/publishers

### 4. Exchange, Binding, Routing key (direct, default exchange)

- **설명**: 프로듀서는 큐가 아니라 exchange에 발행한다. exchange는 binding 규칙에 따라 메시지를 큐로 라우팅한다. direct exchange는 routing key와 binding key가 정확히 같은 큐로 보낸다. default exchange는 이름이 없는 direct exchange이고, 모든 큐가 큐 이름을 routing key로 삼아 자동으로 바인딩된다. 이 binding은 바꿀 수 없다.
- **순위 근거**: RabbitMQ를 단순한 작업 큐와 구분해 주는 모델이고, smq가 차용하겠다고 밝힌 개념(지도 #1)의 중심이다. default exchange 덕분에 "큐 이름으로 바로 발행"하는 단순한 사용도 같은 모델 안에서 표현된다.
- **smq**: 높음. 라우팅은 발행 트랜잭션 안에서 binding 테이블을 조회해 대상 큐마다 행을 넣는 방식으로 구현할 수 있다.
- 출처: https://www.rabbitmq.com/tutorials/amqp-concepts

### 5. Fanout exchange

- **설명**: routing key를 무시하고 바인딩된 모든 큐에 복사본을 보낸다.
- **순위 근거**: pub/sub의 가장 기본 형태다. 구현은 direct보다 쉽다.
- **smq**: 높음.
- 출처: https://www.rabbitmq.com/tutorials/amqp-concepts

### 6. Topic exchange

- **설명**: routing key는 점으로 구분한 단어 목록이고 최대 255바이트다(예: `stock.usd.nyse`). binding 패턴에서 `*`는 단어 정확히 하나, `#`는 단어 0개 이상과 매칭된다.
- **순위 근거**: 공식 튜토리얼 5장에서 다루는, 선택적 pub/sub의 표준 방식이다. direct와 fanout을 일반화한 것이라 실무에서 자주 쓴다.
- **smq**: 중간~높음. 매칭은 SQL보다 Java에서 하는 편이 단순하다. binding 수가 많으면 캐시나 trie 같은 구조를 고려한다.
- 출처: https://www.rabbitmq.com/tutorials/tutorial-five-java , https://www.rabbitmq.com/tutorials/amqp-concepts

### 7. Prefetch (QoS)

- **설명**: `basic.qos`로 채널(또는 컨슈머)이 동시에 들고 있을 수 있는 미확인 메시지 수를 제한한다. 0은 무제한이고, quorum queue는 컨슈머 prefetch 상한이 2000이다. 문서는 100~300을 흔한 권장값으로, 1을 가장 보수적인 값으로 제시한다. `basic.get`(pull)에는 적용되지 않는다.
- **순위 근거**: 수동 ack와 짝을 이루어 컨슈머 사이의 부하를 분산하고 메모리를 제한한다. priority가 효과를 내려면 prefetch가 필요하다는 점도 문서에 나온다.
- **smq**: 중간. RabbitMQ의 prefetch는 broker가 네트워크로 push할 양을 조절한다. 임베디드 pull 모델에서는 "한 번에 리스할 수 있는 최대 메시지 수"나 "컨슈머당 동시 처리 수"로 다시 정의된다.
- 출처: https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/priority

### 8. Dead Letter Exchange (DLX)

- **설명**: 다음 네 경우에 메시지를 지정한 exchange로 다시 발행한다. (1) `requeue=false`로 reject 또는 nack됨, (2) 메시지 TTL 만료, (3) 큐 길이 제한 초과, (4) quorum queue delivery limit 초과. `x-death` 헤더에 사유(`rejected`, `expired`, `maxlen`, `delivery_limit`), 횟수, 원래 exchange와 routing key가 최신순으로 기록된다. 큐 자체가 만료(queue TTL)되면 그 안의 메시지는 dead-letter되지 **않는다**.
- **순위 근거**: 처리에 실패한 메시지를 잃지 않고 격리하는 표준 수단이다. TTL, 길이 제한, delivery limit이 모두 DLX로 이어진다.
- **세부**: classic queue의 dead-lettering은 내부적으로 confirm 없이 다시 발행하므로 at-most-once다(대상 큐가 없으면 유실된다). quorum queue는 at-least-once다. 순환 경로가 생겨도 경로 안에 reject가 없으면 broker가 감지해서 메시지를 버린다.
- **smq**: 높음. SQLite에서는 원래 큐 행을 지우고 DLX 대상 큐에 넣는 일을 **한 트랜잭션으로 원자 처리**할 수 있다. 그래서 classic queue와 달리 쉽게 at-least-once(사실상 정확히 한 번 이동)를 보장할 수 있다.
- 출처: https://www.rabbitmq.com/docs/dlx

### 9. TTL

- **설명**: 메시지 TTL은 큐 단위(`message-ttl`)와 메시지 단위(`expiration` 속성, 밀리초를 문자열로)로 지정한다. 둘 다 있으면 작은 값이 적용된다. 큐 TTL(`x-expires`)은 사용되지 않는 큐를 자동으로 삭제하는 기능으로, 메시지 만료와는 별개다.
- **순위 근거**: 오래된 메시지가 무한히 쌓이지 않게 막고, DLX와 결합해 지연 재시도를 구현하는 전통적 수단이다. broker는 만료된 메시지를 전달하지 않는다고 보장한다. 단 실제 제거는 best-effort이고, 만료 메시지는 큐의 head에 도달해야 제거되거나 dead-letter된다(그 전까지는 통계와 자원을 차지한다).
- **smq**: 중간. 지도 #1의 미정 항목 "메시지 보존과 정리 정책, TTL"과 직결된다. SQL에서는 `expires_at` 컬럼으로 표현하면 head에 있을 때만 제거된다는 RabbitMQ의 제약이 없다. 큐 TTL은 연결과 컨슈머 생존 개념에 기대므로 의미가 약하다.
- 출처: https://www.rabbitmq.com/docs/ttl

### 10. Publisher confirms

- **설명**: confirm 모드 채널에서 broker가 발행된 메시지를 받아들였다는 사실을 비동기 `basic.ack`로 알린다. persistent 메시지가 durable 큐로 갈 때는 디스크에 기록한 뒤에 ack한다. 라우팅 대상이 없는 메시지는 exchange가 그 사실을 판단한 직후 confirm된다. ack 순서는 보장되지 않는다.
- **순위 근거**: 발행 측 신뢰성의 표준 수단이다. 문서는 confirm이 없으면 broker 재시작 때 디스크에 쓰이지 않은 persistent 메시지를 발행자가 모르는 채 잃을 수 있다고 쓴다. 순위가 10위인 이유는 네트워크 broker의 문제를 푸는 기능이라서다.
- **smq**: 낮음. 임베디드에서는 `publish()`가 SQLite 커밋 후에 반환되면 그 자체가 동기 confirm이다. 개념이 따로 필요 없고, 배치 발행 API로 처리량 문제를 대신 푼다.
- 출처: https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/publishers

### 11. Delivery acknowledgement timeout (consumer_timeout)

- **설명**: 컨슈머가 기본 30분(1,800,000ms) 안에 ack하지 않으면 broker가 채널을 `PRECONDITION_FAILED`로 닫고 미확인 메시지를 다시 큐에 넣는다. 서버 설정, policy, `x-consumer-timeout` 인자로 지정한다. 4.3부터는 quorum queue에서만 지원한다.
- **순위 근거**: RabbitMQ에서는 보조 안전장치다. 기본 재전달 트리거는 연결 끊김이기 때문이다.
- **smq**: 높음. smq에는 감지할 연결이 없으므로 이 기능이 **주된 재전달 메커니즘(리스/가시성 타임아웃)** 이 된다. #6의 "재전달 트리거" 질문에 대한 직접적인 선례다.
- 출처: https://www.rabbitmq.com/docs/consumers

### 12. Delivery limit (poison message 처리)

- **설명**: quorum queue는 메시지마다 실패한 전달 횟수를 세고, 한도를 넘으면 메시지를 버리거나 DLX로 보낸다. 4.0부터 기본값은 20이다. `-1`로 끌 수 있지만 권장하지 않는다. 4.3부터는 delayed retry(전달 횟수에 비례하는 선형 backoff, `delayed-retry-min`과 `delayed-retry-max`)도 지원한다.
- **순위 근거**: requeue 루프(2번)를 끊는 장치다. RabbitMQ가 4.0에서 기본값을 "무제한"에서 20으로 바꾼 것은 이 기능을 사실상 필수로 본다는 신호다.
- **smq**: 높음. `delivery_count` 컬럼 하나로 구현할 수 있다. 재시도 지연은 `deliver_at`이나 `visible_at` 컬럼으로 표현한다. #6에서 결정한다.
- 출처: https://www.rabbitmq.com/docs/quorum-queues , https://www.rabbitmq.com/docs/dlx

### 13. Queue length limit와 overflow

- **설명**: `max-length`(개수)와 `max-length-bytes`(바이트)로 큐 크기를 제한한다. overflow 정책은 세 가지다. `drop-head`(기본값, 앞쪽 메시지를 버리거나 dead-letter), `reject-publish`(새 메시지 거부, confirm이 켜져 있으면 nack), `reject-publish-dlx`(거부하면서 dead-letter). ready 상태 메시지만 센다.
- **순위 근거**: 백프레셔와 자원 보호 장치다. 운영에서 중요하지만 기본 전달 의미론에는 영향이 없다.
- **smq**: 중간. 디스크가 한정된 임베디드 환경에서 유용하다. 다만 매 발행마다 COUNT를 세는 비용이 있으므로 카운터 관리 방식을 고민해야 한다.
- 출처: https://www.rabbitmq.com/docs/maxlength

### 14. 지연 메시지 (delayed messages)

- **설명**: 메시지를 발행한 뒤 일정 시간이 지나서야 전달한다. 전통적으로는 `rabbitmq-delayed-message-exchange` 플러그인(`x-delayed-message` exchange 타입, `x-delay` 헤더)을 썼는데, 이 저장소는 **현재 아카이브되었고 더 이상 유지보수되지 않는다**. 이 플러그인에는 노드 하나에만 저장되고 복제되지 않으며, 지연 메시지가 많으면 확장성이 떨어지는 등의 제약이 있었다. **RabbitMQ 4.4.0**부터 quorum queue가 지연 메시지를 기본 지원한다. 메시지에 `x-opt-delivery-time`(Unix epoch 밀리초) 헤더나 annotation을 넣으면 즉시 라우팅하고 enqueue하되, 그 시각이 되어야 컨슈머에게 전달한다. 다른 대안은 TTL과 DLX를 조합하는 것이다.
- **순위 근거**: 예약 작업과 재시도 backoff 같은 실무 수요가 크다. 반면 RabbitMQ 코어에는 오랫동안 없었고 플러그인에 의존했다는 점에서 핵심 기능보다 한 단계 아래로 두었다.
- **smq**: 중간. SQLite에서는 `deliver_at` 컬럼과 인덱스로 쉽고 안전하게 구현할 수 있다. RabbitMQ보다 smq가 오히려 자연스럽게 제공할 수 있는 기능이다. 12번의 delayed retry와 같은 컬럼을 공유할 수 있다.
- 출처: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange , https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.4.0 , https://www.rabbitmq.com/docs/quorum-queues

### 15. Priority queue

- **설명**: 높은 priority 메시지를 먼저 전달한다. classic queue는 `x-max-priority`로 선언하고 1~255를 쓸 수 있지만 2~4단계를 강력히 권장한다. 선언한 뒤에는 바꿀 수 없다. quorum queue는 4.3부터 엄격 priority 32단계(0~31)를 별도 설정 없이 지원한다. 4.3 이전에는 normal과 high 두 버킷을 2:1 비율로 분배했다.
- **순위 근거**: 특정 상황에 필요하지만, 문서는 큐를 priority별로 나누는 것 같은 대안을 먼저 검토하라고 권한다. quorum queue에서는 기아(starvation)가 생길 수 있다는 경고도 있다.
- **smq**: 중간. SQL의 `ORDER BY priority DESC, id`로 표현할 수 있다. 다만 인덱스 설계와 기아 문제를 고려해야 한다.
- 출처: https://www.rabbitmq.com/docs/priority

### 16. Single Active Consumer, exclusive consumer, consumer priority

- **설명**: SAC는 한 큐에서 한 번에 한 컨슈머만 받게 하고, 그 컨슈머가 실패하면 다른 컨슈머로 자동으로 넘긴다. 큐를 선언할 때만 설정할 수 있다. exclusive consumer는 classic queue에서만 동작하고 자동 failover가 없다. consumer priority는 높은 priority 컨슈머가 막혀 있지 않으면 그쪽에 먼저 전달한다.
- **순위 근거**: 엄격한 순서 처리(1번)와 active-standby 구성에 필요하다.
- **smq**: 중간. 여러 프로세스가 같은 큐를 소비할 때 순서를 보장하는 유일한 수단이다. 구현하려면 "활성 컨슈머 리스"(하트비트 행)가 필요하다. 이것도 연결 대신 리스가 쓰이는 사례다.
- 출처: https://www.rabbitmq.com/docs/consumers , https://www.rabbitmq.com/docs/queues

### 17. Headers exchange

- **설명**: routing key 대신 메시지 헤더 값으로 라우팅한다. binding의 `x-match=all|any`로 헤더가 전부 맞아야 하는지 하나만 맞으면 되는지를 정한다.
- **순위 근거**: topic으로 대부분 대체되고 실제로 쓰는 경우가 적다. 네 가지 기본 exchange 타입 중 가장 덜 중요하다.
- **smq**: 낮음. v1 이후 후보다.
- 출처: https://www.rabbitmq.com/tutorials/amqp-concepts

### 18. Alternate exchange와 mandatory 플래그 (unroutable 메시지 처리)

- **설명**: 어떤 큐로도 라우팅되지 않은 메시지는 기본적으로 버려진다. `mandatory=true`면 발행자에게 반환되고, exchange에 alternate exchange(AE)가 설정되어 있으면 그 AE로 다시 발행된다. AE로 라우팅된 메시지는 mandatory 판단에서 "라우팅된 것"으로 친다.
- **순위 근거**: 메시지가 조용히 유실되는 것을 막는 장치다. 설정 실수(binding 누락)를 잡아 주지만 정상 흐름에서는 쓰이지 않는다.
- **smq**: 중간. 임베디드 API라면 `publish()`가 "라우팅된 큐 수"를 반환하거나 옵션에 따라 예외를 던지는 방식으로 mandatory를 단순하게 대체할 수 있다.
- 출처: https://www.rabbitmq.com/docs/ae , https://www.rabbitmq.com/docs/publishers , https://www.rabbitmq.com/tutorials/amqp-concepts

### 19. Exchange-to-exchange binding

- **설명**: `exchange.bind`로 exchange끼리 연결해서 다단계 라우팅 토폴로지를 만든다. RabbitMQ 확장 기능이다.
- **순위 근거**: 고급 토폴로지용이고 사용 빈도가 낮다.
- **smq**: 낮음. 라우팅이 재귀가 되므로 순환을 감지해야 한다. v1 이후 후보다.
- 출처: https://www.rabbitmq.com/docs/e2e

### 20. 트랜잭션 (tx.select / commit / rollback)

- **설명**: 발행, ack, reject를 묶어서 커밋한다. RabbitMQ는 **단일 큐에서도 원자성을 보장하지 않는다**고 명시하고, 자신의 트랜잭션은 "DB의 ACID라기보다 배치 기능에 가깝다"고 설명한다. 문서에 따르면 매 발행마다 커밋하면 처리량이 250분의 1로 떨어진다. 그래서 publisher confirms가 도입되었고, 한 채널에서 트랜잭션과 confirm은 함께 쓸 수 없다.
- **순위 근거**: RabbitMQ에서는 사실상 confirm으로 대체되어 쓰이지 않는다.
- **smq**: 중간. **smq는 RabbitMQ보다 강하게 제공할 수 있다.** SQLite 트랜잭션 하나로 "메시지 ack와 다음 메시지 발행"이나 "여러 큐로의 발행"을 진짜로 원자 처리할 수 있다. 애플리케이션이 같은 SQLite DB를 쓴다면 transactional outbox 패턴도 가능하다. 다만 백엔드를 교체할 수 있으므로(인메모리, PostgreSQL) 트랜잭션 경계를 추상화하는 문제가 생긴다. 이 문제는 #4와 #5에서 다룬다.
- 출처: https://www.rabbitmq.com/docs/semantics , https://www.rabbitmq.com/docs/confirms

### 21. Exclusive / auto-delete 큐, server-named 큐

- **설명**: exclusive 큐는 선언한 연결만 쓸 수 있고 그 연결이 닫히면 삭제된다. auto-delete 큐는 마지막 컨슈머가 구독을 해지하면 삭제된다. 이름을 빈 문자열로 주면 broker가 이름을 생성한다(일시적 상태용).
- **순위 근거**: RPC 응답 큐나 일시적 구독처럼 수명이 짧은 용도에만 쓴다.
- **smq**: 낮음. 이 기능들은 "연결"과 "컨슈머 세션"의 수명에 묶여 있다. 프로세스가 죽은 것을 감지할 수단(리스)이 없으면 정리가 보장되지 않는다. 프로세스 로컬 임시 큐가 필요하다면 인메모리 백엔드가 더 맞다.
- 출처: https://www.rabbitmq.com/docs/queues , https://www.rabbitmq.com/tutorials/amqp-concepts

### 22. Stream

- **설명**: 소비해도 메시지가 지워지지 않는 append-only 로그다. 컨슈머는 `x-stream-offset`(first, last, next, 오프셋, 타임스탬프)으로 시작 위치를 고른다. 보존은 `max-age`와 `max-length-bytes`로 정한다. 대규모 fan-out, 재생(replay), 높은 처리량이 목적이다. 파티셔닝에는 super stream을 쓴다.
- **순위 근거**: 큐와 다른 소비 모델(Kafka에 가까움)이다. 큐를 보완하는 기능이지 메시지 큐의 핵심은 아니다.
- **smq**: 낮음. 개념적으로는 SQLite에서도 구현하기 쉽다(메시지를 지우지 않고 컨슈머별 오프셋 행을 관리). 그래도 v1 범위 밖의 후보로 보는 게 맞다.
- 출처: https://www.rabbitmq.com/docs/streams

### 23. Quorum queue

- **설명**: Raft 합의로 여러 노드에 복제하는 durable 큐다. 과반수가 복제해야 publisher에 confirm한다. transient, exclusive, global QoS, server-named 큐를 지원하지 않는다.
- **순위 근거**: RabbitMQ 4.x에서 고가용성의 기본 선택이지만, 본질은 복제(클러스터) 기능이다.
- **smq**: **해당없음**. 다중 호스트 클러스터링은 지도 #1에서 범위 밖이다. 다만 quorum queue에만 있는 **의미론**, 즉 delivery limit(12번), delayed retry, 지연 메시지(14번), 32단계 priority(15번), at-least-once dead-lettering(8번)은 차용할 가치가 크다. RabbitMQ의 최신 기능 개선은 대부분 quorum queue 쪽에 들어가고 있다.
- 출처: https://www.rabbitmq.com/docs/quorum-queues

### 24. Lazy queue

- **설명**: 메시지를 메모리에 두지 않고 곧바로 디스크에 쓰는 classic queue 모드였다.
- **순위 근거**: RabbitMQ 3.12 이후로는 설정이 무시되어 **사실상 폐지**되었다. 현재 classic queue 구현이 기본으로 비슷하게 동작한다(낮고 안정적인 메모리 사용).
- **smq**: **해당없음**. SQLite 백엔드는 원래 디스크 기반이다.
- 출처: https://www.rabbitmq.com/docs/lazy-queues

## 그 밖에 참고할 점

- **Push와 pull**: RabbitMQ는 구독(push)을 권장하고 `basic.get`(pull)은 "매우 비효율적"이라고 본다. 이는 네트워크 왕복 비용 때문이다. 임베디드 SQLite에서는 pull(폴링)이나 폴링과 알림을 섞은 방식이 자연스럽다. 같은 프로세스 안이면 알림, 다른 프로세스면 폴링 주기를 두는 식이다. API는 push처럼 보여도 내부는 pull로 구현할 수 있다.
- **메시지 속성**: content type, delivery mode, priority, expiration, message_id, correlation_id, reply_to, headers, timestamp 등이 있다. broker는 payload를 불투명한 바이트 배열로 취급한다. smq의 메시지 모델(#4 등)에서 이 목록을 기준으로 삼을 수 있다.
- **x-arguments보다 policy**: RabbitMQ는 TTL, DLX, length limit 같은 설정을 하드코딩한 x-arguments 대신 런타임에 바꿀 수 있는 policy로 지정하라고 강력히 권한다. 단 queue type과 max priority는 선언할 때만 정할 수 있다. smq API에서 큐 설정을 바꿀 수 있게 할지 결정할 때 참고할 만하다.

## 후속 티켓에 주는 시사점

- **#5 (v1 범위)**: 순위 1~8, 11, 12번은 v1 핵심 후보다. 14번(지연)과 20번(트랜잭션)은 SQLite에서 구현 비용이 낮고 차별점이 될 수 있다. 21~24번은 영구 제외나 해당없음 후보다.
- **#6 (전달 보장)**: RabbitMQ의 at-least-once는 "연결이 끊기면 재전달하고, consumer_timeout을 넘기면 재전달하고, delivery limit을 넘기면 DLX로 보내는" 구조다. smq에는 연결 끊김 감지가 없으므로 리스(가시성 타임아웃)를 주 트리거로, delivery_count 한도와 DLX를 그 뒤 단계로 두는 구성이 RabbitMQ 의미론에 가장 가깝다. 중복 처리는 RabbitMQ와 마찬가지로 컨슈머의 멱등성에 맡긴다는 것이 공식 문서의 입장이다.
