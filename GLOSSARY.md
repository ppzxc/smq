# smq

애플리케이션 프로세스 안에서 동작하는 내구성 메시지 큐. 메시지를 청크 단위로 꺼내 ACK/NACK하고, 실패한 메시지는 재시도한다.

## Language

### 큐와 메시지

**저장소(Storage)**:
큐와 메시지를 보관하고 큐 연산 하나하나가 전부 반영되거나 하나도 반영되지 않도록 책임지는 교체 가능한 구성 요소. SQLite, 인메모리 등이 있다.
_Avoid_: 백엔드, 드라이버, repository

**내구성 수준(Durability)**:
발행이나 정산이 성공한 뒤 그 결과가 어떤 장애까지 견디고 남는지를 정하는 저장소의 성질. 전원 장애까지 견디는 수준(Power loss)과 프로세스 크래시만 견디는 수준(Process crash)이 있다.
_Avoid_: synchronous, 영속성 모드, fsync 설정

**큐(Queue)**:
이름으로 식별되며 메시지를 FIFO 순서로 보관하는 곳.
_Avoid_: 토픽, 채널

**선언(Declare)**:
큐를 주어진 속성으로 존재하게 하는 일. 같은 속성으로 다시 선언하면 아무 일도 없고, 속성이 다르면 오류다.
_Avoid_: 생성, create

**메시지(Message)**:
payload, 헤더, 시스템 필드로 이루어진 큐의 단위.
_Avoid_: 이벤트, 작업(job)

**메시지 id(Message id)**:
메시지가 큐에 들어갈 때 큐가 붙이는 식별자. 재전달되어도 바뀌지 않는다.
_Avoid_: 메시지 키, CloudEvents id(프로듀서가 붙이는 별개의 식별자)

**프로듀서(Producer)**:
큐에 메시지를 발행하는 쪽. smq는 프로듀서를 식별하지 않는다.
_Avoid_: publisher, sender

**컨슈머(Consumer)**:
큐에서 청크를 꺼내는 쪽. smq는 컨슈머를 등록하거나 서로 구별하지 않는다.
_Avoid_: subscriber, worker, receiver

### 발행과 꺼내기

**발행(Publish)**:
프로듀서가 큐에 메시지를 넣는 일. 언제나 발행 배치 단위로 이루어진다.
_Avoid_: send, enqueue, push, produce

**발행 배치(Publish batch)**:
프로듀서가 한 번에 발행하는 메시지 묶음. 전부 발행되거나 하나도 발행되지 않는다.
_Avoid_: 청크(발행하는 쪽에서)

**꺼내기(Pull)**:
컨슈머가 큐에서 준비 상태의 메시지를 청크로 가져가는 일.
_Avoid_: fetch, receive, dequeue, consume, claim

**청크(Chunk)**:
컨슈머가 큐에서 한 번에 꺼낸 메시지 묶음. 묶음 전체가 하나의 리스를 공유한다.
_Avoid_: 배치(꺼내는 쪽에서), fetch batch

**리스(Lease)**:
꺼낸 청크에 대해 컨슈머가 갖는 기한부 점유. 꺼낼 때마다 새로 생기며, 연장해도 같은 리스다. 기한 안에 ACK/NACK되지 않은 메시지는 다시 꺼낼 수 있게 된다.
_Avoid_: 가시성 타임아웃, 잠금

**연장(Extend)**:
리스의 기한을 늦추는 일.
_Avoid_: renew, heartbeat, touch

**지연(Delay)**:
메시지를 지정한 시각까지 꺼낼 수 없게 하는 일. 발행할 때나 NACK할 때 줄 수 있다.
_Avoid_: 예약, schedule, 백오프. 꺼내기에서 메시지를 기다리는 시간과 혼동하지 않는다.

### 메시지 상태

**준비(Ready)**:
지금 꺼낼 수 있는 메시지의 상태.
_Avoid_: 대기, available, visible

**리스 중(Leased)**:
어떤 청크의 리스에 묶여 있는 메시지의 상태.
_Avoid_: 처리 중, in-flight, unacked

**지연됨(Delayed)**:
지정한 시각이 오기 전이라 꺼낼 수 없는 메시지의 상태. ACK되거나 dead letter 큐로 옮겨진 메시지는 상태가 아니라 큐에 더는 없는 것이다.
_Avoid_: 예약됨, scheduled

### 처리 결과

**정산(Settle)**:
리스 중인 메시지에 ACK, NACK, reject 중 하나를 적용해 리스에서 풀어 주는 일. 한 번의 정산에 여러 메시지와 서로 다른 처리 결과를 함께 담을 수 있고, 전부 함께 반영된다.
_Avoid_: 확인, resolve, dispose

**ACK**:
처리를 끝낸 메시지를 큐에서 지우는 일.
_Avoid_: commit, complete, 삭제

**NACK**:
리스 중인 메시지를 다시 꺼낼 수 있게 돌려주는 일. 지연 없이 돌려주면 맨 앞으로 간다.
_Avoid_: 반납, release, requeue

**Reject**:
컨슈머가 재시도해도 소용없다고 판단한 메시지를 전달 횟수와 상관없이 바로 dead letter 큐로 보내는 일.
_Avoid_: NACK(requeue=false), 버리기

**낡은 연산(Stale operation)**:
메시지가 이미 다른 리스로 넘어갔거나 같은 리스로 이미 정산된 뒤에 그 리스를 근거로 시도한 ACK, NACK, reject, 연장. 효력이 없고 실패가 아니라 결과로 알려진다.
_Avoid_: 만료된 ACK, 충돌

### 재시도와 dead letter

**전달(Delivery)**:
메시지가 청크에 담겨 컨슈머에게 한 번 꺼내지는 일. 결과가 ACK든 NACK든 리스 만료든 한 번의 전달이다.
_Avoid_: 시도(attempt), 소비

**전달 횟수(Delivery count)**:
메시지가 지금까지 전달된 횟수. 최대 전달 횟수와 중복 판단의 근거가 된다.
_Avoid_: 재시도 횟수, read count

**최대 전달 횟수(Max deliveries)**:
큐에 선언하는 전달 횟수의 한도. 이를 다 쓴 메시지는 더 전달되지 않고 dead letter 큐로 옮겨진다.
_Avoid_: 재전달 한도, 재시도 한도, retry limit

**Dead letter 큐(Dead letter queue)**:
최대 전달 횟수를 넘겼거나 reject된 메시지가 옮겨지는 큐. 그 자체로 평범한 큐다.
_Avoid_: DLX, 실패 큐

**Dead letter 이동(Dead-lettering)**:
메시지를 원래 큐에서 지우고 dead letter 큐에 새 메시지로 넣는 일. 그래서 메시지 id와 전달 횟수가 새로 시작된다.
_Avoid_: 격리, 버리기

**단일 활성 컨슈머(Single active consumer)**:
한 번에 하나의 청크만 리스될 수 있도록 선언된 큐의 성질. 컨슈머를 구별하지 않으므로 청크를 이미 가진 컨슈머가 다시 꺼내도 막힌다. 순서가 필요한 소비를 위해 쓴다.
_Avoid_: 순서 큐, exclusive consumer
