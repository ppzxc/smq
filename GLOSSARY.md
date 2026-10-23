# smq

애플리케이션 프로세스 안에서 동작하는 내구성 메시지 큐. 메시지를 청크 단위로 꺼내 ACK/NACK하고, 실패한 메시지는 재시도한다.

## Language

**청크(Chunk)**:
컨슈머가 큐에서 한 번에 꺼낸 메시지 묶음. 묶음 전체가 하나의 리스를 공유한다.
_Avoid_: 배치(꺼내는 쪽에서), fetch batch

**발행 배치(Publish batch)**:
프로듀서가 한 번에 발행하는 메시지 묶음. 전부 발행되거나 하나도 발행되지 않는다.
_Avoid_: 청크(발행하는 쪽에서)

**리스(Lease)**:
꺼낸 청크에 대해 컨슈머가 갖는 기한부 점유. 기한 안에 ACK/NACK되지 않은 메시지는 다시 꺼낼 수 있게 된다.
_Avoid_: 가시성 타임아웃, 잠금

**Dead letter 큐(Dead letter queue)**:
최대 전달 횟수를 넘긴 메시지가 옮겨지는 큐. 그 자체로 평범한 큐다.
_Avoid_: DLX, 실패 큐

**단일 활성 컨슈머(Single active consumer)**:
한 번에 하나의 청크만 리스될 수 있도록 선언된 큐의 성질. 순서가 필요한 소비를 위해 쓴다.
_Avoid_: 순서 큐, exclusive consumer
