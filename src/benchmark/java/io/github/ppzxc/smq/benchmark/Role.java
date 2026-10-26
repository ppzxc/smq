package io.github.ppzxc.smq.benchmark;

/** 자식 JVM 하나가 맡는 역할. */
enum Role {
    /** S1/S2: 시나리오 전체를 혼자 돈다. */
    WORKER,
    /** S3: 발행만 한다. */
    PRODUCER,
    /** S3: 꺼내기만 하고 지연을 잰다. */
    CONSUMER,
    /** S3 같은 프로세스: 발행 스레드와 꺼내기 스레드를 한 JVM에서 돌린다. */
    BOTH
}
