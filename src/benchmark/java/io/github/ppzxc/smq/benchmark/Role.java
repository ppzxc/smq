package io.github.ppzxc.smq.benchmark;

/** 자식 JVM이 맡는 역할. smq는 한 JVM만 열 수 있으므로(ADR-0012) JVM 하나가 시나리오 전체를 돈다. */
enum Role {
    /** S1/S2: 시나리오 전체를 혼자 돈다. */
    WORKER,
    /** S3: 발행 스레드와 꺼내기 스레드를 한 JVM에서 돌린다. */
    BOTH
}
