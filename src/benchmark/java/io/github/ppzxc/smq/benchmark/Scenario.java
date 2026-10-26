package io.github.ppzxc.smq.benchmark;

/** 스펙의 시나리오. */
enum Scenario {
    /** 전체 주기 처리량: 발행 배치, 꺼내기, 청크 전체 ACK. 핵심 지표다. */
    S1,
    /** 발행 전용 처리량. */
    S2,
    /** 발행에서 꺼내기까지의 지연(p50, p99). */
    S3
}
