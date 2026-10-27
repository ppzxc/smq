package io.github.ppzxc.smq.benchmark.random;

/** 검사기가 판정하는 불변식 8개. 처리량과 지연(latency)은 판정하지 않는다. */
enum Invariant {
    /** 1. 발행에 성공한 태그가 정확히 한 번 ACK되거나 dead letter로 옮겨진 뒤 ACK되지 않았다. */
    LOSS,
    /** 2. 발행하지 않은 태그가 전달됐거나 payload와 헤더가 발행할 때와 다르다. */
    GHOST,
    /** 3. at-least-once 범위를 넘는 중복: 원인 없는 재전달, deliveryCount 빈틈, ACK 뒤 재전달. */
    DUPLICATE,
    /** 4. dead letter 이동이 reject나 최대 전달 횟수 소진과 일대일이 아니거나 헤더가 기록과 다르다. */
    DEAD_LETTER,
    /** 5. 발행 지연이나 NACK 지연이 지켜지지 않았다. */
    DELAY,
    /** 6. 단일 활성 컨슈머 큐에서 리스가 살아 있는 동안 다른 청크가 꺼내졌다. */
    SINGLE_ACTIVE,
    /** 7. 소진 뒤 counts가 (0, 0, 0)이 아니거나 부하 중 표본이 음수다. */
    COUNTS,
    /** 8. 청크 안 메시지가 메시지 id 순서가 아니다. */
    CHUNK_ORDER
}
