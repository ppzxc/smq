package io.github.ppzxc.smq.benchmark.random;

/** 실행 하나의 판정. PASS가 아니면 모두 실패다. */
enum Verdict {
    PASS,
    /** 불변식 위반이 하나 이상이다. */
    VIOLATION,
    /** 소진이 상한 시간 안에 끝나지 않았다. */
    DRAIN_TIMEOUT,
    /** 고정 속도 모드에서 큐 깊이가 계속 늘었다(소비가 발행을 못 따라간다). 불변식 위반은 아니다. */
    BACKLOG_GROWING,
    /** 고정 속도 모드에서 달성률이 {@code --min-achieved} 아래다. 큐가 막히지 않아도 목표 부하를 내지 못한 것이다. 불변식 위반은 아니다. */
    UNDER_RATE,
    /** 하네스나 smq가 예외를 냈다. */
    ERROR
}
