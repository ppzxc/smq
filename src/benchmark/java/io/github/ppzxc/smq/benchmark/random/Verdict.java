package io.github.ppzxc.smq.benchmark.random;

/** 실행 하나의 판정. PASS가 아니면 모두 실패다. */
enum Verdict {
    PASS,
    /** 불변식 위반이 하나 이상이다. */
    VIOLATION,
    /** 소진이 상한 시간 안에 끝나지 않았다. */
    DRAIN_TIMEOUT,
    /** 하네스나 smq가 예외를 냈다. */
    ERROR
}
