package io.github.ppzxc.smq.benchmark.random;

/**
 * {@code counts()} 표본 하나.
 *
 * @param afterDrain 소진 구간이 끝난 뒤에 잰 표본이면 true. 부하 중 표본이면 false
 */
record CountsSample(QueueKind queue, boolean afterDrain, long ready, long leased, long delayed) {}
