package io.github.ppzxc.smq.benchmark.random;

/**
 * 프로듀서의 발행 기록. 발행에 성공한 메시지마다 하나.
 *
 * @param atMillis 발행 호출 직전의 벽시계 시각(epoch ms). 지연 판정이 거짓 위반을 내지 않도록 호출 전에 잰다.
 * @param delayMillis 발행 지연. 없으면 0
 */
record Published(Tag tag, Content content, long atMillis, long delayMillis) {}
