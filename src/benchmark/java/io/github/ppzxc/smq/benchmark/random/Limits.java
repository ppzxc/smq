package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.Durability;
import java.util.Optional;

/**
 * 시드에서 뽑은 매개변수에 씌우는 상한. 시드가 같아도 상한이 다르면 워크로드가 다르다. 풀 실행({@link #full})은 이슈 #86의 표
 * 그대로이고, 스모크는 몇 초 안에 끝나도록 미리 채운 깊이와 리스 기한을 줄인다.
 *
 * @param loadMillis 부하 구간 길이
 * @param maxDepth 미리 채운 깊이의 상한
 * @param maxLeaseMillis 리스 기한의 상한
 * @param ratePerSecond 프로듀서 전체의 목표 초당 메시지 수. 0이면 목표 없이 최대한 보내는 무작위 모드이고, 양수면 고정 속도 모드다
 * @param soakMessageBytes 고정 속도 모드의 메시지 크기 상한(최소는 그 절반). 0이면 기본값 {@link #DEFAULT_SOAK_MESSAGE_BYTES}
 * @param standingQueue 고정 속도 모드에서 큐 깊이가 미리 채운 깊이 이하면 컨슈머가 쉬어서, 미리 채운 깊이가 비워지지 않고 유지되게 한다
 * @param durability 내구성 수준을 고정한다. 비어 있으면 시드에서 무작위로 고른다
 */
record Limits(
        long loadMillis,
        int maxDepth,
        long maxLeaseMillis,
        long ratePerSecond,
        int soakMessageBytes,
        boolean standingQueue,
        Optional<Durability> durability) {

    static final long DEFAULT_LOAD_MILLIS = 10_000;

    /** 고정 속도 모드의 기본 메시지 크기 상한. 운영의 작은 메시지에 가깝다. */
    static final int DEFAULT_SOAK_MESSAGE_BYTES = 1024;

    static Limits full(long loadMillis) {
        return new Limits(loadMillis, 100_000, 10_000, 0, 0, false, Optional.empty());
    }

    static Limits smoke(long loadMillis) {
        return new Limits(loadMillis, 2_000, 1_000, 0, 0, false, Optional.empty());
    }

    Limits withRate(long ratePerSecond) {
        return new Limits(
                loadMillis, maxDepth, maxLeaseMillis, ratePerSecond, soakMessageBytes, standingQueue, durability);
    }

    Limits withMessageBytes(int soakMessageBytes) {
        return new Limits(
                loadMillis, maxDepth, maxLeaseMillis, ratePerSecond, soakMessageBytes, standingQueue, durability);
    }

    Limits withDepth(int maxDepth) {
        return new Limits(
                loadMillis, maxDepth, maxLeaseMillis, ratePerSecond, soakMessageBytes, standingQueue, durability);
    }

    Limits withStandingQueue(boolean standingQueue) {
        return new Limits(
                loadMillis, maxDepth, maxLeaseMillis, ratePerSecond, soakMessageBytes, standingQueue, durability);
    }

    Limits withDurability(Durability durability) {
        return new Limits(
                loadMillis,
                maxDepth,
                maxLeaseMillis,
                ratePerSecond,
                soakMessageBytes,
                standingQueue,
                Optional.of(durability));
    }
}
