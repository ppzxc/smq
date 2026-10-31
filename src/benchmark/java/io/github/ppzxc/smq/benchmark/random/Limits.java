package io.github.ppzxc.smq.benchmark.random;

/**
 * 시드에서 뽑은 매개변수에 씌우는 상한. 시드가 같아도 상한이 다르면 워크로드가 다르다. 풀 실행({@link #full})은 이슈 #86의 표
 * 그대로이고, 스모크는 몇 초 안에 끝나도록 미리 채운 깊이와 리스 기한을 줄인다.
 *
 * @param loadMillis 부하 구간 길이
 * @param maxDepth 미리 채운 깊이의 상한
 * @param maxLeaseMillis 리스 기한의 상한
 * @param ratePerSecond 프로듀서 전체의 목표 초당 메시지 수. 0이면 목표 없이 최대한 보내는 무작위 모드이고, 양수면 고정 속도 모드다
 */
record Limits(long loadMillis, int maxDepth, long maxLeaseMillis, long ratePerSecond) {

    static final long DEFAULT_LOAD_MILLIS = 10_000;

    static Limits full(long loadMillis) {
        return new Limits(loadMillis, 100_000, 10_000, 0);
    }

    static Limits smoke(long loadMillis) {
        return new Limits(loadMillis, 2_000, 1_000, 0);
    }

    Limits withRate(long ratePerSecond) {
        return new Limits(loadMillis, maxDepth, maxLeaseMillis, ratePerSecond);
    }
}
