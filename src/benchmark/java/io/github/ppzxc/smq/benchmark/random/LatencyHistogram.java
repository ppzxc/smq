package io.github.ppzxc.smq.benchmark.random;

/**
 * 발행에서 꺼내기까지 걸린 시간의 1ms 간격 히스토그램. 표본을 전부 들고 있으면 힙이 실행 길이에 비례해 느므로, 크기가 고정된 이
 * 구조로 백분위수를 낸다. {@link #MAX_MILLIS}를 넘는 값은 그 값으로 센다.
 */
final class LatencyHistogram {

    /** 이보다 오래 걸린 값은 이 값으로 센다. 한 시간이면 큐가 사실상 멈춘 것이라 구분할 필요가 없다. */
    static final int MAX_MILLIS = 3_600_000;

    private final int[] counts = new int[MAX_MILLIS + 1];
    private long total;

    void add(long millis) {
        counts[(int) Math.min(Math.max(0, millis), MAX_MILLIS)]++;
        total++;
    }

    long total() {
        return total;
    }

    /** nearest-rank 백분위수. 표본이 없으면 {@link IllegalStateException}. */
    long percentile(int p) {
        if (total == 0) {
            throw new IllegalStateException("표본이 없다");
        }
        long rank = Math.max(1, Math.min(total, (long) Math.ceil(p / 100.0 * total)));
        long seen = 0;
        for (int ms = 0; ms <= MAX_MILLIS; ms++) {
            seen += counts[ms];
            if (seen >= rank) {
                return ms;
            }
        }
        return MAX_MILLIS;
    }
}
