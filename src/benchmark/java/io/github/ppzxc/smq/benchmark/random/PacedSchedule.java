package io.github.ppzxc.smq.benchmark.random;

/**
 * 목표 속도에 맞춘 발행 시각표(open-loop). n번째 배치의 시각은 시작 시각에서 {@code n * 배치 크기 / 속도}만큼 뒤로 정해져 있고,
 * 앞선 배치가 늦어도 뒤로 밀리지 않는다. 늦으면 밀린 만큼 연달아 보내 따라잡고, 그 늦은 정도를 {@link #lagNanos}로 알 수 있다.
 * 시계를 읽지 않으므로 호출하는 쪽이 {@code System.nanoTime()} 값을 넘긴다. 스레드마다 하나씩 쓴다.
 */
final class PacedSchedule {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final long ratePerSecond;
    private final int batchSize;
    private final long startNanos;
    private long sentBatches;

    /**
     * @param ratePerSecond 이 스케줄이 보낼 초당 메시지 수
     * @param batchSize 배치 하나의 메시지 수
     * @param startNanos 첫 배치의 시각
     */
    PacedSchedule(long ratePerSecond, int batchSize, long startNanos) {
        if (ratePerSecond <= 0 || batchSize <= 0) {
            throw new IllegalArgumentException("속도와 배치 크기는 양수여야 한다: " + ratePerSecond + ", " + batchSize);
        }
        this.ratePerSecond = ratePerSecond;
        this.batchSize = batchSize;
        this.startNanos = startNanos;
    }

    /** 다음 배치를 보내기로 한 시각. */
    long nextDueNanos() {
        return startNanos + sentBatches * batchSize * NANOS_PER_SECOND / ratePerSecond;
    }

    /** 배치 하나를 보냈다고 기록한다. */
    void sent() {
        sentBatches++;
    }

    /** {@code nowNanos} 시점에 다음 배치가 얼마나 늦었는지. 늦지 않았으면 0. */
    long lagNanos(long nowNanos) {
        return Math.max(0, nowNanos - nextDueNanos());
    }

    /** {@code nowNanos}부터 다음 배치까지 더 기다려야 하는 시간. 이미 지났으면 0. */
    long waitNanos(long nowNanos) {
        return Math.max(0, nextDueNanos() - nowNanos);
    }
}
