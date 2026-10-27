package io.github.ppzxc.smq.storage.sqlite;

import java.time.Duration;

/** {@link System#nanoTime()} 기준의 마감. 꺼내기의 {@code wait}를 lock과 BUSY 대기에 적용하는 데 쓴다(ADR-0009). */
final class Deadline {

    /** 마감이 없다. lock과 BUSY 재시도를 끝까지 기다린다. */
    static final Deadline NONE = new Deadline(0, 0, false);

    /** 마감이 지나 호출이 아무것도 반영하지 않고 물러났다. 롤백이 확실한 경우에만 던진다. */
    static final class Expired extends RuntimeException {
        Expired() {
            super(null, null, false, false);
        }
    }

    /**
     * lock을 기다리는 시간의 기본 하한. 정상적인 직렬화(다른 스레드의 트랜잭션 하나)는 이 안에 끝나므로, {@code wait}가
     * 0이어도 메시지가 있는데 빈 결과를 돌려주지 않는다. 다른 스레드가 BUSY 재시도로 lock을 오래 쥘 때만 넘는다.
     * 트랜잭션 하나가 이보다 오래 걸리는 저장소는 열 때 하한을 늘린다.
     */
    static final Duration DEFAULT_LOCK_WAIT_FLOOR = Duration.ofMillis(100);

    private final long nanos;
    private final long floorNanos;
    private final boolean bounded;

    private Deadline(long nanos, long floorNanos, boolean bounded) {
        this.nanos = nanos;
        this.floorNanos = floorNanos;
        this.bounded = bounded;
    }

    static Deadline after(Duration wait, Duration lockWaitFloor) {
        long now = System.nanoTime();
        return new Deadline(now + saturated(wait), saturated(lockWaitFloor), true);
    }

    private static long saturated(Duration d) {
        return d.compareTo(Duration.ofNanos(Long.MAX_VALUE / 2)) > 0 ? Long.MAX_VALUE / 2 : d.toNanos();
    }

    boolean bounded() {
        return bounded;
    }

    boolean passed() {
        return bounded && nanos - System.nanoTime() <= 0;
    }

    /** lock을 기다릴 수 있는 시간(나노초). 남은 시간이되 열 때 정한 하한보다 짧지 않다. 마감이 없으면 쓰지 않는다. */
    long lockWaitNanos() {
        return Math.max(floorNanos, nanos - System.nanoTime());
    }
}
