package io.github.ppzxc.smq.storage.sqlite;

import java.time.Duration;

final class Deadline {

    static final Deadline NONE = new Deadline(0, 0, false);

    static final class Expired extends RuntimeException {
        Expired() {
            super(null, null, false, false);
        }
    }

    static final class Interrupted extends RuntimeException {
        private final InterruptedException cause;

        Interrupted(final InterruptedException cause) {
            super(cause);
            this.cause = cause;
        }

        @Override
        public synchronized InterruptedException getCause() {
            return cause;
        }
    }

    static final Duration DEFAULT_LOCK_WAIT_FLOOR = Duration.ofMillis(100);

    private final long nanos;
    private final long floorNanos;
    private final boolean bounded;

    private Deadline(final long nanos, final long floorNanos, final boolean bounded) {
        this.nanos = nanos;
        this.floorNanos = floorNanos;
        this.bounded = bounded;
    }

    static Deadline after(final Duration wait, final Duration lockWaitFloor) {
        final long now;
        now = System.nanoTime();
        return new Deadline(now + saturated(wait), saturated(lockWaitFloor), true);
    }

    private static long saturated(final Duration d) {
        return d.compareTo(Duration.ofNanos(Long.MAX_VALUE / 2)) > 0 ? Long.MAX_VALUE / 2 : d.toNanos();
    }

    boolean bounded() {
        return bounded;
    }

    long lockWaitNanos() {
        return Math.max(floorNanos, nanos - System.nanoTime());
    }
}
