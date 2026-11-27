package io.github.ppzxc.smq;

import java.time.Duration;
import java.util.regex.Pattern;

final class Validation {

    static final Duration MAX_DELAY_OR_LEASE = Duration.ofDays(36_500);
    private static final Pattern QUEUE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");

    private Validation() {}

    static String queueName(final String name) {
        if (name == null || !QUEUE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid queue name (allowed: [A-Za-z0-9._-]{1,200}): " + name);
        }
        return name;
    }

    static long saturatedWaitNanos(final Duration wait) {
        final Duration max;
        max = Duration.ofNanos(Long.MAX_VALUE / 2);
        return wait.compareTo(max) > 0 ? Long.MAX_VALUE / 2 : wait.toNanos();
    }

    static Duration positive(final Duration d, final String what) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException(what + " must be positive: " + d);
        }
        if (d.compareTo(MAX_DELAY_OR_LEASE) > 0) {
            throw new IllegalArgumentException(what + " must not exceed 36500 days: " + d);
        }
        return d;
    }

    static Duration nonNegative(final Duration d, final String what) {
        if (d == null || d.isNegative()) {
            throw new IllegalArgumentException(what + " must not be negative: " + d);
        }
        if (d.compareTo(MAX_DELAY_OR_LEASE) > 0) {
            throw new IllegalArgumentException(what + " must not exceed 36500 days: " + d);
        }
        return d;
    }
}
