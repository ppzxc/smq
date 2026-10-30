package io.github.ppzxc.smq;

import java.time.Duration;
import java.util.regex.Pattern;

final class Validation {

    private static final Pattern QUEUE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");

    private Validation() {}

    static String queueName(String name) {
        if (name == null || !QUEUE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid queue name (allowed: [A-Za-z0-9._-]{1,200}): " + name);
        }
        return name;
    }

    static Duration positive(Duration d, String what) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException(what + " must be positive: " + d);
        }
        return d;
    }
}
