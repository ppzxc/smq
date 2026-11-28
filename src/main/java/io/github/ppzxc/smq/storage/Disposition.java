package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public record Disposition(MessageId id, Kind kind, Duration delay) {

    public enum Kind {
        ACK,
        NACK,

        REJECT
    }

    public Disposition {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(kind, "kind");
        if (delay.isNegative() || (kind != Kind.NACK && !delay.isZero())) {
            throw new IllegalArgumentException("delay is only allowed for NACK and must not be negative: " + delay);
        }
    }

    public @Nullable Instant availableAt(final Instant now) {
        return delay.isZero() ? null : now.plus(delay);
    }

    public Disposition(final MessageId id, final Kind kind) {
        this(id, kind, Duration.ZERO);
    }
}
