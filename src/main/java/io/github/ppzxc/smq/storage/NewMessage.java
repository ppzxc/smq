package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

@SuppressWarnings("ArrayRecordComponent") // Internal storage input retains its caller-owned payload.
public record NewMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public NewMessage {
        Objects.requireNonNull(delay, "delay");
    }

    public @Nullable Instant availableAt(final Instant now) {
        return delay.isZero() ? null : now.plus(delay);
    }

    public NewMessage(final byte[] payload, final Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }
}
