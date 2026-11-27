package io.github.ppzxc.smq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

@SuppressWarnings("ArrayRecordComponent") // Payload is copied at construction and access.
public record OutgoingMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public OutgoingMessage {
        payload = payload.clone();
        headers = Map.copyOf(headers);
        headers.forEach((key, value) -> {
            if (!StandardCharsets.UTF_8.newEncoder().canEncode(key)
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
                throw new IllegalArgumentException("headers must be valid UTF-8 strings");
            }
        });
        if (delay == null || delay.isNegative() || delay.compareTo(Validation.MAX_DELAY_OR_LEASE) > 0) {
            throw new IllegalArgumentException("delay must be between zero and 36500 days: " + delay);
        }
    }

    public OutgoingMessage(final byte[] payload, final Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }

    public static OutgoingMessage of(final byte[] payload) {
        return new OutgoingMessage(payload, Map.of());
    }

    public OutgoingMessage withHeaders(final Map<String, String> headers) {
        return new OutgoingMessage(payload, headers, delay);
    }

    public OutgoingMessage withDelay(final Duration delay) {
        return new OutgoingMessage(payload, headers, Validation.positive(delay, "delay"));
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
