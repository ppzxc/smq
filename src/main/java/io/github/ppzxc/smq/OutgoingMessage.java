package io.github.ppzxc.smq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** 발행할 메시지. {@code delay}가 {@link Duration#ZERO}이면 지연 없이 바로 꺼낼 수 있다. 지연의 상한은 36,500일(100년)이며 상한을 포함한다. */
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

    public OutgoingMessage(byte[] payload, Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }

    public static OutgoingMessage of(byte[] payload) {
        return new OutgoingMessage(payload, Map.of());
    }

    public OutgoingMessage withHeaders(Map<String, String> headers) {
        return new OutgoingMessage(payload, headers, delay);
    }

    /** 발행 시각부터 {@code delay}가 지나야 꺼낼 수 있게 한다. */
    public OutgoingMessage withDelay(Duration delay) {
        return new OutgoingMessage(payload, headers, Validation.positive(delay, "delay"));
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
