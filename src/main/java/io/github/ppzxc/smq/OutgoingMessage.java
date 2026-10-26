package io.github.ppzxc.smq;

import java.time.Duration;
import java.util.Map;

/** 발행할 메시지. {@code delay}가 {@link Duration#ZERO}이면 지연 없이 바로 꺼낼 수 있다. */
public record OutgoingMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public OutgoingMessage {
        payload = payload.clone();
        headers = Map.copyOf(headers);
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative: " + delay);
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
