package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** 발행할 메시지 한 건. {@code delay}가 {@link Duration#ZERO}이면 지연이 없다. */
public record NewMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public NewMessage {
        Objects.requireNonNull(delay, "delay");
    }

    /** 꺼낼 수 있게 되는 시각. 지연이 없으면 null이다. */
    public Instant availableAt(Instant now) {
        return delay.isZero() ? null : now.plus(delay);
    }

    public NewMessage(byte[] payload, Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }
}
