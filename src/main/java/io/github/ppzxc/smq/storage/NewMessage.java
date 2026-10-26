package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.util.Map;

/** 발행할 메시지 한 건. {@code delay}가 {@link Duration#ZERO}이면 지연이 없다. */
public record NewMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public NewMessage(byte[] payload, Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }
}
