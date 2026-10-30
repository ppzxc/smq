package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** 발행할 메시지 한 건. {@code delay}가 {@link Duration#ZERO}이면 지연이 없다. */
// payload 배열은 저장소 경계를 넘지 않으며 호출자가 복사해 전달한다.
@SuppressWarnings("ArrayRecordComponent")
public record NewMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public NewMessage {
        Objects.requireNonNull(delay, "delay");
    }

    /** 꺼낼 수 있게 되는 시각. 지연이 없으면 null이다. */
    public @Nullable Instant availableAt(Instant now) {
        return delay.isZero() ? null : now.plus(delay);
    }

    public NewMessage(byte[] payload, Map<String, String> headers) {
        this(payload, headers, Duration.ZERO);
    }
}
