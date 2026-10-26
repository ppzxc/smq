package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** 정산 한 건: 메시지 하나에 대한 처리. {@code delay}는 NACK에서만 뜻이 있고 {@link Duration#ZERO}이면 지연이 없다. */
public record Disposition(MessageId id, Kind kind, Duration delay) {

    public enum Kind {
        ACK,
        NACK
    }

    public Disposition {
        Objects.requireNonNull(delay, "delay");
    }

    /** NACK 뒤 다시 꺼낼 수 있게 되는 시각. 지연이 없으면 null이다. */
    public Instant availableAt(Instant now) {
        return delay.isZero() ? null : now.plus(delay);
    }

    public Disposition(MessageId id, Kind kind) {
        this(id, kind, Duration.ZERO);
    }
}
