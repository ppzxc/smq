package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Duration;

/** 정산 한 건: 메시지 하나에 대한 처리. {@code delay}는 NACK에서만 뜻이 있고 {@link Duration#ZERO}이면 지연이 없다. */
public record Disposition(MessageId id, Kind kind, Duration delay) {

    public enum Kind {
        ACK,
        NACK
    }

    public Disposition(MessageId id, Kind kind) {
        this(id, kind, Duration.ZERO);
    }
}
