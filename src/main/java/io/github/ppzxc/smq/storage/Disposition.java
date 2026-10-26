package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;

/** 정산 한 건: 메시지 하나에 대한 처리. */
public record Disposition(MessageId id, Kind kind) {

    public enum Kind {
        ACK
    }
}
