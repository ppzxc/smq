package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import java.time.Instant;
import java.util.List;

/** 꺼내기 결과. 메시지가 없으면 {@code token}과 {@code leaseDeadline}은 null이다. */
public record PullResult(LeaseToken token, Instant leaseDeadline, List<Message> messages) {

    public static PullResult empty() {
        return new PullResult(null, null, List.of());
    }
}
