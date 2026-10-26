package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import java.time.Instant;
import java.util.List;

/**
 * 꺼내기 결과. 메시지가 없으면 {@code token}과 {@code leaseDeadline}은 null이다.
 * {@code maybeMore}는 메시지가 없는데도 dead letter 이동 상한에 걸려 준비 메시지가 더 있을 수 있다는 표시다.
 * 코어는 이때 기다리지 않고 바로 다시 꺼낸다.
 */
public record PullResult(LeaseToken token, Instant leaseDeadline, List<Message> messages, boolean maybeMore) {

    public PullResult(LeaseToken token, Instant leaseDeadline, List<Message> messages) {
        this(token, leaseDeadline, messages, false);
    }

    public static PullResult empty() {
        return new PullResult(null, null, List.of(), false);
    }

    /** 이동 상한에 걸려 비었지만 더 있을 수 있는 결과. */
    public static PullResult emptyMaybeMore() {
        return new PullResult(null, null, List.of(), true);
    }
}
