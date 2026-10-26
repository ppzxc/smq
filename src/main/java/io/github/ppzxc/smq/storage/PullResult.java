package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import java.time.Instant;
import java.util.List;

/**
 * 꺼내기 결과. 메시지가 없으면 {@code token}과 {@code leaseDeadline}은 null이다.
 * {@code deadLettersRemain}은 메시지가 없는데 dead letter 이동 상한에 걸려 옮기지 못한 도달 메시지가 남았다는
 * 표시다. 옮기고 나면 그 뒤에 준비 메시지가 있을 수 있다.
 * 코어는 이때 기다리지 않고 바로 다시 꺼낸다.
 */
public record PullResult(LeaseToken token, Instant leaseDeadline, List<Message> messages, boolean deadLettersRemain) {

    public static PullResult of(LeaseToken token, Instant leaseDeadline, List<Message> messages) {
        return new PullResult(token, leaseDeadline, messages, false);
    }

    public static PullResult empty() {
        return new PullResult(null, null, List.of(), false);
    }

    /** 이동 상한에 걸려 비었지만 옮길 도달 메시지가 더 남은 결과. */
    public static PullResult emptyDeadLettersRemain() {
        return new PullResult(null, null, List.of(), true);
    }
}
