package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import java.time.Instant;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * 꺼내기 결과. 메시지가 없으면 {@code token}과 {@code leaseDeadline}은 null이다.
 * {@code deadLettersRemain}은 메시지가 없는데 dead letter 이동 상한에 걸려 옮기지 못한 도달 메시지가 남았다는
 * 표시다. 옮기고 나면 그 뒤에 준비 메시지가 있을 수 있다.
 * 코어는 이때 기다리지 않고 바로 다시 꺼낸다. {@code deadLettersMoved}는 이번 꺼내기가
 * 하나 이상 이동했음을 나타내며 코어가 대기자를 깨우는 데 쓴다.
 */
public record PullResult(
        @Nullable LeaseToken token,
        @Nullable Instant leaseDeadline,
        List<Message> messages,
        boolean deadLettersRemain,
        boolean deadLettersMoved) {

    public static PullResult of(LeaseToken token, Instant leaseDeadline, List<Message> messages) {
        return of(token, leaseDeadline, messages, false);
    }

    public static PullResult of(LeaseToken token, Instant leaseDeadline, List<Message> messages, boolean moved) {
        return new PullResult(token, leaseDeadline, messages, false, moved);
    }

    public static PullResult empty() {
        return empty(false);
    }

    public static PullResult empty(boolean moved) {
        return new PullResult(null, null, List.of(), false, moved);
    }

    /** 이동 상한에 걸려 비었지만 옮길 도달 메시지가 더 남은 결과. */
    public static PullResult emptyDeadLettersRemain() {
        return emptyDeadLettersRemain(false);
    }

    public static PullResult emptyDeadLettersRemain(boolean moved) {
        return new PullResult(null, null, List.of(), true, moved);
    }
}
