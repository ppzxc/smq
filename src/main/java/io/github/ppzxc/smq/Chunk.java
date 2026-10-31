package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.LeaseToken;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * 한 번 꺼낸 메시지 묶음과 그 리스. 정산 상태를 기억하지 않는 불변 값이다. 리스 토큰은 숨긴다.
 * 메시지가 없는 빈 청크의 {@code leaseDeadline}은 null이다.
 */
public final class Chunk {

    private final String queue;
    private final List<Message> messages;
    private final @Nullable Instant leaseDeadline;
    private final @Nullable LeaseToken token;

    Chunk(String queue, List<Message> messages, @Nullable Instant leaseDeadline, @Nullable LeaseToken token) {
        this.queue = queue;
        this.messages = List.copyOf(messages);
        this.leaseDeadline = leaseDeadline;
        this.token = token;
    }

    public String queue() {
        return queue;
    }

    public List<Message> messages() {
        return messages;
    }

    public @Nullable Instant leaseDeadline() {
        return leaseDeadline;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Nullable
    LeaseToken token() {
        return token;
    }

    List<MessageId> messageIds() {
        return messages.stream().map(Message::id).toList();
    }

    /** 메시지마다 다른 처리 결과를 고르는 정산 빌더. */
    public Settlement.Builder settle() {
        return new Settlement.Builder(this);
    }

    /** 청크의 모든 메시지를 ACK하는 정산 값. */
    public Settlement ackAll() {
        return settle().ackRest();
    }

    /** 청크의 모든 메시지를 {@code delay} 지연 NACK하는 정산 값. */
    public Settlement nackAll(Duration delay) {
        return settle().nackRest(delay);
    }

    /** 메시지 하나만 ACK하는 정산 값. 청크는 정산 상태를 기억하지 않으므로 반영은 {@code queue.settle}이 한다. */
    public Settlement ack(MessageId id) {
        return settle().ack(id).build();
    }

    @Override
    public String toString() {
        return "Chunk[queue=" + queue + ", messages=" + messages.size() + ", leaseDeadline=" + leaseDeadline + "]";
    }
}
