package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.LeaseToken;
import java.time.Instant;
import java.util.List;

/**
 * 한 번 꺼낸 메시지 묶음과 그 리스. 정산 상태를 기억하지 않는 불변 값이다. 리스 토큰은 숨긴다.
 * 메시지가 없는 빈 청크의 {@code leaseDeadline}은 null이다.
 */
public final class Chunk {

    private final String queue;
    private final List<Message> messages;
    private final Instant leaseDeadline;
    private final LeaseToken token;

    Chunk(String queue, List<Message> messages, Instant leaseDeadline, LeaseToken token) {
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

    public Instant leaseDeadline() {
        return leaseDeadline;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    LeaseToken token() {
        return token;
    }

    List<MessageId> messageIds() {
        return messages.stream().map(Message::id).toList();
    }

    /** 청크의 모든 메시지를 ACK하는 정산 값. */
    public Settlement ackAll() {
        return new Settlement(queue, token, messageIds());
    }

    @Override
    public String toString() {
        return "Chunk[queue=" + queue + ", messages=" + messages.size() + ", leaseDeadline=" + leaseDeadline + "]";
    }
}
