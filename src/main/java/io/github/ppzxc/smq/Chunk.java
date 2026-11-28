package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.LeaseToken;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.jspecify.annotations.Nullable;

public final class Chunk {

    private final Object session;
    private final String queue;
    private final List<Message> messages;
    private final @Nullable Instant leaseDeadline;
    private final @Nullable LeaseToken token;

    Chunk(
            final Object session,
            final String queue,
            final List<Message> messages,
            final @Nullable Instant leaseDeadline,
            final @Nullable LeaseToken token) {
        this.session = session;
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

    Object session() {
        return session;
    }

    List<MessageId> messageIds() {
        return messages.stream().map(Message::id).toList();
    }

    public Settlement.Builder settle() {
        return new Settlement.Builder(this);
    }

    public Settlement ackAll() {
        return settle().ackRest();
    }

    public Settlement nackAll(final Duration delay) {
        return settle().nackRest(delay);
    }

    public Settlement ack(final MessageId id) {
        return settle().ack(id).build();
    }

    @Override
    public String toString() {
        return "Chunk[queue=" + queue + ", messages=" + messages.size() + ", leaseDeadline=" + leaseDeadline + "]";
    }
}
