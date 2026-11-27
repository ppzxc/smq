package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.LeaseToken;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

public final class Settlement {

    private final Object session;
    private final String queue;
    private final @Nullable LeaseToken token;
    private final List<Disposition> items;

    Settlement(
            final Object session, final String queue, final @Nullable LeaseToken token, final List<Disposition> items) {
        this.session = session;
        this.queue = queue;
        this.token = token;
        this.items = items;
    }

    String queue() {
        return queue;
    }

    Object session() {
        return session;
    }

    @Nullable
    LeaseToken token() {
        return token;
    }

    List<Disposition> items() {
        return items;
    }

    public static final class Builder {

        private final Chunk chunk;
        private final Set<MessageId> chunkIds;
        private final Map<MessageId, Disposition> items = new LinkedHashMap<>();

        Builder(final Chunk chunk) {
            this.chunk = chunk;
            this.chunkIds = new LinkedHashSet<>(chunk.messageIds());
        }

        public Builder ack(final MessageId id) {
            return put(new Disposition(id, Disposition.Kind.ACK));
        }

        public Builder nack(final MessageId id) {
            return put(new Disposition(id, Disposition.Kind.NACK));
        }

        public Builder nack(final MessageId id, final Duration delay) {
            return put(new Disposition(id, Disposition.Kind.NACK, Validation.nonNegative(delay, "delay")));
        }

        public Builder reject(final MessageId id) {
            return put(new Disposition(id, Disposition.Kind.REJECT));
        }

        private Builder put(final Disposition d) {
            if (!chunkIds.contains(d.id())) {
                throw new IllegalArgumentException("message is not in the chunk: " + d.id());
            }
            if (items.putIfAbsent(d.id(), d) != null) {
                throw new IllegalArgumentException("message already has a disposition: " + d.id());
            }
            return this;
        }

        public Settlement ackRest() {
            for (final MessageId id : chunkIds) {
                items.putIfAbsent(id, new Disposition(id, Disposition.Kind.ACK));
            }
            return build();
        }

        public Settlement nackRest(final Duration delay) {
            Validation.nonNegative(delay, "delay");
            for (final MessageId id : chunkIds) {
                items.putIfAbsent(id, new Disposition(id, Disposition.Kind.NACK, delay));
            }
            return build();
        }

        public Settlement build() {
            return new Settlement(chunk.session(), chunk.queue(), chunk.token(), List.copyOf(items.values()));
        }
    }
}
