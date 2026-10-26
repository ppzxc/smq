package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.LeaseToken;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 청크에서 만든 정산 값. 불변이며 같은 값을 다시 반영해도 안전하다. */
public final class Settlement {

    private final String queue;
    private final LeaseToken token;
    private final List<Disposition> items;

    Settlement(String queue, LeaseToken token, List<Disposition> items) {
        this.queue = queue;
        this.token = token;
        this.items = List.copyOf(items);
    }

    String queue() {
        return queue;
    }

    LeaseToken token() {
        return token;
    }

    List<Disposition> items() {
        return items;
    }

    /** 청크 안의 메시지마다 처리 결과를 골라 정산 값 하나로 모으는 빌더. "나머지"는 이 빌더에 담긴 것만 기준으로 계산한다. */
    public static final class Builder {

        private final Chunk chunk;
        private final Map<MessageId, Disposition> items = new LinkedHashMap<>();

        Builder(Chunk chunk) {
            this.chunk = chunk;
        }

        public Builder ack(MessageId id) {
            return put(new Disposition(id, Disposition.Kind.ACK));
        }

        /** 지연 없이 NACK한다. 메시지는 자기 id 자리로 돌아가 바로 다시 꺼낼 수 있다. */
        public Builder nack(MessageId id) {
            return put(new Disposition(id, Disposition.Kind.NACK));
        }

        private Builder put(Disposition d) {
            if (!chunk.messageIds().contains(d.id())) {
                throw new IllegalArgumentException("message is not in the chunk: " + d.id());
            }
            if (items.putIfAbsent(d.id(), d) != null) {
                throw new IllegalArgumentException("message already has a disposition: " + d.id());
            }
            return this;
        }

        /** 아직 처리 결과를 고르지 않은 나머지 메시지를 모두 ACK하고 마무리한다. */
        public Settlement ackRest() {
            for (MessageId id : chunk.messageIds()) {
                items.putIfAbsent(id, new Disposition(id, Disposition.Kind.ACK));
            }
            return build();
        }

        public Settlement build() {
            return new Settlement(chunk.queue(), chunk.token(), List.copyOf(items.values()));
        }
    }
}
