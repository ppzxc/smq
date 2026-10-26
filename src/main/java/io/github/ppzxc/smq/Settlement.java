package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.LeaseToken;
import java.util.List;

/** 청크에서 만든 정산 값. 불변이며 같은 값을 다시 반영해도 안전하다. */
public final class Settlement {

    private final String queue;
    private final LeaseToken token;
    private final List<Disposition> items;

    Settlement(String queue, LeaseToken token, List<MessageId> acks) {
        this.queue = queue;
        this.token = token;
        this.items = acks.stream().map(id -> new Disposition(id, Disposition.Kind.ACK)).toList();
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
}
