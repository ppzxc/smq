package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DeadLetters {

    public static final String ORIGINAL_QUEUE = "smq-original-queue";
    public static final String ORIGINAL_MESSAGE_ID = "smq-original-message-id";
    public static final String LAST_DELIVERY_COUNT = "smq-last-delivery-count";
    public static final String MOVED_AT = "smq-moved-at";
    public static final String REASON = "smq-reason";

    public static final int MAX_MOVES_PER_PULL = 100;

    public enum Reason {
        REJECTED("rejected"),

        MAX_DELIVERIES("max-deliveries");

        private final String value;

        Reason(final String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    public record Origin(String queue, MessageId id, Map<String, String> headers, int deliveryCount) {
        public Origin {
            headers = Map.copyOf(headers);
        }
    }

    private DeadLetters() {}

    public static Map<String, String> headers(final Origin origin, final Reason reason, final Instant movedAt) {
        final Map<String, String> headers;
        headers = new LinkedHashMap<>(origin.headers());
        headers.put(ORIGINAL_QUEUE, origin.queue());
        headers.put(ORIGINAL_MESSAGE_ID, String.valueOf(origin.id().value()));
        headers.put(LAST_DELIVERY_COUNT, String.valueOf(origin.deliveryCount()));
        headers.put(MOVED_AT, movedAt.toString());
        headers.put(REASON, reason.value());
        return headers;
    }
}
