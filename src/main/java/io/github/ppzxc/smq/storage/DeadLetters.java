package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * dead letter 이동의 {@code smq-} 시스템 헤더 규칙. reject와 최대 전달 횟수 도달이 같은 규칙을 쓴다.
 * 원래 헤더는 그대로 두고, 같은 이름이 있으면 시스템 헤더가 덮어쓴다.
 */
public final class DeadLetters {

    public static final String ORIGINAL_QUEUE = "smq-original-queue";
    public static final String ORIGINAL_MESSAGE_ID = "smq-original-message-id";
    public static final String LAST_DELIVERY_COUNT = "smq-last-delivery-count";
    public static final String MOVED_AT = "smq-moved-at";
    public static final String REASON = "smq-reason";

    public static final String REASON_REJECTED = "rejected";
    public static final String REASON_MAX_DELIVERIES = "max-deliveries";

    private DeadLetters() {
    }

    public static Map<String, String> headers(Map<String, String> original, String originalQueue,
            MessageId originalId, int lastDeliveryCount, Instant movedAt, String reason) {
        Map<String, String> headers = new LinkedHashMap<>(original);
        headers.put(ORIGINAL_QUEUE, originalQueue);
        headers.put(ORIGINAL_MESSAGE_ID, String.valueOf(originalId.value()));
        headers.put(LAST_DELIVERY_COUNT, String.valueOf(lastDeliveryCount));
        headers.put(MOVED_AT, movedAt.toString());
        headers.put(REASON, reason);
        return headers;
    }
}
