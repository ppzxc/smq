package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * dead letter 이동의 {@code smq-} 시스템 헤더 규칙. reject와 최대 전달 횟수 도달이 같은 규칙을 쓴다.
 * 원래 헤더는 그대로 두되, {@code smq-} 접두사는 시스템 예약이다. 같은 이름의 사용자 헤더는 시스템 헤더가 덮어쓴다.
 */
public final class DeadLetters {

    public static final String ORIGINAL_QUEUE = "smq-original-queue";
    public static final String ORIGINAL_MESSAGE_ID = "smq-original-message-id";
    public static final String LAST_DELIVERY_COUNT = "smq-last-delivery-count";
    public static final String MOVED_AT = "smq-moved-at";
    public static final String REASON = "smq-reason";

    /** dead letter 이동의 이유. {@link #value()}가 {@code smq-reason} 헤더 값이다. */
    public enum Reason {
        REJECTED("rejected"),
        /** 최대 전달 횟수 도달 이동(#29)이 쓸 값. 헤더 값 규칙을 한 곳에 모아 두려고 미리 정의했다. */
        MAX_DELIVERIES("max-deliveries");

        private final String value;

        Reason(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /** 옮겨지기 전 원래 메시지의 위치와 상태. */
    public record Origin(String queue, MessageId id, Map<String, String> headers, int deliveryCount) {
    }

    private DeadLetters() {
    }

    public static Map<String, String> headers(Origin origin, Reason reason, Instant movedAt) {
        Map<String, String> headers = new LinkedHashMap<>(origin.headers());
        headers.put(ORIGINAL_QUEUE, origin.queue());
        headers.put(ORIGINAL_MESSAGE_ID, String.valueOf(origin.id().value()));
        headers.put(LAST_DELIVERY_COUNT, String.valueOf(origin.deliveryCount()));
        headers.put(MOVED_AT, movedAt.toString());
        headers.put(REASON, reason.value());
        return headers;
    }
}
