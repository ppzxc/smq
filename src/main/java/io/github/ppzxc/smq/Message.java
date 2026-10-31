package io.github.ppzxc.smq;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

// 배열은 생성자와 접근자에서 복사해 공개 API의 불변성 계약을 보장한다.
@SuppressWarnings("ArrayRecordComponent")
public record Message(
        MessageId id, byte[] payload, Map<String, String> headers, int deliveryCount, Instant publishedAt) {

    public Message {
        Objects.requireNonNull(id, "id");
        payload = payload.clone();
        headers = Map.copyOf(headers);
        Objects.requireNonNull(publishedAt, "publishedAt");
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
