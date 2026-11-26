package io.github.ppzxc.smq;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("ArrayRecordComponent") // Payload is copied at construction and access.
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
