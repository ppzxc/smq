package io.github.ppzxc.smq;

import java.util.Map;

public record OutgoingMessage(byte[] payload, Map<String, String> headers) {

    public OutgoingMessage {
        payload = payload.clone();
        headers = Map.copyOf(headers);
    }

    public static OutgoingMessage of(byte[] payload) {
        return new OutgoingMessage(payload, Map.of());
    }

    public OutgoingMessage withHeaders(Map<String, String> headers) {
        return new OutgoingMessage(payload, headers);
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
