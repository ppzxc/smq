package io.github.ppzxc.smq.storage;

import java.util.Map;

public record NewMessage(byte[] payload, Map<String, String> headers) {
}
