package io.github.ppzxc.smq.harness.kafka;

import java.util.Iterator;
import java.util.Map;

final class KafkaJson {

    private KafkaJson() {}

    static String encode(final Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String text) {
            return quote(text);
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            final StringBuilder json = new StringBuilder("{");
            final Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
            while (entries.hasNext()) {
                final Map.Entry<?, ?> entry = entries.next();
                json.append(quote(entry.getKey().toString())).append(':').append(encode(entry.getValue()));
                if (entries.hasNext()) {
                    json.append(',');
                }
            }
            return json.append('}').toString();
        }
        if (value instanceof Iterable<?> values) {
            final StringBuilder json = new StringBuilder("[");
            final Iterator<?> iterator = values.iterator();
            while (iterator.hasNext()) {
                json.append(encode(iterator.next()));
                if (iterator.hasNext()) {
                    json.append(',');
                }
            }
            return json.append(']').toString();
        }
        throw new IllegalArgumentException(
                "unsupported JSON value: " + value.getClass().getName());
    }

    static String quote(final String text) {
        final StringBuilder result = new StringBuilder("\"");
        for (int index = 0; index < text.length(); index++) {
            final char character = text.charAt(index);
            switch (character) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (character < 0x20) {
                        result.append(String.format("\\u%04x", (int) character));
                    } else {
                        result.append(character);
                    }
                }
            }
        }
        return result.append('"').toString();
    }
}
