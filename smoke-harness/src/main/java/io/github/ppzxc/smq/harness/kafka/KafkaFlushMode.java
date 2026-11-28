package io.github.ppzxc.smq.harness.kafka;

import java.util.List;

enum KafkaFlushMode {
    DEFAULT("default", null),
    EVERY_MESSAGE("flush-every-message", "1");

    private final String name;
    private final String topicFlushMessages;

    KafkaFlushMode(final String name, final String topicFlushMessages) {
        this.name = name;
        this.topicFlushMessages = topicFlushMessages;
    }

    String label() {
        return this.name;
    }

    String topicFlushMessages() {
        return this.topicFlushMessages;
    }

    static List<KafkaFlushMode> all() {
        return List.of(values());
    }
}
