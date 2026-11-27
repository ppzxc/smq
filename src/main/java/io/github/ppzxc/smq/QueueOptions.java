package io.github.ppzxc.smq;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

public record QueueOptions(OptionalInt maxDeliveries, Optional<String> deadLetterQueue, boolean singleActiveConsumer) {

    public QueueOptions {
        Objects.requireNonNull(maxDeliveries, "maxDeliveries");
        Objects.requireNonNull(deadLetterQueue, "deadLetterQueue");
        if (maxDeliveries.isPresent() && maxDeliveries.getAsInt() < 1) {
            throw new IllegalArgumentException("maxDeliveries must be >= 1: " + maxDeliveries.getAsInt());
        }
        deadLetterQueue.ifPresent(Validation::queueName);
        if (maxDeliveries.isPresent() && deadLetterQueue.isEmpty()) {
            throw new IllegalArgumentException("maxDeliveries requires a dead letter queue");
        }
    }

    public static QueueOptions defaults() {
        return new QueueOptions(OptionalInt.empty(), Optional.empty(), false);
    }

    public QueueOptions maxDeliveries(final int limit, final String dlq) {
        return new QueueOptions(OptionalInt.of(limit), Optional.of(Validation.queueName(dlq)), singleActiveConsumer);
    }

    public QueueOptions deadLetterQueue(final String dlq) {
        return new QueueOptions(maxDeliveries, Optional.of(Validation.queueName(dlq)), singleActiveConsumer);
    }

    public QueueOptions singleActiveConsumer(final boolean enabled) {
        return new QueueOptions(maxDeliveries, deadLetterQueue, enabled);
    }
}
