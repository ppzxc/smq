package io.github.ppzxc.smq;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * 큐 선언 옵션. 불변 값이다. 최대 전달 횟수를 지정하려면 dead letter 큐가 필수이고, 지정하지 않으면 무제한이다.
 * 리스 시간은 큐 속성이 아니다.
 */
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

    /** 최대 전달 횟수와 넘겼을 때 옮길 dead letter 큐를 함께 지정한다. */
    public QueueOptions maxDeliveries(int limit, String dlq) {
        return new QueueOptions(OptionalInt.of(limit), Optional.of(Validation.queueName(dlq)), singleActiveConsumer);
    }

    /** dead letter 큐만 지정한다(reject한 메시지가 간다). 최대 전달 횟수는 그대로 둔다. */
    public QueueOptions deadLetterQueue(String dlq) {
        return new QueueOptions(maxDeliveries, Optional.of(Validation.queueName(dlq)), singleActiveConsumer);
    }

    public QueueOptions singleActiveConsumer(boolean enabled) {
        return new QueueOptions(maxDeliveries, deadLetterQueue, enabled);
    }
}
