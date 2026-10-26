package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.ConditionChangeHint;
import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

final class DefaultSmq implements Smq {

    private final Storage storage;
    private final InstantSource clock;
    private final Duration leaseDuration;
    private final PullWaiter waiter;

    DefaultSmq(Storage storage, InstantSource clock, Duration leaseDuration) {
        this.storage = storage;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
        this.waiter = new PullWaiter(storage.changeHint().orElseGet(ConditionChangeHint::new));
    }

    @Override
    public Queue declare(String name, QueueOptions options) {
        Validation.queueName(name);
        Objects.requireNonNull(options, "options");
        if (options.deadLetterQueue().filter(name::equals).isPresent()) {
            throw new IllegalArgumentException("a queue cannot be its own dead letter queue: " + name);
        }
        storage.declare(name, options);
        return new DefaultQueue(storage, clock, leaseDuration, waiter, name, options);
    }

    @Override
    public Queue queue(String name) {
        Validation.queueName(name);
        QueueOptions options = storage.options(name).orElseThrow(() -> new QueueNotFoundException(name));
        return new DefaultQueue(storage, clock, leaseDuration, waiter, name, options);
    }

    @Override
    public boolean delete(String name) {
        Validation.queueName(name);
        return storage.delete(name);
    }

    @Override
    public void close() {
        waiter.close();
        storage.close();
    }
}
