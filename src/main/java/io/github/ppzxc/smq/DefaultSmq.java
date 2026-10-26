package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

final class DefaultSmq implements Smq {

    private final Storage storage;
    private final InstantSource clock;
    private final Duration leaseDuration;

    DefaultSmq(Storage storage, InstantSource clock, Duration leaseDuration) {
        this.storage = storage;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
    }

    @Override
    public Queue declare(String name, QueueOptions options) {
        Validation.queueName(name);
        Objects.requireNonNull(options, "options");
        storage.declare(name, options);
        return new DefaultQueue(storage, clock, leaseDuration, name, options);
    }

    @Override
    public Queue queue(String name) {
        Validation.queueName(name);
        if (!storage.exists(name)) {
            throw new QueueNotFoundException(name);
        }
        return new DefaultQueue(storage, clock, leaseDuration, name, QueueOptions.defaults());
    }

    @Override
    public void close() {
        storage.close();
    }
}
