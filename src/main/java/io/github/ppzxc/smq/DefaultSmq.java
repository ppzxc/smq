package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Storage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

final class DefaultSmq implements Smq {

    private final Storage storage;
    private final InstantSource clock;
    private final Duration leaseDuration;
    private final PullWaiter waiter;
    private final Object session = new Object();

    DefaultSmq(final Storage storage, final InstantSource clock, final Duration leaseDuration) {
        this.storage = storage;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
        this.waiter = new PullWaiter(storage.changeHint(), storage.supportsQueueWakeups());
    }

    @Override
    public Queue declare(final String name, final QueueOptions options) {
        Validation.queueName(name);
        Objects.requireNonNull(options, "options");
        if (options.deadLetterQueue().filter(name::equals).isPresent()) {
            throw new IllegalArgumentException("a queue cannot be its own dead letter queue: " + name);
        }
        waiter.requireOpen();
        storage.declare(name, options);
        return new DefaultQueue(storage, clock, leaseDuration, waiter, session, name, options);
    }

    @Override
    public Queue queue(final String name) {
        final QueueOptions options;
        Validation.queueName(name);
        waiter.requireOpen();
        options = storage.options(name).orElseThrow(() -> new QueueNotFoundException(name));
        return new DefaultQueue(storage, clock, leaseDuration, waiter, session, name, options);
    }

    @Override
    public boolean delete(final String name) {
        Validation.queueName(name);
        waiter.requireOpen();
        return storage.delete(name);
    }

    @Override
    public void snapshot(final Path target) {
        Objects.requireNonNull(target, "target");
        waiter.requireOpen();
        storage.snapshot(target);
    }

    @Override
    public void close() {
        waiter.close();
        storage.close();
    }
}
