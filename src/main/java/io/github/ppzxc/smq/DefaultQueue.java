package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Extended;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import io.github.ppzxc.smq.storage.Result;
import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class DefaultQueue implements Queue {

    private final Storage storage;
    private final InstantSource clock;
    private final Duration defaultLease;
    private final PullWaiter waiter;
    private final Object session;
    private final String name;
    private final QueueOptions options;

    DefaultQueue(
            final Storage storage,
            final InstantSource clock,
            final Duration defaultLease,
            final PullWaiter waiter,
            final Object session,
            final String name,
            final QueueOptions options) {
        this.storage = storage;
        this.clock = clock;
        this.defaultLease = defaultLease;
        this.waiter = waiter;
        this.session = session;
        this.name = name;
        this.options = options;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public QueueOptions options() {
        return options;
    }

    @Override
    public QueueCounts counts() {
        waiter.requireOpen();
        return storage.counts(name);
    }

    @Override
    public List<MessageId> publish(final List<OutgoingMessage> batch) {
        final List<NewMessage> converted;
        final List<MessageId> ids;
        Objects.requireNonNull(batch, "batch");
        waiter.requireOpen();
        converted = batch.stream()
                .map(m -> {
                    Objects.requireNonNull(m, "batch element");
                    return new NewMessage(m.payload(), m.headers(), m.delay());
                })
                .toList();
        if (converted.isEmpty()) {
            return List.of();
        }
        ids = storage.publish(name, converted, clock);
        waiter.wake(name);
        return ids;
    }

    @Override
    public MessageId publish(final OutgoingMessage message) {
        return publish(List.of(Objects.requireNonNull(message, "message"))).getFirst();
    }

    @Override
    public Chunk pull(final int max, final Duration wait) throws InterruptedException {
        return pull(max, wait, defaultLease);
    }

    @Override
    public Chunk pull(final int max, final Duration wait, final Duration leaseDuration) throws InterruptedException {
        final long deadline;
        if (max < 1) {
            throw new IllegalArgumentException("max must be >= 1: " + max);
        }
        if (wait == null || wait.isNegative()) {
            throw new IllegalArgumentException("wait must not be negative: " + wait);
        }
        Validation.positive(leaseDuration, "leaseDuration");
        deadline = System.nanoTime() + Validation.saturatedWaitNanos(wait);
        return waiter.await(
                name,
                deadline,
                () -> {
                    final Duration remaining;
                    final PullResult r;
                    final long nanos;
                    remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                    r = storage.pull(name, max, leaseDuration, clock, remaining, this::wakeDeadLetter);
                    if (!r.messages().isEmpty()) {
                        return PullWaiter.Attempt.found(new Chunk(
                                session,
                                name,
                                r.messages(),
                                Objects.requireNonNull(r.leaseDeadline(), "non-empty pull must have a lease deadline"),
                                Objects.requireNonNull(r.token(), "non-empty pull must have a lease token")));
                    }

                    if (r.deadLettersRemain()) {
                        return PullWaiter.Attempt.retryNow();
                    }
                    nanos = deadline - System.nanoTime();
                    if (nanos <= 0) {
                        return PullWaiter.Attempt.empty();
                    }
                    return PullWaiter.Attempt.empty(storage.nextReadyAt(name, clock, Duration.ofNanos(nanos))
                            .map(at -> Duration.between(clock.instant(), at))
                            .orElse(Duration.ZERO));
                },
                () -> new Chunk(session, name, List.of(), null, null));
    }

    @Override
    public Outcomes settle(final Settlement settlement) {
        final Map<MessageId, Outcome> byMessage;
        Objects.requireNonNull(settlement, "settlement");
        requireOwn(settlement.queue(), settlement.session(), "settlement");
        waiter.requireOpen();
        if (settlement.items().isEmpty()) {
            return new Outcomes(Map.of());
        }
        byMessage = new LinkedHashMap<>();
        for (final Result r : storage.settle(
                Objects.requireNonNull(settlement.token(), "non-empty settlement must have a lease token"),
                settlement.items(),
                clock,
                this::wakeDeadLetter)) {
            byMessage.put(r.id(), r.outcome());
        }

        waiter.wake(name);
        return new Outcomes(byMessage);
    }

    @Override
    public Extension extend(final Chunk chunk, final Duration leaseDuration) {
        final Extended extended;
        final Map<MessageId, Outcome> byMessage;
        Objects.requireNonNull(chunk, "chunk");
        Validation.positive(leaseDuration, "leaseDuration");
        requireOwn(chunk.queue(), chunk.session(), "chunk");
        waiter.requireOpen();
        if (chunk.isEmpty()) {
            return new Extension(null, new Outcomes(Map.of()));
        }
        extended = storage.extend(
                Objects.requireNonNull(chunk.token(), "non-empty chunk must have a lease token"),
                chunk.messageIds(),
                leaseDuration,
                clock);
        byMessage = new LinkedHashMap<>();
        for (final Result r : extended.results()) {
            byMessage.put(r.id(), r.outcome());
        }
        waiter.wake(name);
        return new Extension(extended.leaseDeadline(), new Outcomes(byMessage));
    }

    private void wakeDeadLetter(final Optional<String> destination) {
        destination.ifPresentOrElse(waiter::wake, waiter::wake);
    }

    private void requireOwn(final String sourceQueue, final Object sourceSession, final String what) {
        if (!name.equals(sourceQueue) || !session.equals(sourceSession)) {
            throw new IllegalArgumentException(what + " is not from this queue session");
        }
    }
}
