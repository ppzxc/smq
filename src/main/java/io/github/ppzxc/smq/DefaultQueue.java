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
            Storage storage,
            InstantSource clock,
            Duration defaultLease,
            PullWaiter waiter,
            Object session,
            String name,
            QueueOptions options) {
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
    public List<MessageId> publish(List<OutgoingMessage> batch) {
        Objects.requireNonNull(batch, "batch");
        waiter.requireOpen();
        List<NewMessage> converted = batch.stream()
                .map(m -> {
                    Objects.requireNonNull(m, "batch element");
                    return new NewMessage(m.payload(), m.headers(), m.delay());
                })
                .toList();
        if (converted.isEmpty()) {
            return List.of();
        }
        List<MessageId> ids = storage.publish(name, converted, clock);
        waiter.wake(name);
        return ids;
    }

    @Override
    public MessageId publish(OutgoingMessage message) {
        return publish(List.of(Objects.requireNonNull(message, "message"))).getFirst();
    }

    @Override
    public Chunk pull(int max, Duration wait) throws InterruptedException {
        return pull(max, wait, defaultLease);
    }

    @Override
    public Chunk pull(int max, Duration wait, Duration leaseDuration) throws InterruptedException {
        if (max < 1) {
            throw new IllegalArgumentException("max must be >= 1: " + max);
        }
        if (wait == null || wait.isNegative()) {
            throw new IllegalArgumentException("wait must not be negative: " + wait);
        }
        Validation.positive(leaseDuration, "leaseDuration");
        long deadline = System.nanoTime() + Validation.saturatedWaitNanos(wait);
        return waiter.await(
                name,
                deadline,
                () -> {
                    Duration remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                    PullResult r = storage.pull(name, max, leaseDuration, clock, remaining, this::wakeDeadLetter);
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
                    long nanos = deadline - System.nanoTime();
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
    public Outcomes settle(Settlement settlement) {
        Objects.requireNonNull(settlement, "settlement");
        requireOwn(settlement.queue(), settlement.session(), "settlement");
        waiter.requireOpen();
        if (settlement.items().isEmpty()) {
            return new Outcomes(Map.of());
        }
        Map<MessageId, Outcome> byMessage = new LinkedHashMap<>();
        for (Result r : storage.settle(
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
    public Extension extend(Chunk chunk, Duration leaseDuration) {
        Objects.requireNonNull(chunk, "chunk");
        Validation.positive(leaseDuration, "leaseDuration");
        requireOwn(chunk.queue(), chunk.session(), "chunk");
        waiter.requireOpen();
        if (chunk.isEmpty()) {
            return new Extension(null, new Outcomes(Map.of()));
        }
        Extended extended = storage.extend(
                Objects.requireNonNull(chunk.token(), "non-empty chunk must have a lease token"),
                chunk.messageIds(),
                leaseDuration,
                clock);
        Map<MessageId, Outcome> byMessage = new LinkedHashMap<>();
        for (Result r : extended.results()) {
            byMessage.put(r.id(), r.outcome());
        }
        waiter.wake(name);
        return new Extension(extended.leaseDeadline(), new Outcomes(byMessage));
    }

    private void wakeDeadLetter(Optional<String> destination) {
        destination.ifPresentOrElse(waiter::wake, waiter::wake);
    }

    /** 정산과 연장은 이 세션의 같은 큐에서 꺼낸 청크로만 할 수 있다. */
    private void requireOwn(String sourceQueue, Object sourceSession, String what) {
        if (!name.equals(sourceQueue) || !session.equals(sourceSession)) {
            throw new IllegalArgumentException(what + " is not from this queue session");
        }
    }
}
