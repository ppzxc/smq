package io.github.ppzxc.smq;

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

final class DefaultQueue implements Queue {

    private static final long POLL_NANOS = Duration.ofMillis(5).toNanos();

    private final Storage storage;
    private final InstantSource clock;
    private final Duration defaultLease;
    private final String name;
    private final QueueOptions options;

    DefaultQueue(Storage storage, InstantSource clock, Duration defaultLease, String name, QueueOptions options) {
        this.storage = storage;
        this.clock = clock;
        this.defaultLease = defaultLease;
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
        return storage.counts(name, clock.instant());
    }

    @Override
    public List<MessageId> publish(List<OutgoingMessage> batch) {
        Objects.requireNonNull(batch, "batch");
        List<NewMessage> converted = batch.stream()
                .map(m -> {
                    Objects.requireNonNull(m, "batch element");
                    return new NewMessage(m.payload(), m.headers());
                })
                .toList();
        if (converted.isEmpty()) {
            return List.of();
        }
        return storage.publish(name, converted, clock.instant());
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
        // wait는 주입한 시계가 아니라 실제 경과 시간으로 잰다.
        long deadline = System.nanoTime() + wait.toNanos();
        while (true) {
            PullResult r = storage.pull(name, max, leaseDuration, clock.instant());
            if (!r.messages().isEmpty()) {
                return new Chunk(name, r.messages(), r.leaseDeadline(), r.token());
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return new Chunk(name, List.of(), null, null);
            }
            Thread.sleep(Duration.ofNanos(Math.min(POLL_NANOS, remaining)));
        }
    }

    @Override
    public Outcomes settle(Settlement settlement) {
        Objects.requireNonNull(settlement, "settlement");
        if (settlement.items().isEmpty()) {
            return new Outcomes(Map.of());
        }
        Map<MessageId, Outcome> byMessage = new LinkedHashMap<>();
        for (Result r : storage.settle(settlement.token(), settlement.items(), clock.instant())) {
            byMessage.put(r.id(), r.outcome());
        }
        return new Outcomes(byMessage);
    }
}
