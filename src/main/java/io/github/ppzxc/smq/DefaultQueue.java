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

final class DefaultQueue implements Queue {

    private final Storage storage;
    private final InstantSource clock;
    private final Duration defaultLease;
    private final PullWaiter waiter;
    private final String name;
    private final QueueOptions options;

    DefaultQueue(Storage storage, InstantSource clock, Duration defaultLease, PullWaiter waiter, String name, QueueOptions options) {
        this.storage = storage;
        this.clock = clock;
        this.defaultLease = defaultLease;
        this.waiter = waiter;
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
                    return new NewMessage(m.payload(), m.headers(), m.delay());
                })
                .toList();
        if (converted.isEmpty()) {
            return List.of();
        }
        List<MessageId> ids = storage.publish(name, converted, clock.instant());
        waiter.wake();
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
        return waiter.await(wait, () -> {
            PullResult r;
            do {
                r = storage.pull(name, max, leaseDuration, clock.instant());
                // 이동 상한에 걸려 빈 결과면 기다리지 않고 바로 다시 꺼낸다.
            } while (r.messages().isEmpty() && r.maybeMore());
            return r.messages().isEmpty() ? null : new Chunk(name, r.messages(), r.leaseDeadline(), r.token());
        }, () -> new Chunk(name, List.of(), null, null));
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
        // NACK은 메시지를 다시 준비로 돌릴 수 있다. 깨우기는 저장소가 아니라 코어가 맡는다.
        waiter.wake();
        return new Outcomes(byMessage);
    }

    @Override
    public Extension extend(Chunk chunk, Duration leaseDuration) {
        Objects.requireNonNull(chunk, "chunk");
        Validation.positive(leaseDuration, "leaseDuration");
        if (chunk.isEmpty()) {
            return new Extension(null, new Outcomes(Map.of()));
        }
        Extended extended = storage.extend(chunk.token(), chunk.messageIds(), leaseDuration, clock.instant());
        Map<MessageId, Outcome> byMessage = new LinkedHashMap<>();
        for (Result r : extended.results()) {
            byMessage.put(r.id(), r.outcome());
        }
        return new Extension(extended.leaseDeadline(), new Outcomes(byMessage));
    }
}
