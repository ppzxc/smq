package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface Storage extends AutoCloseable {

    void declare(String queue, QueueOptions options);

    boolean exists(String queue);

    boolean delete(String queue);

    Optional<QueueOptions> options(String queue);

    QueueCounts counts(String queue);

    List<MessageId> publish(String queue, List<NewMessage> batch, InstantSource clock);

    PullResult pull(String queue, int max, Duration lease, InstantSource clock);

    default PullResult pull(
            final String queue, final int max, final Duration lease, final InstantSource clock, Duration wait)
            throws InterruptedException {
        return pull(queue, max, lease, clock);
    }

    default PullResult pull(
            final String queue,
            final int max,
            final Duration lease,
            final InstantSource clock,
            final Duration wait,
            final Consumer<Optional<String>> deadLetter)
            throws InterruptedException {
        final PullResult result;
        result = pull(queue, max, lease, clock, wait);
        if (result.deadLettersMoved()) {
            deadLetter.accept(Optional.empty());
        }
        return result;
    }

    default boolean supportsQueueWakeups() {
        return false;
    }

    default Optional<Instant> nextReadyAt(String queue, InstantSource clock, Duration wait)
            throws InterruptedException {
        return Optional.empty();
    }

    List<Result> settle(LeaseToken token, List<Disposition> items, InstantSource clock);

    default List<Result> settle(
            final LeaseToken token,
            final List<Disposition> items,
            final InstantSource clock,
            final Consumer<Optional<String>> deadLetter) {
        final List<Result> results;
        results = settle(token, items, clock);
        if (items.stream().anyMatch(item -> item.kind() == Disposition.Kind.REJECT)) {
            deadLetter.accept(Optional.empty());
        }
        return results;
    }

    Extended extend(LeaseToken token, List<MessageId> ids, Duration d, InstantSource clock);

    void snapshot(Path target);

    default Optional<ChangeHint> changeHint() {
        return Optional.empty();
    }

    @Override
    void close();
}
