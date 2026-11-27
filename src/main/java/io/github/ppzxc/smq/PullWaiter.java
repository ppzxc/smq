package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.ChangeHint;
import io.github.ppzxc.smq.storage.ConditionChangeHint;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class PullWaiter {

    private static final long MIN_POLL_INTERVAL_NANOS = Duration.ofMillis(1).toNanos();
    private static final long MAX_POLL_INTERVAL_NANOS = Duration.ofMillis(250).toNanos();

    private final ChangeHint hint;
    private final boolean storageHint;
    private final boolean scoped;
    private final Supplier<ChangeHint> newQueueHint;
    private final Map<String, QueueSignal> queues = new HashMap<>();

    private static final class QueueSignal {
        private final ChangeHint hint;

        private QueueSignal(final ChangeHint hint) {
            this.hint = hint;
        }

        private int users;
    }

    private final AtomicBoolean closed = new AtomicBoolean();

    PullWaiter(final Optional<ChangeHint> storageHint) {
        this(storageHint, false);
    }

    PullWaiter(final Optional<ChangeHint> hint, final boolean scoped) {
        this(hint.orElseGet(ConditionChangeHint::new), hint.isPresent(), scoped, ConditionChangeHint::new);
    }

    PullWaiter(final ChangeHint hint, final boolean storageHint) {
        this(hint, storageHint, false, ConditionChangeHint::new);
    }

    PullWaiter(
            final ChangeHint hint,
            final boolean storageHint,
            final boolean scoped,
            final Supplier<ChangeHint> newQueueHint) {
        this.hint = hint;
        this.storageHint = storageHint;
        this.scoped = scoped && !storageHint;
        this.newQueueHint = newQueueHint;
    }

    void wake() {
        hint.wake();
        synchronized (queues) {
            queues.values().forEach(signal -> signal.hint.wake());
        }
    }

    void wake(final String queue) {
        if (!scoped) {
            wake();
            return;
        }
        synchronized (queues) {
            final QueueSignal signal;
            signal = queues.get(queue);
            if (signal != null) {
                signal.hint.wake();
            }
        }
    }

    void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Smq is closed");
        }
    }

    void close() {
        closed.set(true);
        wake();
    }

    sealed interface Attempt<T> {

        record Found<T>(T value) implements Attempt<T> {}

        record Empty<T>(Duration untilReady) implements Attempt<T> {}

        record RetryNow<T>() implements Attempt<T> {}

        static <T> Attempt<T> found(final T value) {
            return new Found<>(value);
        }

        static <T> Attempt<T> empty() {
            return empty(Duration.ZERO);
        }

        static <T> Attempt<T> empty(final Duration untilReady) {
            return new Empty<>(untilReady);
        }

        static <T> Attempt<T> retryNow() {
            return new RetryNow<>();
        }
    }

    @FunctionalInterface
    interface InterruptibleAttempt<T> {
        Attempt<T> get() throws InterruptedException;
    }

    <T> T await(final Duration wait, final InterruptibleAttempt<T> attempt, final Supplier<T> empty)
            throws InterruptedException {
        return await(hint, System.nanoTime() + Validation.saturatedWaitNanos(wait), attempt, empty);
    }

    <T> T await(final String queue, final long deadline, final InterruptibleAttempt<T> attempt, final Supplier<T> empty)
            throws InterruptedException {
        final QueueSignal signal;
        if (!scoped) {
            return await(hint, deadline, attempt, empty);
        }

        synchronized (queues) {
            signal = queues.computeIfAbsent(queue, _ -> new QueueSignal(newQueueHint.get()));
            signal.users++;
        }
        try {
            return await(signal.hint, deadline, attempt, empty);
        } finally {
            synchronized (queues) {
                signal.users--;
                if (signal.users == 0) {
                    queues.remove(queue);
                }
            }
        }
    }

    private <T> T await(
            final ChangeHint hint, final long deadline, final InterruptibleAttempt<T> attempt, final Supplier<T> empty)
            throws InterruptedException {
        long pollInterval;
        pollInterval = storageHint ? MAX_POLL_INTERVAL_NANOS : MIN_POLL_INTERVAL_NANOS;
        while (true) {

            final long seen;
            final Optional<Attempt<T>> attempted;
            final Attempt<T> result;
            final long remaining;
            final Duration pause;
            seen = hint.version();
            if (closed.get()) {
                return empty.get();
            }
            attempted = attemptUnlessClosed(attempt);
            if (attempted.isEmpty()) {
                return empty.get();
            }
            result = attempted.orElseThrow();
            if (result instanceof Attempt.Found<T> found) {
                return found.value();
            }
            if (result instanceof Attempt.RetryNow<T>) {
                checkInterrupted();
                continue;
            }
            remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return empty.get();
            }
            pause = pauseFor(result, pollInterval, remaining);
            hint.await(seen, pause);
            if (!storageHint) {
                pollInterval = Math.min(pollInterval * 2, MAX_POLL_INTERVAL_NANOS);
            }
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    private static Duration pauseFor(final Attempt<?> result, final long pollInterval, final long remaining) {
        final Duration pause;
        pause = Duration.ofNanos(Math.min(pollInterval, remaining));
        if (result instanceof Attempt.Empty<?> pending
                && pending.untilReady().isPositive()
                && pending.untilReady().compareTo(pause) < 0) {
            return pending.untilReady();
        }
        return pause;
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "Any storage runtime failure becomes an empty result if closure raced with the attempt.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private <T> Optional<Attempt<T>> attemptUnlessClosed(final InterruptibleAttempt<T> attempt)
            throws InterruptedException {
        try {
            return Optional.of(attempt.get());
        } catch (RuntimeException e) {
            if (closed.get()) {
                return Optional.empty();
            }
            throw e;
        }
    }
}
