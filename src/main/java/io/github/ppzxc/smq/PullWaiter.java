package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.ChangeHint;
import io.github.ppzxc.smq.storage.ConditionChangeHint;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * 꺼내기 대기 루프. 저장소 {@code pull}은 기다리지 않으므로 코어가 여기서 기다린다.
 * 결과가 비면 두 갈래로 기다린다.
 * <ul>
 *   <li>저장소가 변경 힌트를 내면 힌트를 기다린다. 힌트는 거짓 깨움과 누락을 허용하고 지연됨에서 준비로 바뀌는
 *       시점은 알리지 못하므로, 기다림에는 폴링 간격 상한({@link #MAX_POLL_INTERVAL_NANOS})을 둔다.</li>
 *   <li>힌트가 없으면 1ms에서 250ms까지 늘어나는 폴링 간격으로 폴링한다. 같은 프로세스 안의 변경은
 *       코어가 만든 지역 신호로 간격을 기다리는 중에도 바로 깨운다.</li>
 * </ul>
 * {@code wait}와 폴링 간격은 주입한 시계가 아니라 단조 시계로 잰다.
 */
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

        private QueueSignal(ChangeHint hint) {
            this.hint = hint;
        }

        private int users;
    }

    private final AtomicBoolean closed = new AtomicBoolean();

    PullWaiter(Optional<ChangeHint> storageHint) {
        this(storageHint, false);
    }

    PullWaiter(Optional<ChangeHint> hint, boolean scoped) {
        this(hint.orElseGet(ConditionChangeHint::new), hint.isPresent(), scoped, ConditionChangeHint::new);
    }

    PullWaiter(ChangeHint hint, boolean storageHint) {
        this(hint, storageHint, false, ConditionChangeHint::new);
    }

    PullWaiter(ChangeHint hint, boolean storageHint, boolean scoped, Supplier<ChangeHint> newQueueHint) {
        this.hint = hint;
        this.storageHint = storageHint;
        this.scoped = scoped && !storageHint;
        this.newQueueHint = newQueueHint;
    }

    /** 같은 프로세스 안의 변경을 알려 기다리던 꺼내기를 깨운다. 저장소와 상관없이 코어가 부른다. */
    void wake() {
        hint.wake();
        synchronized (queues) {
            queues.values().forEach(signal -> signal.hint.wake());
        }
    }

    void wake(String queue) {
        if (!scoped) {
            wake();
            return;
        }
        synchronized (queues) {
            QueueSignal signal = queues.get(queue);
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

    /** 닫히면 대기자는 빈 결과로 깨어난다. */
    void close() {
        closed.set(true);
        wake();
    }

    /** 한 번 꺼내 본 결과. 결과가 있거나, 비어서 기다리거나, 비었지만 바로 다시 꺼낸다. */
    sealed interface Attempt<T> {

        record Found<T>(T value) implements Attempt<T> {}

        record Empty<T>(Duration untilReady) implements Attempt<T> {}

        record RetryNow<T>() implements Attempt<T> {}

        static <T> Attempt<T> found(T value) {
            return new Found<>(value);
        }

        /** 비었다. 힌트나 폴링 간격만큼 기다린 뒤 다시 꺼낸다. */
        static <T> Attempt<T> empty() {
            return empty(Duration.ZERO);
        }

        static <T> Attempt<T> empty(Duration untilReady) {
            return new Empty<>(untilReady);
        }

        /** 비었지만 기다리지 않고 바로 다시 꺼낸다. 그 사이에도 닫힘과 interrupt는 확인한다. */
        static <T> Attempt<T> retryNow() {
            return new RetryNow<>();
        }
    }

    @FunctionalInterface
    interface InterruptibleAttempt<T> {
        Attempt<T> get() throws InterruptedException;
    }

    /**
     * @param attempt 한 번 꺼내 본다
     * @param empty 시간이 다했거나 닫혔을 때의 빈 결과
     */
    <T> T await(Duration wait, InterruptibleAttempt<T> attempt, Supplier<T> empty) throws InterruptedException {
        return await(hint, System.nanoTime() + Validation.saturatedWaitNanos(wait), attempt, empty);
    }

    <T> T await(String queue, long deadline, InterruptibleAttempt<T> attempt, Supplier<T> empty)
            throws InterruptedException {
        if (!scoped) {
            return await(hint, deadline, attempt, empty);
        }
        QueueSignal signal;
        synchronized (queues) {
            signal = queues.computeIfAbsent(queue, _ -> new QueueSignal(newQueueHint.get()));
            signal.users++;
        }
        try {
            return await(signal.hint, deadline, attempt, empty);
        } finally {
            synchronized (queues) {
                if (--signal.users == 0) {
                    queues.remove(queue);
                }
            }
        }
    }

    private <T> T await(ChangeHint hint, long deadline, InterruptibleAttempt<T> attempt, Supplier<T> empty)
            throws InterruptedException {
        long pollInterval = storageHint ? MAX_POLL_INTERVAL_NANOS : MIN_POLL_INTERVAL_NANOS;
        while (true) {

            long seen = hint.version();
            if (closed.get()) {
                return empty.get();
            }
            Attempt<T> result;
            try {
                result = attempt.get();
            } catch (RuntimeException e) {

                if (closed.get()) {
                    return empty.get();
                }
                throw e;
            }
            if (result instanceof Attempt.Found<T> found) {
                return found.value();
            }
            if (result instanceof Attempt.RetryNow<T>) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                continue;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return empty.get();
            }
            Duration pause = Duration.ofNanos(Math.min(pollInterval, remaining));
            if (result instanceof Attempt.Empty<T> pending
                    && pending.untilReady().isPositive()
                    && pending.untilReady().compareTo(pause) < 0) {
                pause = pending.untilReady();
            }
            hint.await(seen, pause);
            if (!storageHint) {
                pollInterval = Math.min(pollInterval * 2, MAX_POLL_INTERVAL_NANOS);
            }
        }
    }
}
