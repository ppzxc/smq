package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.ppzxc.smq.PullWaiter.Attempt;
import io.github.ppzxc.smq.storage.ChangeHint;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

@Order(1)
class PullWaiterTest {

    private static final class RecordingHint implements ChangeHint {
        long version;
        long versionSeenAtWait;
        final List<Duration> awaits = new ArrayList<>();
        Runnable beforeVersion = () -> {};

        @Override
        public long version() {
            beforeVersion.run();
            return version;
        }

        @Override
        public void await(long seen, Duration timeout) {
            versionSeenAtWait = seen;
            awaits.add(timeout);
        }

        @Override
        public void wake() {
            version++;
        }
    }

    @Test
    void 큐별_깨움은_다른_큐에_전달되지_않고_전체_깨움은_둘_다에_전달된다() throws Exception {
        RecordingHint first = new RecordingHint();
        RecordingHint second = new RecordingHint();
        var hints = new ArrayDeque<ChangeHint>(List.of(first, second));
        PullWaiter waiter = new PullWaiter(new RecordingHint(), false, true, hints::removeFirst);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();

        String result = waiter.await(
                "first",
                deadline,
                () -> Attempt.found(waiter.await(
                        "second",
                        deadline,
                        () -> {
                            waiter.wake("first");
                            assertEquals(1, first.version());
                            assertEquals(0, second.version());
                            waiter.wake();
                            assertEquals(2, first.version());
                            assertEquals(1, second.version());
                            return Attempt.found("found");
                        },
                        () -> "empty")),
                () -> "empty");
        assertEquals("found", result);
    }

    @Test
    void 빈_꺼내기_중에_온_깨움은_대기할_때_이미_다른_버전이다() throws Exception {
        RecordingHint hint = new RecordingHint();
        PullWaiter waiter = new PullWaiter(hint, true);
        AtomicInteger calls = new AtomicInteger();

        assertEquals(
                "found",
                waiter.await(
                        Duration.ofSeconds(30),
                        () -> {
                            if (calls.getAndIncrement() == 0) {
                                waiter.wake();
                                return Attempt.empty();
                            }
                            return Attempt.found("found");
                        },
                        () -> "empty"));
        assertEquals(0, hint.versionSeenAtWait);
        assertEquals(1, hint.version());
    }

    @Test
    void 준비_시각이_폴링보다_가까우면_그_간격만_기다린다() throws Exception {
        RecordingHint hint = new RecordingHint();
        PullWaiter waiter = new PullWaiter(hint, true);
        AtomicInteger calls = new AtomicInteger();
        Duration delay = Duration.ofMillis(40);

        assertEquals(
                "found",
                waiter.await(
                        Duration.ofSeconds(30),
                        () -> calls.getAndIncrement() == 0 ? Attempt.empty(delay) : Attempt.found("found"),
                        () -> "empty"));
        assertEquals(List.of(delay), hint.awaits);
    }

    @Test
    void 버전을_읽는_사이에_닫혀도_깨움을_놓치지_않고_기다리지_않는다() throws Exception {
        RecordingHint hint = new RecordingHint();
        PullWaiter waiter = new PullWaiter(hint, true);
        AtomicInteger reads = new AtomicInteger();
        hint.beforeVersion = () -> {
            if (reads.getAndIncrement() == 0) {
                waiter.close();
            }
        };
        String result = waiter.await(Duration.ofSeconds(30), () -> Attempt.empty(), () -> "empty");
        assertEquals("empty", result);
        assertEquals(List.of(), hint.awaits);
    }

    @Test
    void 꺼내는_중에_저장소가_닫혀_예외가_나도_닫힌_상태면_빈_결과다() throws Exception {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        String result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    waiter.close();
                    throw new IllegalStateException("storage closed");
                },
                () -> "empty");
        assertEquals("empty", result);
    }

    @Test
    void 닫히지_않았을_때의_예외는_그대로_던진다() {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        assertThrows(
                IllegalStateException.class,
                () -> waiter.await(
                        Duration.ofSeconds(30),
                        () -> {
                            throw new IllegalStateException("boom");
                        },
                        () -> "empty"));
    }

    private static List<Duration> awaitsUntilFifthAttempt(PullWaiter waiter, RecordingHint hint) throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> attempts.incrementAndGet() < 6 ? Attempt.empty() : Attempt.found("got"),
                () -> "empty");
        return hint.awaits;
    }

    @Test
    void 저장소_힌트가_있으면_폴링_간격을_늘리지_않고_상한만큼_힌트를_기다린다() throws Exception {
        RecordingHint hint = new RecordingHint();
        List<Duration> awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, true), hint);
        assertEquals(
                List.of(
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250)),
                awaits);
    }

    @Test
    void 저장소_힌트가_없으면_폴링_간격이_1ms에서_250ms까지_늘어난다() throws Exception {
        RecordingHint hint = new RecordingHint();
        List<Duration> awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, false), hint);
        assertEquals(
                List.of(
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(4),
                        Duration.ofMillis(8),
                        Duration.ofMillis(16)),
                awaits);
    }

    @Test
    void 바로_다시_꺼내라는_표시가_없고_시간이_다했으면_한_번만_꺼내고_빈_결과다() throws Exception {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        AtomicInteger attempts = new AtomicInteger();
        String result = waiter.await(
                Duration.ZERO,
                () -> {
                    if (attempts.incrementAndGet() > 3) {
                        throw new AssertionError("표시가 없는데 다시 꺼냈다");
                    }
                    return Attempt.empty();
                },
                () -> "empty");
        assertEquals("empty", result);
        assertEquals(1, attempts.get());
    }

    @Test
    void 바로_다시_꺼내라는_표시가_있는_동안은_힌트를_기다리지_않고_표시가_없어지면_기다린다() throws Exception {
        RecordingHint hint = new RecordingHint();
        PullWaiter waiter = new PullWaiter(hint, true);
        AtomicInteger attempts = new AtomicInteger();
        String result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    int n = attempts.incrementAndGet();
                    if (n > 10) {
                        throw new AssertionError("표시가 없어졌는데도 다시 꺼냈다");
                    }
                    return n == 5 ? Attempt.found("got") : n < 3 ? Attempt.retryNow() : Attempt.empty();
                },
                () -> "empty");
        assertEquals("got", result);
        assertEquals(5, attempts.get());

        assertEquals(2, hint.awaits.size());
    }

    @Test
    void 바로_다시_꺼내라는_표시가_계속되어도_닫히면_멈춘다() throws Exception {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        AtomicInteger attempts = new AtomicInteger();
        String result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    int n = attempts.incrementAndGet();
                    if (n > 3) {
                        throw new AssertionError("닫힌 뒤에도 다시 꺼냈다");
                    }
                    if (n == 3) {
                        waiter.close();
                    }
                    return Attempt.retryNow();
                },
                () -> "empty");
        assertEquals("empty", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void 바로_다시_꺼내라는_표시가_계속되어도_interrupt되면_InterruptedException() throws Exception {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        AtomicInteger attempts = new AtomicInteger();
        assertThrows(
                InterruptedException.class,
                () -> waiter.await(
                        Duration.ofSeconds(30),
                        () -> {
                            int n = attempts.incrementAndGet();
                            if (n > 3) {
                                throw new AssertionError("interrupt된 뒤에도 다시 꺼냈다");
                            }
                            if (n == 3) {
                                Thread.currentThread().interrupt();
                            }
                            return Attempt.retryNow();
                        },
                        () -> "empty"));
        assertEquals(3, attempts.get());
        assertFalse(Thread.currentThread().isInterrupted());
        assertEquals("next", waiter.await(Duration.ZERO, () -> Attempt.found("next"), () -> "empty"));
    }
}
