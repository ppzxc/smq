package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ppzxc.smq.PullWaiter.Attempt;
import io.github.ppzxc.smq.storage.ChangeHint;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
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
        public void await(final long seen, final Duration timeout) {
            versionSeenAtWait = seen;
            awaits.add(timeout);
        }

        @Override
        public void wake() {
            version++;
        }
    }

    @Test
    @DisplayName("큐별 깨움은 다른 큐에 전달되지 않고 전체 깨움은 둘 다에 전달된다")
    void queueSpecificWakeupIsNotPropagatedToOtherQueuesWhileFullWakeupIsPropagatedToBoth() throws Exception {
        final RecordingHint first;
        final RecordingHint second;
        final PullWaiter waiter;
        final long deadline;
        final List<Long> wakeVersions;
        final String result;
        first = new RecordingHint();
        second = new RecordingHint();
        final var hints = new ArrayDeque<ChangeHint>(List.of(first, second));
        waiter = new PullWaiter(new RecordingHint(), false, true, hints::removeFirst);
        deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        wakeVersions = new ArrayList<>();

        result = waiter.await(
                "first",
                deadline,
                () -> Attempt.found(waiter.await(
                        "second",
                        deadline,
                        () -> {
                            waiter.wake("first");
                            wakeVersions.add(first.version());
                            wakeVersions.add(second.version());
                            waiter.wake();
                            wakeVersions.add(first.version());
                            wakeVersions.add(second.version());
                            return Attempt.found("found");
                        },
                        () -> "empty")),
                () -> "empty");
        assertEquals(
                List.of("found", List.of(1L, 0L, 2L, 1L)),
                List.of(result, wakeVersions),
                "Wakeups should update only the intended queue versions.");
    }

    @Test
    @DisplayName("빈 꺼내기 중에 온 깨움은 대기할 때 이미 다른 버전이다 — found result")
    void wakeupDuringPullReturnsFoundResult() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger calls;
        final String result;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        calls = new AtomicInteger();
        result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    if (calls.getAndIncrement() == 0) {
                        waiter.wake();
                        return Attempt.empty();
                    }
                    return Attempt.found("found");
                },
                () -> "empty");
        assertEquals("found", result, "The wait should return the delivered result.");
    }

    @Test
    @DisplayName("빈 꺼내기 중에 온 깨움은 대기할 때 이미 다른 버전이다 — observed version")
    void wakeupDuringPullIsSeenByWait() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger calls;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        calls = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    if (calls.getAndIncrement() == 0) {
                        waiter.wake();
                        return Attempt.empty();
                    }
                    return Attempt.found("found");
                },
                () -> "empty");
        assertEquals(0, hint.versionSeenAtWait, "Version seen at wait should match the expected value.");
    }

    @Test
    @DisplayName("빈 꺼내기 중에 온 깨움은 대기할 때 이미 다른 버전이다 — new version")
    void wakeupDuringPullChangesVersion() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger calls;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        calls = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    if (calls.getAndIncrement() == 0) {
                        waiter.wake();
                        return Attempt.empty();
                    }
                    return Attempt.found("found");
                },
                () -> "empty");
        assertEquals(1, hint.version(), "Version should match the expected value.");
    }

    @Test
    @DisplayName("준비 시각이 폴링보다 가까우면 그 간격만 기다린다 — found result")
    void readyTimeWaitReturnsFoundResult() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger calls;
        final Duration delay;
        final String result;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        calls = new AtomicInteger();
        delay = Duration.ofMillis(40);
        result = waiter.await(
                Duration.ofSeconds(30),
                () -> calls.getAndIncrement() == 0 ? Attempt.empty(delay) : Attempt.found("found"),
                () -> "empty");
        assertEquals("found", result, "The wait should return the delivered result.");
    }

    @Test
    @DisplayName("준비 시각이 폴링보다 가까우면 그 간격만 기다린다 — wait duration")
    void readyTimeUsesShorterWait() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger calls;
        final Duration delay;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        calls = new AtomicInteger();
        delay = Duration.ofMillis(40);
        waiter.await(
                Duration.ofSeconds(30),
                () -> calls.getAndIncrement() == 0 ? Attempt.empty(delay) : Attempt.found("found"),
                () -> "empty");
        assertEquals(List.of(delay), hint.awaits, "Awaits should match the expected value.");
    }

    @Test
    @DisplayName("버전을 읽는 사이에 닫혀도 깨움을 놓치지 않고 기다리지 않는다 — empty result")
    void closeDuringVersionReadReturnsEmpty() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger reads;
        final String result;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        reads = new AtomicInteger();
        hint.beforeVersion = () -> {
            if (reads.getAndIncrement() == 0) {
                waiter.close();
            }
        };
        result = waiter.await(Duration.ofSeconds(30), () -> Attempt.empty(), () -> "empty");
        assertEquals("empty", result, "Result should match the expected value.");
    }

    @Test
    @DisplayName("버전을 읽는 사이에 닫혀도 깨움을 놓치지 않고 기다리지 않는다 — no wait")
    void closeDuringVersionReadDoesNotWait() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger reads;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        reads = new AtomicInteger();
        hint.beforeVersion = () -> {
            if (reads.getAndIncrement() == 0) {
                waiter.close();
            }
        };
        waiter.await(Duration.ofSeconds(30), () -> Attempt.empty(), () -> "empty");
        assertEquals(List.of(), hint.awaits, "Awaits should match the expected value.");
    }

    @Test
    @DisplayName("꺼내는 중에 저장소가 닫혀 예외가 나도 닫힌 상태면 빈 결과다")
    void ifTheRepositoryIsClosedDuringRetrievalAndAnExceptionOccursTheResultIsEmptyIfItIsStillClosed()
            throws Exception {
        final PullWaiter waiter;
        final String result;
        waiter = new PullWaiter(new RecordingHint(), true);
        result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    waiter.close();
                    throw new IllegalStateException("storage closed");
                },
                () -> "empty");
        assertEquals("empty", result, "Result should match the expected value.");
    }

    @Test
    @DisplayName("닫히지 않았을 때의 예외는 그대로 던진다")
    void exceptionsThatOccurWhenItIsNotClosedAreThrownAsIs() {
        final PullWaiter waiter;
        waiter = new PullWaiter(new RecordingHint(), true);
        assertThrows(
                IllegalStateException.class,
                () -> waiter.await(
                        Duration.ofSeconds(30),
                        () -> {
                            throw new IllegalStateException("boom");
                        },
                        () -> "empty"),
                "Await should throw IllegalStateException.");
    }

    private static List<Duration> awaitsUntilFifthAttempt(final PullWaiter waiter, final RecordingHint hint)
            throws Exception {
        final AtomicInteger attempts;
        attempts = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> attempts.incrementAndGet() < 6 ? Attempt.empty() : Attempt.found("got"),
                () -> "empty");
        return hint.awaits;
    }

    @Test
    @DisplayName("저장소 힌트가 있으면 폴링 간격을 늘리지 않고 상한만큼 힌트를 기다린다")
    void ifThereIsAStorageHintThePollingIntervalIsNotIncreasedAndTheHintIsWaitedForUpToTheUpperLimit()
            throws Exception {
        final RecordingHint hint;
        final List<Duration> awaits;
        hint = new RecordingHint();
        awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, true), hint);
        assertEquals(
                List.of(
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofMillis(250)),
                awaits,
                "Awaits should match the expected value.");
    }

    @Test
    @DisplayName("저장소 힌트가 없으면 폴링 간격이 1ms에서 250ms까지 늘어난다")
    void ifThereIsNoStorageHintThePollingIntervalIncreasesFrom1msTo250ms() throws Exception {
        final RecordingHint hint;
        final List<Duration> awaits;
        hint = new RecordingHint();
        awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, false), hint);
        assertEquals(
                List.of(
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(4),
                        Duration.ofMillis(8),
                        Duration.ofMillis(16)),
                awaits,
                "Awaits should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 없고 시간이 다했으면 한 번만 꺼내고 빈 결과다 — empty result")
    void expiredWaitWithoutRetryReturnsEmpty() throws Exception {
        final PullWaiter waiter;
        final AtomicInteger attempts;
        final String result;
        waiter = new PullWaiter(new RecordingHint(), true);
        attempts = new AtomicInteger();
        result = waiter.await(
                Duration.ZERO,
                () -> {
                    if (attempts.incrementAndGet() > 3) {
                        throw new AssertionError("표시가 없는데 다시 꺼냈다");
                    }
                    return Attempt.empty();
                },
                () -> "empty");
        assertEquals("empty", result, "Result should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 없고 시간이 다했으면 한 번만 꺼내고 빈 결과다 — single pull")
    void expiredWaitWithoutRetryPullsOnce() throws Exception {
        final PullWaiter waiter;
        final AtomicInteger attempts;
        waiter = new PullWaiter(new RecordingHint(), true);
        attempts = new AtomicInteger();
        waiter.await(
                Duration.ZERO,
                () -> {
                    if (attempts.incrementAndGet() > 3) {
                        throw new AssertionError("표시가 없는데 다시 꺼냈다");
                    }
                    return Attempt.empty();
                },
                () -> "empty");
        assertEquals(1, attempts.get(), "Attempts should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 있는 동안은 힌트를 기다리지 않고 표시가 없어지면 기다린다 — found result")
    void retryThenWaitReturnsFoundResult() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger attempts;
        final String result;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        attempts = new AtomicInteger();
        result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    final int n;
                    n = attempts.incrementAndGet();
                    if (n > 10) {
                        throw new AssertionError("표시가 없어졌는데도 다시 꺼냈다");
                    }
                    return n == 5 ? Attempt.found("got") : n < 3 ? Attempt.retryNow() : Attempt.empty();
                },
                () -> "empty");
        assertEquals("got", result, "Result should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 있는 동안은 힌트를 기다리지 않고 표시가 없어지면 기다린다 — pull count")
    void retryThenWaitPullsUntilFound() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger attempts;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        attempts = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    final int n;
                    n = attempts.incrementAndGet();
                    if (n > 10) {
                        throw new AssertionError("표시가 없어졌는데도 다시 꺼냈다");
                    }
                    return n == 5 ? Attempt.found("got") : n < 3 ? Attempt.retryNow() : Attempt.empty();
                },
                () -> "empty");
        assertEquals(5, attempts.get(), "Attempts should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 있는 동안은 힌트를 기다리지 않고 표시가 없어지면 기다린다 — wait count")
    void retryThenWaitAwaitsOnlyAfterRetryClears() throws Exception {
        final RecordingHint hint;
        final PullWaiter waiter;
        final AtomicInteger attempts;
        hint = new RecordingHint();
        waiter = new PullWaiter(hint, true);
        attempts = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    final int n;
                    n = attempts.incrementAndGet();
                    if (n > 10) {
                        throw new AssertionError("표시가 없어졌는데도 다시 꺼냈다");
                    }
                    return n == 5 ? Attempt.found("got") : n < 3 ? Attempt.retryNow() : Attempt.empty();
                },
                () -> "empty");
        assertEquals(2, hint.awaits.size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 계속되어도 닫히면 멈춘다 — empty result")
    void closeDuringRetriesReturnsEmpty() throws Exception {
        final PullWaiter waiter;
        final AtomicInteger attempts;
        final String result;
        waiter = new PullWaiter(new RecordingHint(), true);
        attempts = new AtomicInteger();
        result = waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    final int n;
                    n = attempts.incrementAndGet();
                    if (n > 3) {
                        throw new AssertionError("닫힌 뒤에도 다시 꺼냈다");
                    }
                    if (n == 3) {
                        waiter.close();
                    }
                    return Attempt.retryNow();
                },
                () -> "empty");
        assertEquals("empty", result, "Result should match the expected value.");
    }

    @Test
    @DisplayName("바로 다시 꺼내라는 표시가 계속되어도 닫히면 멈춘다 — pull count")
    void closeDuringRetriesStopsFurtherPulls() throws Exception {
        final PullWaiter waiter;
        final AtomicInteger attempts;
        waiter = new PullWaiter(new RecordingHint(), true);
        attempts = new AtomicInteger();
        waiter.await(
                Duration.ofSeconds(30),
                () -> {
                    final int n;
                    n = attempts.incrementAndGet();
                    if (n > 3) {
                        throw new AssertionError("닫힌 뒤에도 다시 꺼냈다");
                    }
                    if (n == 3) {
                        waiter.close();
                    }
                    return Attempt.retryNow();
                },
                () -> "empty");
        assertEquals(3, attempts.get(), "Attempts should match the expected value.");
    }

    private record InterruptedRetry(boolean thrown, int attempts, boolean interrupted, String next) {}

    private static InterruptedRetry runInterruptedRetries() throws InterruptedException {
        final PullWaiter waiter;
        final AtomicInteger attempts;
        boolean thrown;
        final boolean interrupted;
        final String next;
        waiter = new PullWaiter(new RecordingHint(), true);
        attempts = new AtomicInteger();
        thrown = false;
        try {
            waiter.await(
                    Duration.ofSeconds(30),
                    () -> {
                        final int n;
                        n = attempts.incrementAndGet();
                        if (n > 3) {
                            throw new AssertionError("interrupt된 뒤에도 다시 꺼냈다");
                        }
                        if (n == 3) {
                            Thread.currentThread().interrupt();
                        }
                        return Attempt.retryNow();
                    },
                    () -> "empty");
        } catch (InterruptedException expected) {
            thrown = true;
        }
        interrupted = Thread.interrupted();
        next = waiter.await(Duration.ZERO, () -> Attempt.found("next"), () -> "empty");
        return new InterruptedRetry(thrown, attempts.get(), interrupted, next);
    }

    @Test
    @DisplayName("반복 꺼내기 중 interrupt하면 InterruptedException이 발생한다")
    void interruptDuringRetriesThrows() throws Exception {
        assertTrue(runInterruptedRetries().thrown(), "Interrupted retries should throw InterruptedException.");
    }

    @Test
    @DisplayName("interrupt된 반복 꺼내기는 세 번 실행한다")
    void interruptDuringRetriesStopsAfterThirdAttempt() throws Exception {
        assertEquals(3, runInterruptedRetries().attempts(), "Retries should stop after the third attempt.");
    }

    @Test
    @DisplayName("반복 꺼내기의 interrupt 예외는 상태를 지운다")
    void interruptDuringRetriesClearsInterruptStatus() throws Exception {
        assertFalse(runInterruptedRetries().interrupted(), "The interrupt status should be cleared.");
    }

    @Test
    @DisplayName("반복 꺼내기에서 interrupt된 뒤 다음 꺼내기는 성공한다")
    void pullSucceedsAfterInterruptDuringRetries() throws Exception {
        assertEquals("next", runInterruptedRetries().next(), "The next pull should return its result.");
    }
}
