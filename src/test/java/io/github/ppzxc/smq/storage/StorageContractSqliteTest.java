package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.QueueOptions;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
@Order(1)
class StorageContractSqliteTest extends StorageContractTest {

    @Parameter
    Durability durability;

    @Test
    @DisplayName("꺼내기 쓰기 트랜잭션은 시계를 다시 읽는다 — check 1")
    void pullWriteTransactionReadsTheClockAgain() {
        final AtomicInteger reads;
        final PullResult result;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        reads = new AtomicInteger();
        result = storage.pull("q", 1, LEASE, () -> reads.getAndIncrement() == 0 ? T0 : T0.plusSeconds(60));
        final var assertionValue2Argument2 = reads.get();
        assertEquals(2, assertionValue2Argument2, "Reads should match the expected value.");
        final var assertionValue1Argument1 = T0.plusSeconds(70);
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.leaseDeadline();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("꺼내기 쓰기 트랜잭션은 시계를 다시 읽는다 — check 2")
    void pullWriteTransactionReadsTheClockAgainCheck2() {
        final AtomicInteger reads;
        final PullResult result;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        reads = new AtomicInteger();
        result = storage.pull("q", 1, LEASE, () -> reads.getAndIncrement() == 0 ? T0 : T0.plusSeconds(60));
        final var assertionValue2Argument2 = reads.get();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = T0.plusSeconds(70);
        final var assertionValue1Argument2 = result.leaseDeadline();
        assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Lease deadline should match the expected value.");
    }

    @Override
    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 1")
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeased() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            assertTrue(assertionValue7Argument1, "The operation should complete before the timeout.");
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 2")
    @Override
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                assertEquals(
                        Thread.State.TIMED_WAITING, assertionValue6Argument2, "State should match the expected value.");
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 3")
    @Override
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                assertFalse(assertionValue5Argument1, "Puller should not be alive.");
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 4")
    @Override
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck4() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                assertInstanceOf(
                        InterruptedException.class,
                        assertionValue4,
                        "Value should be an instance of InterruptedException.");
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 5")
    @Override
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck5() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                assertTrue(assertionValue3Argument1, "The original lock wait should appear in the stack trace.");
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 6")
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck6() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        assertEquals(0, assertionValue2Argument2, "Leased should match the expected value.");
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidThrowingRawExceptionTypes"})
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 7")
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck7() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread puller;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue7Argument1 = locked.await(5, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue7Argument1);
            puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final InterruptedException interrupted;
                puller.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue6Argument2 = puller.getState();
                evaluatePmdArgument(assertionValue6Argument2);
                puller.interrupt();
                puller.join(5000);
                final var assertionValue5Argument1 = puller.isAlive();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4 = failure.get();
                interrupted = (InterruptedException) assertionValue4;
                final var assertionValue3Argument1 = java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue3Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        final var assertionValue2Argument2 = storage.counts("q").leased();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 =
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size();
        assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각을 lock 대기 중 interrupt하면 예외를 전달한다 — check 1")
    void nextReadyAtPropagatesInterruptionWhileWaitingForLock() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<InterruptedException> failure;
        storage.declare("q", QueueOptions.defaults());
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread reader;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final var assertionValue5Argument1 = locked.await(5, TimeUnit.SECONDS);
                assertTrue(assertionValue5Argument1, "The operation should complete before the timeout.");
                reader.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue4Argument2 = reader.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                reader.interrupt();
                reader.join(5000);
                final var assertionValue3Argument1 = reader.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument1 = failure.get();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = java.util.Arrays.stream(
                                failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                reader.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각을 lock 대기 중 interrupt하면 예외를 전달한다 — check 2")
    void nextReadyAtPropagatesInterruptionWhileWaitingForLockCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<InterruptedException> failure;
        storage.declare("q", QueueOptions.defaults());
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread reader;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final var assertionValue5Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue5Argument1);
                reader.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue4Argument2 = reader.getState();
                assertEquals(
                        Thread.State.TIMED_WAITING, assertionValue4Argument2, "State should match the expected value.");
                reader.interrupt();
                reader.join(5000);
                final var assertionValue3Argument1 = reader.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument1 = failure.get();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = java.util.Arrays.stream(
                                failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                reader.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각을 lock 대기 중 interrupt하면 예외를 전달한다 — check 3")
    void nextReadyAtPropagatesInterruptionWhileWaitingForLockCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<InterruptedException> failure;
        storage.declare("q", QueueOptions.defaults());
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread reader;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final var assertionValue5Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue5Argument1);
                reader.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue4Argument2 = reader.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                reader.interrupt();
                reader.join(5000);
                final var assertionValue3Argument1 = reader.isAlive();
                assertFalse(assertionValue3Argument1, "Reader should not be alive.");
                final var assertionValue2Argument1 = failure.get();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = java.util.Arrays.stream(
                                failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                reader.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각을 lock 대기 중 interrupt하면 예외를 전달한다 — check 4")
    void nextReadyAtPropagatesInterruptionWhileWaitingForLockCheck4() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<InterruptedException> failure;
        storage.declare("q", QueueOptions.defaults());
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread reader;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final var assertionValue5Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue5Argument1);
                reader.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue4Argument2 = reader.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                reader.interrupt();
                reader.join(5000);
                final var assertionValue3Argument1 = reader.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument1 = failure.get();
                assertNotNull(assertionValue2Argument1, "Failure should be present.");
                final var assertionValue1Argument1 = java.util.Arrays.stream(
                                failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                reader.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각을 lock 대기 중 interrupt하면 예외를 전달한다 — check 5")
    void nextReadyAtPropagatesInterruptionWhileWaitingForLockCheck5() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<InterruptedException> failure;
        storage.declare("q", QueueOptions.defaults());
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread reader;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                final var assertionValue5Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue5Argument1);
                reader.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                final var assertionValue4Argument2 = reader.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                reader.interrupt();
                reader.join(5000);
                final var assertionValue3Argument1 = reader.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument1 = failure.get();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = java.util.Arrays.stream(
                                failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks."));
                assertTrue(assertionValue1Argument1, "The original lock wait should appear in the stack trace.");
            } finally {
                reader.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각은 lock 대기 마감이 끝나면 비어있다 — check 1")
    void nextReadyAtReturnsEmptyWhenLockWaitExpires() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        final var assertionValue3Argument1 = storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(1))
                .isPresent();
        assertTrue(assertionValue3Argument1, "Next ready at should be present.");
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            try {
                final var assertionValue2Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = storage.nextReadyAt(
                                "q", InstantSource.fixed(T0), Duration.ofMillis(50))
                        .isEmpty();
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각은 lock 대기 마감이 끝나면 비어있다 — check 2")
    void nextReadyAtReturnsEmptyWhenLockWaitExpiresCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        final var assertionValue3Argument1 = storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(1))
                .isPresent();
        evaluatePmdArgument(assertionValue3Argument1);
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            try {
                final var assertionValue2Argument1 = locked.await(5, TimeUnit.SECONDS);
                assertTrue(assertionValue2Argument1, "The operation should complete before the timeout.");
                final var assertionValue1Argument1 = storage.nextReadyAt(
                                "q", InstantSource.fixed(T0), Duration.ofMillis(50))
                        .isEmpty();
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("다음 준비 시각은 lock 대기 마감이 끝나면 비어있다 — check 3")
    void nextReadyAtReturnsEmptyWhenLockWaitExpiresCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        final var assertionValue3Argument1 = storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(1))
                .isPresent();
        evaluatePmdArgument(assertionValue3Argument1);
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            try {
                final var assertionValue2Argument1 = locked.await(5, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue1Argument1 = storage.nextReadyAt(
                                "q", InstantSource.fixed(T0), Duration.ofMillis(50))
                        .isEmpty();
                assertTrue(assertionValue1Argument1, "Next ready at should be empty.");
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("리스하는 동안 interrupt되어도 꺼낸 결과를 돌려준다 — check 1")
    void evenIfInterruptedDuringTheLeaseTheRetrievedResultIsReturned() throws Exception {
        final AtomicInteger reads;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        reads = new AtomicInteger();
        try {
            final PullResult result;
            result = storage.pull(
                    "q",
                    1,
                    LEASE,
                    () -> {
                        if (reads.incrementAndGet() == 2) Thread.currentThread().interrupt();
                        return T0;
                    },
                    Duration.ofSeconds(30));
            final var assertionValue2Argument2 = result.messages().size();
            assertEquals(1, assertionValue2Argument2, "Size should match the expected value.");
            final var assertionValue1Argument1 = Thread.currentThread().isInterrupted();
            evaluatePmdArgument(assertionValue1Argument1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("리스하는 동안 interrupt되어도 꺼낸 결과를 돌려준다 — check 2")
    void evenIfInterruptedDuringTheLeaseTheRetrievedResultIsReturnedCheck2() throws Exception {
        final AtomicInteger reads;
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        reads = new AtomicInteger();
        try {
            final PullResult result;
            result = storage.pull(
                    "q",
                    1,
                    LEASE,
                    () -> {
                        if (reads.incrementAndGet() == 2) Thread.currentThread().interrupt();
                        return T0;
                    },
                    Duration.ofSeconds(30));
            final var assertionValue2Argument2 = result.messages().size();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = Thread.currentThread().isInterrupted();
            assertTrue(assertionValue1Argument1, "Current thread should be interrupted.");
        } finally {
            Thread.interrupted();
        }
    }

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteNew(durability);
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
