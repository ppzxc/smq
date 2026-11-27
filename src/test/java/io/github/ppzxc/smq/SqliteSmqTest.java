package io.github.ppzxc.smq;

import io.github.ppzxc.smq.Smq.SqliteBuilder;
import io.github.ppzxc.smq.storage.StorageFactory;
import java.lang.Thread.State;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass
@EnumSource(Durability.class)
class SqliteSmqTest extends SmqConformanceTest {

    @Parameter
    Durability durability;

    private final List<Smq> direct = new ArrayList<>();

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteNew(this.durability);
    }

    @AfterEach
    void closeDirect() {
        this.direct.forEach(Smq::close);
    }

    private SqliteBuilder builder() {
        return Smq.sqlite(this.dir.resolve("smq.db"));
    }

    @Test
    @DisplayName("이름이 memory인 경로도 파일에 저장한다")
    void pathsNamedMemoryAreAlsoSavedToTheFile() throws Exception {
        final Path path;
        Assumptions.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        path = this.dir.resolve(":memory:");
        try (Smq smq = Smq.sqlite(path).durability(this.durability).open()) {
            smq.declare("q", QueueOptions.defaults()).publish(msg("saved"));
        }
        try (Smq smq = Smq.sqlite(path).durability(this.durability).open()) {
            Assertions.assertEquals(
                    "saved",
                    text(smq.queue("q").pull(1, Duration.ZERO).messages().getFirst()),
                    "Text should match the expected value.");
        }
    }

    @Test
    @DisplayName("같은 SQLite 파일을 다시 열면 이전 세션의 청크를 거부한다 — check 1")
    void reopeningTheSameSQLiteFileRejectsChunksFromThePreviousSessionCheck1() throws Exception {
        final Settlement oldSettlement;
        final Chunk oldChunk;
        try (Smq first = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue original;
            original = first.declare("q", QueueOptions.defaults());
            original.publish(msg("a"));
            oldChunk = original.pull(1, Duration.ZERO);
            oldSettlement = oldChunk.ackAll();
        }
        try (Smq reopened = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue queue;
            final Throwable assertionValue1;
            queue = reopened.queue("q");
            assertionValue1 = captureThrowable(() -> queue.settle(oldSettlement));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue1, "Settle should throw IllegalArgumentException.");
            evaluate(captureThrowable(() -> queue.extend(oldChunk, Duration.ofSeconds(20L))));
            evaluate(new QueueCounts(0L, 1L, 0L));
        }
    }

    @Test
    @DisplayName("같은 SQLite 파일을 다시 열면 이전 세션의 청크를 거부한다 — check 2")
    void reopeningTheSameSQLiteFileRejectsChunksFromThePreviousSessionCheck2() throws Exception {
        final Settlement oldSettlement;
        final Chunk oldChunk;
        try (Smq first = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue original;
            original = first.declare("q", QueueOptions.defaults());
            original.publish(msg("a"));
            oldChunk = original.pull(1, Duration.ZERO);
            oldSettlement = oldChunk.ackAll();
        }
        try (Smq reopened = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue queue;
            final Throwable assertionValue2;
            queue = reopened.queue("q");
            evaluate(captureThrowable(() -> queue.settle(oldSettlement)));
            assertionValue2 = captureThrowable(() -> queue.extend(oldChunk, Duration.ofSeconds(20L)));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue2, "Extend should throw IllegalArgumentException.");
            evaluate(new QueueCounts(0L, 1L, 0L));
        }
    }

    @Test
    @DisplayName("같은 SQLite 파일을 다시 열면 이전 세션의 청크를 거부한다 — check 3")
    void reopeningTheSameSQLiteFileRejectsChunksFromThePreviousSessionCheck3() throws Exception {
        final Settlement oldSettlement;
        final Chunk oldChunk;
        try (Smq first = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue original;
            original = first.declare("q", QueueOptions.defaults());
            original.publish(msg("a"));
            oldChunk = original.pull(1, Duration.ZERO);
            oldSettlement = oldChunk.ackAll();
        }
        try (Smq reopened = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofSeconds(10L))
                .open()) {
            final Queue queue;
            final QueueCounts assertionValue3Argument1;
            final QueueCounts assertionValue3Argument2;
            queue = reopened.queue("q");
            evaluate(captureThrowable(() -> queue.settle(oldSettlement)));
            evaluate(captureThrowable(() -> queue.extend(oldChunk, Duration.ofSeconds(20L))));
            assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
            assertionValue3Argument2 = queue.counts();
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 1")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyState() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                Assertions.assertTrue(assertionValue7Argument1, "The operation should complete before the timeout.");
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 2")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    Assertions.assertEquals(
                            State.TIMED_WAITING, assertionValue6Argument2, "State should match the expected value.");
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 3")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    Assertions.assertFalse(assertionValue5Argument1, "Puller should not be alive.");
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 4")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck4() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    Assertions.assertInstanceOf(
                            InterruptedException.class,
                            assertionValue4Argument2,
                            "Value should be an instance of InterruptedException.");
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 5")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck5() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    Assertions.assertFalse(assertionValue3Argument1, "The worker should not be interrupted.");
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 6")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck6() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            final var assertionValue2Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 메시지는 준비 상태로 남는다 — check 7")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.AvoidThrowingRawExceptionTypes",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.AvoidThrowingRawExceptionTypes", "PMD.AvoidCatchingGenericException"})
    void ifARetrievalOperationWaitingForALockIsInterruptedTheMessageRemainsInTheReadyStateCheck7() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicBoolean hold;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        hold = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (hold.get()) {
                        locked.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            final AtomicReference<Throwable> failure;
            final AtomicBoolean interrupted;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            hold.set(true);
            failure = new AtomicReference<>();
            interrupted = new AtomicBoolean(true);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                final Future<?> holder;
                final Thread puller;
                holder = executor.submit(() -> q.publish(msg("b")));
                final var assertionValue7Argument1 = locked.await(5L, TimeUnit.SECONDS);
                evaluatePmdArgument(assertionValue7Argument1);
                puller = new Thread(() -> {
                    try {
                        q.pull(1, Duration.ofSeconds(30L));
                    } catch (Throwable e) {
                        failure.set(e);
                        interrupted.set(Thread.currentThread().isInterrupted());
                    }
                });
                try {
                    final long deadline;
                    puller.start();
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                    while (puller.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument2 = puller.getState();
                    evaluatePmdArgument(assertionValue6Argument2);
                    puller.interrupt();
                    puller.join(5000L);
                    final var assertionValue5Argument1 = puller.isAlive();
                    evaluatePmdArgument(assertionValue5Argument1);
                    final var assertionValue4Argument2 = failure.get();
                    evaluatePmdArgument(assertionValue4Argument2);
                    final var assertionValue3Argument1 = interrupted.get();
                    evaluatePmdArgument(assertionValue3Argument1);
                } finally {
                    hold.set(false);
                    release.countDown();
                }
                holder.get(5L, TimeUnit.SECONDS);
            }
            final var assertionValue2Argument1 = new QueueCounts(2L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 =
                    q.pull(2, Duration.ZERO).messages().size();
            Assertions.assertEquals(2, assertionValue1Argument2, "Size should match the expected value.");
        }
    }

    @Test
    @DisplayName("리스하는 동안 interrupt되어도 청크를 돌려준다 — check 1")
    void evenIfInterruptedDuringTheLeaseTheChunkIsReturned() throws Exception {
        final AtomicInteger reads;
        final AtomicBoolean pulling;
        reads = new AtomicInteger();
        pulling = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (pulling.get() && reads.incrementAndGet() == 2) {
                        Thread.currentThread().interrupt();
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            pulling.set(true);
            try {
                final var assertionValue2Argument2 =
                        q.pull(1, Duration.ofSeconds(30L)).messages().size();
                Assertions.assertEquals(1, assertionValue2Argument2, "Size should match the expected value.");
                final var assertionValue1Argument1 = Thread.currentThread().isInterrupted();
                evaluatePmdArgument(assertionValue1Argument1);
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    @DisplayName("리스하는 동안 interrupt되어도 청크를 돌려준다 — check 2")
    void evenIfInterruptedDuringTheLeaseTheChunkIsReturnedCheck2() throws Exception {
        final AtomicInteger reads;
        final AtomicBoolean pulling;
        reads = new AtomicInteger();
        pulling = new AtomicBoolean();
        try (Smq smq = this.builder()
                .durability(this.durability)
                .clock(() -> {
                    if (pulling.get() && reads.incrementAndGet() == 2) {
                        Thread.currentThread().interrupt();
                    }
                    return T0;
                })
                .open()) {
            final Queue q;
            q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            pulling.set(true);
            try {
                final var assertionValue2Argument2 =
                        q.pull(1, Duration.ofSeconds(30L)).messages().size();
                evaluatePmdArgument(assertionValue2Argument2);
                final var assertionValue1Argument1 = Thread.currentThread().isInterrupted();
                Assertions.assertTrue(assertionValue1Argument1, "Current thread should be interrupted.");
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    @DisplayName("다시 열어도 선언한 옵션과 참조가 남아 있고 시작할 때마다 안심하고 선언할 수 있다 — check 1")
    void
            evenIfYouReopenItTheDeclaredOptionsAndReferencesRemainAndYouCanDeclareThemWithConfidenceEachTimeYouStartCheck1() {
        final QueueOptions options;
        options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = this.builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = this.builder().open()) {
            final QueueOptions assertionValue1Argument2;
            second.declare("dead", QueueOptions.defaults());
            assertionValue1Argument2 = second.declare("orders", options).options();
            Assertions.assertEquals(options, assertionValue1Argument2, "Options should match the expected value.");
            evaluate(second.queue("orders").options());
            evaluate(captureThrowable(() -> second.declare("orders", QueueOptions.defaults())));
            evaluate(captureThrowable(() -> second.delete("dead")));
        }
    }

    @Test
    @DisplayName("다시 열어도 선언한 옵션과 참조가 남아 있고 시작할 때마다 안심하고 선언할 수 있다 — check 2")
    void
            evenIfYouReopenItTheDeclaredOptionsAndReferencesRemainAndYouCanDeclareThemWithConfidenceEachTimeYouStartCheck2() {
        final QueueOptions options;
        options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = this.builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = this.builder().open()) {
            final QueueOptions assertionValue2Argument2;
            second.declare("dead", QueueOptions.defaults());
            evaluate(second.declare("orders", options).options());
            assertionValue2Argument2 = second.queue("orders").options();
            Assertions.assertEquals(options, assertionValue2Argument2, "Options should match the expected value.");
            evaluate(captureThrowable(() -> second.declare("orders", QueueOptions.defaults())));
            evaluate(captureThrowable(() -> second.delete("dead")));
        }
    }

    @Test
    @DisplayName("다시 열어도 선언한 옵션과 참조가 남아 있고 시작할 때마다 안심하고 선언할 수 있다 — check 3")
    void
            evenIfYouReopenItTheDeclaredOptionsAndReferencesRemainAndYouCanDeclareThemWithConfidenceEachTimeYouStartCheck3() {
        final QueueOptions options;
        options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = this.builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = this.builder().open()) {
            final Throwable assertionValue3;
            second.declare("dead", QueueOptions.defaults());
            evaluate(second.declare("orders", options).options());
            evaluate(second.queue("orders").options());
            assertionValue3 = captureThrowable(() -> second.declare("orders", QueueOptions.defaults()));
            Assertions.assertInstanceOf(
                    QueueDeclarationConflictException.class,
                    assertionValue3,
                    "Declare should throw QueueDeclarationConflictException.");
            evaluate(captureThrowable(() -> second.delete("dead")));
        }
    }

    @Test
    @DisplayName("다시 열어도 선언한 옵션과 참조가 남아 있고 시작할 때마다 안심하고 선언할 수 있다 — check 4")
    void
            evenIfYouReopenItTheDeclaredOptionsAndReferencesRemainAndYouCanDeclareThemWithConfidenceEachTimeYouStartCheck4() {
        final QueueOptions options;
        options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = this.builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = this.builder().open()) {
            final Throwable assertionValue4;
            second.declare("dead", QueueOptions.defaults());
            evaluate(second.declare("orders", options).options());
            evaluate(second.queue("orders").options());
            evaluate(captureThrowable(() -> second.declare("orders", QueueOptions.defaults())));
            assertionValue4 = captureThrowable(() -> second.delete("dead"));
            Assertions.assertInstanceOf(
                    QueueInUseException.class, assertionValue4, "Delete should throw QueueInUseException.");
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 1")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck1()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final QueueCounts assertionValue1Argument1;
            final QueueCounts assertionValue1Argument2;
            final Chunk again;
            q = second.queue("q");
            assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
            assertionValue1Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 2")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck2()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final boolean assertionValue2Argument1;
            final Chunk again;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
            Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 3")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck3()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final QueueCounts assertionValue3Argument1;
            final QueueCounts assertionValue3Argument2;
            final Chunk again;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
            assertionValue3Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
            again = q.pull(1, Duration.ZERO);
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 4")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck4()
            throws Exception {
        final MessageId id;
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            id = q.publish(msg("a"));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Chunk again;
            final MessageId assertionValue4Argument2;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            assertionValue4Argument2 = again.messages().getFirst().id();
            Assertions.assertEquals(id, assertionValue4Argument2, "Message ID should match the expected value.");
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 5")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck5()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Chunk again;
            final int assertionValue5Argument2;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            assertionValue5Argument2 = again.messages().getFirst().deliveryCount();
            Assertions.assertEquals(2, assertionValue5Argument2, "Delivery count should match the expected value.");
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 6")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck6()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Chunk again;
            final Throwable assertionValue6;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            assertionValue6 = captureThrowable(() -> q.settle(lost.ackAll()));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue6, "Settle should throw IllegalArgumentException.");
            evaluate(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("닫아도 정산하지 않은 리스는 남고 다시 연 뒤 만료되면 재전달된다 — check 7")
    void evenIfYouCloseItAnyUnsettledLeasesRemainAndTheyAreRetransmittedWhenTheyExpireAfterBeingReopenedCheck7()
            throws Exception {
        final Chunk lost;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            evaluate(q.publish(msg("a")));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10L));
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Chunk again;
            final boolean assertionValue7Argument1;
            q = second.queue("q");
            evaluate(new QueueCounts(0L, 1L, 0L));
            evaluate(q.pull(1, Duration.ZERO).isEmpty());
            this.clock.advance(Duration.ofSeconds(10L));
            evaluate(new QueueCounts(0L, 1L, 0L));
            again = q.pull(1, Duration.ZERO);
            evaluate(captureThrowable(() -> q.settle(lost.ackAll())));
            assertionValue7Argument1 = q.settle(again.ackAll()).allOk();
            Assertions.assertTrue(assertionValue7Argument1, "Outcomes should succeed.");
        }
    }

    @Test
    @DisplayName("다시 연 세션에서는 이전 세션의 청크를 정산하거나 연장할 수 없다 — check 1")
    void inAReopenedSessionChunksFromThePreviousSessionCannotBeSettledOrExtendedCheck1() throws Exception {
        final Chunk held;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Throwable assertionValue1;
            q = second.queue("q");
            assertionValue1 = captureThrowable(() -> q.settle(held.ackAll()));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue1, "Settle should throw IllegalArgumentException.");
            evaluate(captureThrowable(() -> q.extend(held, Duration.ofSeconds(20L))));
            evaluate(new QueueCounts(0L, 1L, 0L));
        }
    }

    @Test
    @DisplayName("다시 연 세션에서는 이전 세션의 청크를 정산하거나 연장할 수 없다 — check 2")
    void inAReopenedSessionChunksFromThePreviousSessionCannotBeSettledOrExtendedCheck2() throws Exception {
        final Chunk held;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final Throwable assertionValue2;
            q = second.queue("q");
            evaluate(captureThrowable(() -> q.settle(held.ackAll())));
            assertionValue2 = captureThrowable(() -> q.extend(held, Duration.ofSeconds(20L)));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue2, "Extend should throw IllegalArgumentException.");
            evaluate(new QueueCounts(0L, 1L, 0L));
        }
    }

    @Test
    @DisplayName("다시 연 세션에서는 이전 세션의 청크를 정산하거나 연장할 수 없다 — check 3")
    void inAReopenedSessionChunksFromThePreviousSessionCannotBeSettledOrExtendedCheck3() throws Exception {
        final Chunk held;
        try (Smq first = this.builder().clock(this.clock).open()) {
            final Queue q;
            q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = this.builder().clock(this.clock).open()) {
            final Queue q;
            final QueueCounts assertionValue3Argument1;
            final QueueCounts assertionValue3Argument2;
            q = second.queue("q");
            evaluate(captureThrowable(() -> q.settle(held.ackAll())));
            evaluate(captureThrowable(() -> q.extend(held, Duration.ofSeconds(20L))));
            assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
            assertionValue3Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        }
    }

    @Test
    @DisplayName("호출 순서와 상관없이 durability와 공용 설정을 체인할 수 있다 — check 1")
    void durabilityAndCommonSettingsCanBeChainedRegardlessOfTheOrderOfCallsCheck1() {
        try (Smq a = Smq.sqlite(this.dir.resolve("a.db"))
                        .clock(this.clock)
                        .durability(Durability.PROCESS_CRASH)
                        .leaseDuration(Duration.ofSeconds(1L))
                        .open();
                Smq b = Smq.sqlite(this.dir.resolve("b.db"))
                        .durability(Durability.PROCESS_CRASH)
                        .clock(this.clock)
                        .leaseDuration(Duration.ofSeconds(1L))
                        .open()) {
            final Queue assertionValue1Argument1;
            assertionValue1Argument1 = a.declare("q", QueueOptions.defaults());
            Assertions.assertNotNull(assertionValue1Argument1, "Declare should be present.");
            evaluate(b.declare("q", QueueOptions.defaults()));
        }
    }

    @Test
    @DisplayName("호출 순서와 상관없이 durability와 공용 설정을 체인할 수 있다 — check 2")
    void durabilityAndCommonSettingsCanBeChainedRegardlessOfTheOrderOfCallsCheck2() {
        try (Smq a = Smq.sqlite(this.dir.resolve("a.db"))
                        .clock(this.clock)
                        .durability(Durability.PROCESS_CRASH)
                        .leaseDuration(Duration.ofSeconds(1L))
                        .open();
                Smq b = Smq.sqlite(this.dir.resolve("b.db"))
                        .durability(Durability.PROCESS_CRASH)
                        .clock(this.clock)
                        .leaseDuration(Duration.ofSeconds(1L))
                        .open()) {
            final Queue assertionValue2Argument1;
            evaluate(a.declare("q", QueueOptions.defaults()));
            assertionValue2Argument1 = b.declare("q", QueueOptions.defaults());
            Assertions.assertNotNull(assertionValue2Argument1, "Declare should be present.");
        }
    }

    @Test
    @DisplayName("기본 내구성 수준은 PROCESS CRASH다")
    void theDefaultDurabilityLevelIsPROCESSCRASH() {
        Assertions.assertEquals(
                Durability.PROCESS_CRASH,
                Smq.sqlite(this.dir.resolve("default.db")).durability(),
                "Durability should match the expected value.");
    }

    @Test
    @DisplayName("자동 회수 옵션은 새 DB에만 적용된다 — check 1")
    void theAutoCleanupOptionAppliesOnlyToNewDBsCheck1() throws Exception {
        final Path path;
        path = this.dir.resolve("vacuum-option.db");
        Smq.sqlite(path).autoVacuum(false).open().close();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA auto_vacuum")) {
            final int assertionValue1Argument2;
            if (!result.next()) {
                throw new AssertionError("Expected a query result row");
            }
            assertionValue1Argument2 = result.getInt(1);
            Assertions.assertEquals(0, assertionValue1Argument2, "Int should match the expected value.");
        }
        Smq.sqlite(path).autoVacuum(true).open().close();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA auto_vacuum")) {
            if (!result.next()) {
                throw new AssertionError("Expected a query result row");
            }
            evaluate(result.getInt(1));
        }
    }

    @Test
    @DisplayName("자동 회수 옵션은 새 DB에만 적용된다 — check 2")
    void theAutoCleanupOptionAppliesOnlyToNewDBsCheck2() throws Exception {
        final Path path;
        path = this.dir.resolve("vacuum-option.db");
        Smq.sqlite(path).autoVacuum(false).open().close();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA auto_vacuum")) {
            if (!result.next()) {
                throw new AssertionError("Expected a query result row");
            }
            evaluate(result.getInt(1));
        }
        Smq.sqlite(path).autoVacuum(true).open().close();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA auto_vacuum")) {
            final int assertionValue2Argument2;
            if (!result.next()) {
                throw new AssertionError("Expected a query result row");
            }
            assertionValue2Argument2 = result.getInt(1);
            Assertions.assertEquals(0, assertionValue2Argument2, "Int should match the expected value.");
        }
    }

    @Test
    @DisplayName("SQLite 빌더의 공개 옵션에 lock 대기 하한이 없다 — check 1")
    void thereIsNoLowerLimitForLockWaitTimeInThePublicOptionsOfTheSQLiteBuilderCheck1() {
        final Set<String> publicMethods;
        final Set<String> assertionValue1Argument1;
        publicMethods = Arrays.stream(SqliteBuilder.class.getMethods())
                .filter(method -> method.getDeclaringClass() == SqliteBuilder.class)
                .map(Method::getName)
                .collect(Collectors.toSet());
        assertionValue1Argument1 = Set.of("durability", "clock", "leaseDuration", "autoVacuum", "open");
        Assertions.assertEquals(
                assertionValue1Argument1, publicMethods, "Public methods should match the expected value.");
    }

    @Test
    @DisplayName("SQLite 빌더의 공개 옵션에 lock 대기 하한이 없다 — check 2")
    void thereIsNoLowerLimitForLockWaitTimeInThePublicOptionsOfTheSQLiteBuilderCheck2() {
        evaluate(Arrays.stream(SqliteBuilder.class.getMethods())
                .filter(method -> method.getDeclaringClass() == SqliteBuilder.class)
                .map(Method::getName)
                .collect(Collectors.toSet()));
        Assertions.assertEquals(0, SqliteBuilder.class.getFields().length, "Length should match the expected value.");
    }

    @Test
    @DisplayName("두 내구성 수준 모두 발행과 꺼내기가 동작한다")
    void bothDurabilityLevelsSupportIssuingAndRetrieving() throws Exception {
        for (final Durability d : Durability.values()) {
            try (Smq smq = Smq.sqlite(this.dir.resolve(d + ".db")).durability(d).open()) {
                final Queue q;
                q = smq.declare("q", QueueOptions.defaults());
                q.publish(msg("a"));
                Assertions.assertEquals(
                        1, q.pull(1, Duration.ZERO).messages().size(), "Size should match the expected value.");
            }
        }
    }

    @Test
    @DisplayName("lock 대기 하한을 없애 빈 청크가 실제로 나오는 조건에서도 동시 꺼내기 계약을 지킨다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq in direct and closes it after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            byRemovingTheLockWaitLowerLimitTheConcurrentRetrievalContractIsUpheldEvenUnderConditionsWhereEmptyChunksActuallyAppear()
                    throws Exception {
        final Smq smq;
        final int attempts;
        final List<String> failures;
        SmqConformanceTest.ConcurrentPullResult result;
        smq = this.builder()
                .clock(this.clock)
                .leaseDuration(Duration.ofMinutes(10L))
                .lockWaitFloor(Duration.ZERO)
                .open();
        this.direct.add(smq);
        attempts = 10;
        failures = new ArrayList<>();
        result = new SmqConformanceTest.ConcurrentPullResult(0, List.of());
        for (int attempt = 0; attempt < attempts && result.roundsWithEmptyChunk() == 0; attempt++) {
            result = collectConcurrentPullResult(smq, "race" + attempt + "-");
            failures.addAll(result.failures());
        }
        Assertions.assertTrue(
                result.roundsWithEmptyChunk() > 0 && failures.isEmpty(),
                "하한을 없앤 동시 꺼내기 실패. 빈 청크 횟수=" + result.roundsWithEmptyChunk() + ", 위반=" + String.join("; ", failures)
                        + "; " + attempts + "판 동안 빈 청크가 없으면 이 조건이 경합을 만들지 못한 것입니다.");
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
