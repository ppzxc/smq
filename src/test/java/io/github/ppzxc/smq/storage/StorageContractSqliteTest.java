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
    void 꺼내기_쓰기_트랜잭션은_시계를_다시_읽는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        AtomicInteger reads = new AtomicInteger();
        PullResult result = storage.pull("q", 1, LEASE, () -> reads.getAndIncrement() == 0 ? T0 : T0.plusSeconds(60));
        assertEquals(2, reads.get());
        assertEquals(T0.plusSeconds(70), result.leaseDeadline());
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
    void lock을_기다리는_꺼내기를_interrupt하면_리스하지_않는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<?> holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            assertTrue(locked.await(5, TimeUnit.SECONDS));
            Thread puller = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                puller.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (puller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                assertEquals(Thread.State.TIMED_WAITING, puller.getState());
                puller.interrupt();
                puller.join(5000);
                assertFalse(puller.isAlive());
                InterruptedException interrupted = assertInstanceOf(InterruptedException.class, failure.get());
                assertTrue(java.util.Arrays.stream(interrupted.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks.")));
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
        assertEquals(0, storage.counts("q").leased());
        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size());
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "The test waits for the lock holder and propagates worker failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    void 다음_준비_시각을_lock_대기_중_interrupt하면_예외를_전달한다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<InterruptedException> failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<?> holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            Thread reader = new Thread(() -> {
                try {
                    storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    failure.set(e);
                }
            });
            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                reader.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (reader.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                assertEquals(Thread.State.TIMED_WAITING, reader.getState());
                reader.interrupt();
                reader.join(5000);
                assertFalse(reader.isAlive());
                assertNotNull(failure.get());
                assertTrue(java.util.Arrays.stream(failure.get().getStackTrace())
                        .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks.")));
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
    void 다음_준비_시각은_lock_대기_마감이_끝나면_비어있다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        assertTrue(storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofSeconds(1))
                .isPresent());
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<?> holder = executor.submit(() -> storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                assertTrue(storage.nextReadyAt("q", InstantSource.fixed(T0), Duration.ofMillis(50))
                        .isEmpty());
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void 리스하는_동안_interrupt되어도_꺼낸_결과를_돌려준다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
        AtomicInteger reads = new AtomicInteger();
        try {
            PullResult result = storage.pull(
                    "q",
                    1,
                    LEASE,
                    () -> {
                        if (reads.incrementAndGet() == 2) Thread.currentThread().interrupt();
                        return T0;
                    },
                    Duration.ofSeconds(30));
            assertEquals(1, result.messages().size());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Override
    StorageFactory factory() {
        return StorageFactory.sqliteNew(durability);
    }
}
