package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

abstract class StorageContractTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration LEASE = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    Storage storage;

    abstract StorageFactory factory();

    private static NewMessage message(int b) {
        return new NewMessage(new byte[] {(byte) b}, Map.of());
    }

    @BeforeEach
    void openStorage() {
        storage = factory().open(dir);
    }

    @AfterEach
    void closeStorage() {
        storage.close();
    }

    @Test
    void 발행은_lock_안에서_시각을_읽는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        afterLockWait(clock -> {
            storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5))), clock);
            return null;
        });
        assertTrue(storage.pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64)))
                .messages()
                .isEmpty());
        assertEquals(
                T0.plusSeconds(60),
                storage.pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65)))
                        .messages()
                        .getLast()
                        .publishedAt());
    }

    @Test
    void 꺼내기는_lock_안에서_시각을_읽는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult result = afterLockWait(clock -> storage.pull("q", 1, LEASE, clock));
        assertEquals(T0.plusSeconds(70), result.leaseDeadline());
    }

    @Test
    void 꺼내기가_끝난_지연을_준비로_기록하고_집계를_갱신한다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5))),
                InstantSource.fixed(T0));

        assertEquals(new QueueCounts(0, 0, 2), storage.counts("q"));
        assertTrue(storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4)))
                .messages()
                .isEmpty());
        assertEquals(new QueueCounts(0, 0, 2), storage.counts("q"));

        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5)))
                        .messages()
                        .size());
        assertEquals(new QueueCounts(1, 1, 0), storage.counts("q"));
    }

    @Test
    void 단일_활성_리스가_꺼내기를_막으면_끝난_지연과_만료_리스_집계를_유지한다() {
        storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20))),
                InstantSource.fixed(T0));
        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).messages().size());

        assertEquals(new QueueCounts(0, 1, 2), storage.counts("q"));
        assertTrue(storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5)))
                .messages()
                .isEmpty());
        assertEquals(new QueueCounts(0, 1, 2), storage.counts("q"));

        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10)))
                        .messages()
                        .size());
        assertEquals(new QueueCounts(1, 1, 1), storage.counts("q"));
    }

    @Test
    void 가상_시계_명령열은_독립_리스_모델과_만료_정각에도_일치한다() {
        storage.declare("q", QueueOptions.defaults());
        var model = new LeaseReferenceModel();
        var published = storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);

        var oldLease = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        var expectedOld = model.pull(T0, LEASE);
        assertEquals(
                expectedOld.messages(),
                oldLease.messages().stream().map(Message::id).toList());
        Instant oldDeadline = T0.plus(LEASE);

        assertEquals(model.counts(), storage.counts("q"));
        Instant atDeadline = oldDeadline;
        var redelivery = storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        var expectedRedelivery = model.pull(atDeadline, LEASE);
        assertEquals(
                expectedRedelivery.messages(),
                redelivery.messages().stream().map(Message::id).toList());
        assertEquals(model.counts(), storage.counts("q"));

        var stale = storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Disposition.Kind.ACK)),
                InstantSource.fixed(atDeadline));
        assertEquals(Outcome.STALE, model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        assertEquals(Outcome.STALE, stale.getFirst().outcome());
        var current = storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Disposition.Kind.ACK)),
                InstantSource.fixed(atDeadline));
        assertEquals(Outcome.OK, model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        assertEquals(Outcome.OK, current.getFirst().outcome());
        assertEquals(model.counts(), storage.counts("q"));
        var finalPull = storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        assertEquals(
                model.pull(atDeadline, LEASE).messages(),
                finalPull.messages().stream().map(Message::id).toList());
    }

    private static final class LeaseReferenceModel {
        private final List<MessageId> ready = new ArrayList<>();
        private final Map<MessageId, Lease> leased = new java.util.LinkedHashMap<>();
        private long generation;

        void publish(List<MessageId> ids) {
            ready.addAll(ids);
        }

        ExpectedPull pull(Instant now, Duration lease) {
            var expired = leased.entrySet().stream()
                    .filter(e -> !e.getValue().deadline().isAfter(now))
                    .map(Map.Entry::getKey)
                    .toList();
            for (MessageId id : expired) {
                leased.remove(id);
                ready.add(id);
            }
            ready.sort(MessageId::compareTo);
            if (ready.isEmpty()) return new ExpectedPull(List.of(), -1);
            MessageId id = ready.removeFirst();
            generation++;
            long leaseGeneration = generation;
            leased.put(id, new Lease(leaseGeneration, now.plus(lease)));
            return new ExpectedPull(List.of(id), leaseGeneration);
        }

        Outcome settle(MessageId id, long leaseGeneration) {
            Lease lease = leased.get(id);
            if (lease == null || lease.generation() != leaseGeneration) return Outcome.STALE;
            leased.remove(id);
            return Outcome.OK;
        }

        QueueCounts counts() {
            return new QueueCounts(ready.size(), leased.size(), 0);
        }

        private record Lease(long generation, Instant deadline) {}

        private record ExpectedPull(List<MessageId> messages, long generation) {}
    }

    @Test
    void 리스_만료_정각의_재꺼내기와_정산_경합은_직렬_순서_중_하나와_일치한다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult old = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        var start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<List<Result>> settle = executor.submit(() -> {
                start.await();
                return storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Disposition.Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            Future<PullResult> pull = executor.submit(() -> {
                start.await();
                return storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            var settled = settle.get(5, TimeUnit.SECONDS).getFirst();
            var repulled = pull.get(5, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                assertTrue(repulled.messages().isEmpty(), "정산이 먼저 커밋되면 다시 꺼낼 메시지가 없다");
                assertEquals(new QueueCounts(0, 0, 0), storage.counts("q"));
            } else {
                assertEquals(Outcome.STALE, settled.outcome());
                assertEquals(
                        old.messages().getFirst().id(),
                        repulled.messages().getFirst().id());
                assertEquals(new QueueCounts(0, 1, 0), storage.counts("q"));
            }
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    void lock_대기_마감이_끝나면_꺼내기는_상태를_바꾸지_않는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
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
            Future<PullResult> pull =
                    executor.submit(() -> storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofMillis(50)));
            PullResult result;
            try {
                result = pull.get(1, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
            assertTrue(result.messages().isEmpty());
            assertEquals(new QueueCounts(1, 0, 0), storage.counts("q"));
        }
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
    void lock을_기다리는_꺼내기를_interrupt하면_리스하지_않는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
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
            Thread pull = new Thread(() -> {
                try {
                    storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                pull.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (pull.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                assertEquals(Thread.State.TIMED_WAITING, pull.getState());
                pull.interrupt();
                pull.join(5000);
                assertFalse(pull.isAlive());
                assertInstanceOf(InterruptedException.class, failure.get());
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
            assertEquals(new QueueCounts(1, 0, 0), storage.counts("q"));
        }
    }

    @Test
    void 정산은_lock_안에서_시각을_읽는다() throws Exception {
        storage.declare("q", QueueOptions.defaults());
        MessageId id = storage.publish("q", List.of(message(1)), InstantSource.fixed(T0))
                .getFirst();
        PullResult pulled = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        afterLockWait(clock -> storage.settle(
                pulled.token(), List.of(new Disposition(id, Disposition.Kind.NACK, Duration.ofSeconds(5))), clock));
        assertTrue(storage.pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64)))
                .messages()
                .isEmpty());
        assertEquals(
                id,
                storage.pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65)))
                        .messages()
                        .getFirst()
                        .id());
    }

    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    private <T> T afterLockWait(java.util.function.Function<InstantSource, T> operation) throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
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
            CountDownLatch started = new CountDownLatch(1);
            Future<T> result = executor.submit(() -> {
                started.countDown();
                return operation.apply(() -> {
                    reads.incrementAndGet();
                    return T0.plusSeconds(60);
                });
            });
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> result.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, reads.get());
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
            T value = result.get(5, TimeUnit.SECONDS);
            assertTrue(reads.get() > 0);
            return value;
        }
    }

    @Test
    void 발행은_저장소가_읽은_시각을_사용한다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        assertEquals(
                T0,
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0))
                        .messages()
                        .getFirst()
                        .publishedAt());
    }

    @Test
    void 꺼내기가_정확히_상한만큼_옮기면_모두_옮기고_남았다고_하지_않는다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult exact = pullAfterExhausting("exact", cap);
        assertTrue(exact.messages().isEmpty());
        assertFalse(exact.deadLettersRemain());
        assertTrue(exact.deadLettersMoved());
        assertEquals(cap, storage.counts("dead-exact").ready());
        assertEquals(0, storage.counts("exact").ready());
    }

    @Test
    void 상한보다_하나_많으면_상한만큼_옮기고_남았다고_알린다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult over = pullAfterExhausting("over", cap + 1);
        assertTrue(over.messages().isEmpty());
        assertTrue(over.deadLettersRemain());
        assertTrue(over.deadLettersMoved());
        assertEquals(cap, storage.counts("dead-over").ready());
    }

    @Test
    void 남았다는_표시_뒤의_다음_꺼내기가_나머지를_옮긴다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        pullAfterExhausting("over", cap + 1);
        PullResult next = storage.pull("over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertTrue(next.messages().isEmpty());
        assertFalse(next.deadLettersRemain());
        assertEquals(cap + 1, storage.counts("dead-over").ready());
    }

    @Test
    void 메시지를_꺼내면서_이동하면_이동_표시가_참이다() {
        storage.declare("dead", QueueOptions.defaults());
        storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        assertFalse(storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).deadLettersMoved());
        storage.publish("q", List.of(message(2)), InstantSource.fixed(T0));
        PullResult result = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertEquals(1, result.messages().size());
        assertTrue(result.deadLettersMoved());
    }

    @Test
    void 옮겨진_메시지는_새_메시지라_전달_횟수를_처음부터_센다() {
        pullAfterExhausting("over", DeadLetters.MAX_MOVES_PER_PULL + 1);
        Message moved = storage.pull("dead-over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .messages()
                .getFirst();
        assertEquals(1, moved.deliveryCount());
    }

    @Test
    void 발행_배치_중간에_실패하면_배치_전체가_반영되지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        var ok = message(1);
        var bad = new NewMessage(null, Map.of());
        assertThrows(RuntimeException.class, () -> storage.publish("q", List.of(ok, bad), InstantSource.fixed(T0)));
        assertEquals(0, storage.counts("q").ready());
    }

    @Test
    void 빈_꺼내기는_토큰과_기한이_비고_남았다는_표시가_거짓이다() {
        storage.declare("q", QueueOptions.defaults());
        PullResult empty = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        assertTrue(empty.messages().isEmpty());
        assertNull(empty.token());
        assertNull(empty.leaseDeadline());
        assertFalse(empty.deadLettersRemain());
        assertFalse(empty.deadLettersMoved());
    }

    @Test
    void 꺼낼_때마다_새_리스_토큰이_나온다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        LeaseToken first = storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        LeaseToken second = storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first, second);
    }

    @Test
    void 리스가_만료된_뒤_같은_큐에서_다시_꺼내도_새_리스_토큰이_나온다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        LeaseToken first = storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        PullResult again = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertEquals(1, again.messages().size());
        assertNotEquals(first, again.token());
    }

    @Test
    void 리스_기한_정확히에는_다시_꺼낼_수_있고_1ns_앞에는_꺼낼_수_없다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Instant deadline = T0.plus(LEASE);
        assertTrue(storage.pull("q", 1, LEASE, InstantSource.fixed(deadline.minusNanos(1)))
                .messages()
                .isEmpty());
        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(deadline))
                        .messages()
                        .size());
    }

    @Test
    void 지연_시각_정확히에는_꺼낼_수_있고_1ns_앞에는_꺼낼_수_없다() {
        storage.declare("q", QueueOptions.defaults());
        Duration delay = Duration.ofSeconds(5);
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), delay)), InstantSource.fixed(T0));
        Instant availableAt = T0.plus(delay);
        assertTrue(storage.pull("q", 1, LEASE, InstantSource.fixed(availableAt.minusNanos(1)))
                .messages()
                .isEmpty());
        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(availableAt))
                        .messages()
                        .size());
    }

    @Test
    void 메시지가_전부_낡은_연장도_기한은_now_더하기_d이고_메시지별_결과는_STALE이다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult pulled = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        MessageId id = pulled.messages().getFirst().id();
        storage.settle(pulled.token(), List.of(new Disposition(id, Disposition.Kind.ACK)), InstantSource.fixed(T0));

        Instant now = T0.plusSeconds(1);
        Duration d = Duration.ofSeconds(30);
        Extended extended = storage.extend(pulled.token(), List.of(id), d, InstantSource.fixed(now));

        assertEquals(now.plus(d), extended.leaseDeadline());
        assertEquals(List.of(new Result(id, Outcome.STALE)), extended.results());
    }

    @Test
    void 단일_활성_큐에서_다른_청크가_꺼낸_뒤_늦게_도착한_연장은_만료된_리스를_되살리지_못한다() {
        storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        PullResult first = storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        MessageId a = first.messages().get(0).id();
        MessageId b = first.messages().get(1).id();

        PullResult second = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertEquals(a, second.messages().getFirst().id());

        Extended late = storage.extend(first.token(), List.of(a, b), LEASE, InstantSource.fixed(T0.plusSeconds(12)));

        assertEquals(List.of(new Result(a, Outcome.STALE), new Result(b, Outcome.STALE)), late.results());

        assertEquals(new QueueCounts(0, 2, 0), storage.counts("q"));
        PullResult third =
                storage.pull("q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE).plus(LEASE)));
        assertEquals(List.of(a, b), third.messages().stream().map(Message::id).toList());
    }

    @Test
    void 단일_활성_큐에서_둘째_청크가_정산한_뒤에_도착한_연장도_만료된_리스를_되살리지_못한다() {
        storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        PullResult first = storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        MessageId a = first.messages().get(0).id();
        MessageId b = first.messages().get(1).id();
        PullResult second = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        storage.settle(
                second.token(),
                List.of(new Disposition(a, Disposition.Kind.ACK)),
                InstantSource.fixed(T0.plusSeconds(11)));

        Extended late = storage.extend(first.token(), List.of(b), LEASE, InstantSource.fixed(T0.plusSeconds(12)));

        assertEquals(List.of(new Result(b, Outcome.STALE)), late.results());

        assertEquals(new QueueCounts(0, 1, 0), storage.counts("q"));
        PullResult third = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15)));
        assertEquals(List.of(b), third.messages().stream().map(Message::id).toList());
    }

    @Test
    void 단일_활성_큐에서_상한에_막혀_옮겨지지_못한_소진_메시지도_늦은_연장이_되살리지_못한다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        storage.declare("dead", QueueOptions.defaults());
        storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        List<NewMessage> batch = new ArrayList<>();
        for (int i = 0; i < cap + 1; i++) {
            batch.add(message(i));
        }
        storage.publish("q", batch, InstantSource.fixed(T0));
        PullResult first = storage.pull("q", cap + 1, LEASE, InstantSource.fixed(T0));
        List<MessageId> ids = first.messages().stream().map(Message::id).toList();

        assertTrue(
                storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE))).deadLettersRemain());

        Extended late = storage.extend(first.token(), ids, LEASE, InstantSource.fixed(T0.plusSeconds(12)));

        assertTrue(late.results().stream().allMatch(r -> r.outcome() == Outcome.STALE));

        PullResult third = storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15)));
        assertFalse(third.deadLettersRemain());
        assertEquals(new QueueCounts(0, 0, 0), storage.counts("q"));
    }

    @Test
    void 일반_큐는_기한이_지났어도_아무도_다시_꺼내지_않았다면_연장이_OK이고_리스를_되살린다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult pulled = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        MessageId id = pulled.messages().getFirst().id();

        Instant now = T0.plus(LEASE).plusSeconds(5);
        Extended extended = storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(now));

        assertEquals(List.of(new Result(id, Outcome.OK)), extended.results());
        assertEquals(1, storage.counts("q").leased());
    }

    @Test
    void 단일_활성_큐의_연장은_기한_정확히에는_STALE이고_1ns_앞에는_OK다() {
        storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult pulled = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        MessageId id = pulled.messages().getFirst().id();

        Instant justBefore = T0.plus(LEASE).minusNanos(1);
        Extended before = storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(justBefore));
        assertEquals(List.of(new Result(id, Outcome.OK)), before.results());

        Extended exactly =
                storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(before.leaseDeadline()));
        assertEquals(List.of(new Result(id, Outcome.STALE)), exactly.results());
    }

    @Test
    void 낡은_리스_토큰으로_정산하면_STALE이고_저장소_상태가_바뀌지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        PullResult first = storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        MessageId id = first.messages().getFirst().id();
        Instant second = T0.plus(LEASE);
        PullResult current = storage.pull("q", 1, LEASE, InstantSource.fixed(second));
        assertEquals(1, current.messages().size());

        List<Result> results = storage.settle(
                first.token(), List.of(new Disposition(id, Disposition.Kind.ACK)), InstantSource.fixed(second));

        assertEquals(List.of(new Result(id, Outcome.STALE)), results);
        assertEquals(new QueueCounts(0, 1, 0), storage.counts("q"));
        Instant expired = second.plus(LEASE);
        assertEquals(
                1,
                storage.pull("q", 1, LEASE, InstantSource.fixed(expired))
                        .messages()
                        .size());
    }

    @Test
    void 정산_안에_dead_letter_큐_없는_reject가_있으면_정산_전체가_반영되지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        PullResult pulled = storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        MessageId acked = pulled.messages().get(0).id();
        MessageId rejected = pulled.messages().get(1).id();

        assertThrows(
                RuntimeException.class,
                () -> storage.settle(
                        pulled.token(),
                        List.of(
                                new Disposition(acked, Disposition.Kind.ACK),
                                new Disposition(rejected, Disposition.Kind.REJECT)),
                        InstantSource.fixed(T0)));

        assertEquals(new QueueCounts(0, 2, 0), storage.counts("q"));
        assertEquals(
                2,
                storage.pull("q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                        .messages()
                        .size());
    }

    @Test
    void 잘못된_NACK_시각은_앞선_ACK도_반영하지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        PullResult pulled = storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        Instant nearMaximum = Instant.MAX.minusSeconds(2);

        assertThrows(
                java.time.DateTimeException.class,
                () -> storage.settle(
                        pulled.token(),
                        List.of(
                                new Disposition(pulled.messages().get(0).id(), Disposition.Kind.ACK),
                                new Disposition(
                                        pulled.messages().get(1).id(), Disposition.Kind.NACK, Duration.ofSeconds(3))),
                        InstantSource.fixed(nearMaximum)));

        assertEquals(new QueueCounts(0, 2, 0), storage.counts("q"));
        assertEquals(
                List.of(Outcome.OK, Outcome.OK),
                storage
                        .settle(
                                pulled.token(),
                                pulled.messages().stream()
                                        .map(message -> new Disposition(message.id(), Disposition.Kind.ACK))
                                        .toList(),
                                InstantSource.fixed(T0))
                        .stream()
                        .map(Result::outcome)
                        .toList());
    }

    private PullResult pullAfterExhausting(String queue, int count) {
        storage.declare("dead-" + queue, QueueOptions.defaults());
        storage.declare(queue, QueueOptions.defaults().maxDeliveries(1, "dead-" + queue));
        List<NewMessage> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(message(i));
        }
        storage.publish(queue, batch, InstantSource.fixed(T0));
        assertEquals(
                count,
                storage.pull(queue, count, LEASE, InstantSource.fixed(T0))
                        .messages()
                        .size());
        return storage.pull(queue, 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
    }
}
