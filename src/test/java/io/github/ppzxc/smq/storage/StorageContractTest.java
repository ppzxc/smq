package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.PolicyExceptions;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.Disposition.Kind;
import java.lang.Thread.State;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@PolicyException(
        rule = "PMD.CyclomaticComplexity",
        reason = "Each independent test method stays below the limit; the class total sums all scenarios.",
        approvedBy = "ppzxc",
        reviewBy = "2027-11-28")
@SuppressWarnings("PMD.CyclomaticComplexity")
abstract class StorageContractTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    static final Duration LEASE = Duration.ofSeconds(10L);

    @TempDir
    Path dir;

    Storage storage;

    abstract StorageFactory factory();

    private static NewMessage message(final int b) {
        return new NewMessage(new byte[] {(byte) b}, Map.of());
    }

    @BeforeEach
    void openStorage() {
        this.storage = this.factory().open(this.dir);
    }

    @AfterEach
    void closeStorage() {
        this.storage.close();
    }

    @Test
    @DisplayName("발행은 lock 안에서 시각을 읽는다 — check 1")
    void publishReadsTheClockInsideTheLockCheck1() throws Exception {
        final boolean assertionValue1Argument1;
        this.storage.declare("q", QueueOptions.defaults());
        this.afterLockWait(clock -> {
            this.storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L))), clock);
            return null;
        });
        assertionValue1Argument1 = this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64L)))
                .messages()
                .isEmpty();
        Assertions.assertTrue(assertionValue1Argument1, "Messages should be empty.");
        evaluate(T0.plusSeconds(60L));
        evaluate(this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65L)))
                .messages()
                .getLast()
                .publishedAt());
    }

    @Test
    @DisplayName("발행은 lock 안에서 시각을 읽는다 — check 2")
    void publishReadsTheClockInsideTheLockCheck2() throws Exception {
        final Instant assertionValue2Argument1;
        final Instant assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.afterLockWait(clock -> {
            this.storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L))), clock);
            return null;
        });
        evaluate(this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64L)))
                .messages()
                .isEmpty());
        assertionValue2Argument1 = T0.plusSeconds(60L);
        assertionValue2Argument2 = this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65L)))
                .messages()
                .getLast()
                .publishedAt();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Published at should match the expected value.");
    }

    @Test
    @DisplayName("꺼내기는 lock 안에서 시각을 읽는다")
    void theFetchOperationReadsTheTimestampWithinTheLock() throws Exception {
        final PullResult result;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        result = this.afterLockWait(clock -> this.storage.pull("q", 1, LEASE, clock));
        Assertions.assertEquals(
                T0.plusSeconds(70L), result.leaseDeadline(), "Lease deadline should match the expected value.");
    }

    @Test
    @DisplayName("꺼내기가 끝난 지연을 준비로 기록하고 집계를 갱신한다 — check 1")
    void recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck1() {
        final QueueCounts assertionValue1Argument1;
        final QueueCounts assertionValue1Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L))),
                InstantSource.fixed(T0));
        assertionValue1Argument1 = new QueueCounts(0L, 0L, 2L);
        assertionValue1Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 0L));
    }

    @Test
    @DisplayName("꺼내기가 끝난 지연을 준비로 기록하고 집계를 갱신한다 — check 2")
    void recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck2() {
        final boolean assertionValue2Argument1;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L))),
                InstantSource.fixed(T0));
        evaluate(new QueueCounts(0L, 0L, 2L));
        assertionValue2Argument1 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4L)))
                .messages()
                .isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Messages should be empty.");
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 0L));
    }

    @Test
    @DisplayName("꺼내기가 끝난 지연을 준비로 기록하고 집계를 갱신한다 — check 3")
    void recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck3() {
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L))),
                InstantSource.fixed(T0));
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4L)))
                .messages()
                .isEmpty());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 2L);
        assertionValue3Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 0L));
    }

    @Test
    @DisplayName("꺼내기가 끝난 지연을 준비로 기록하고 집계를 갱신한다 — check 4")
    void recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck4() {
        final int assertionValue4Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L))),
                InstantSource.fixed(T0));
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 0L, 2L));
        assertionValue4Argument2 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .size();
        Assertions.assertEquals(1, assertionValue4Argument2, "Size should match the expected value.");
        evaluate(new QueueCounts(1L, 1L, 0L));
    }

    @Test
    @DisplayName("꺼내기가 끝난 지연을 준비로 기록하고 집계를 갱신한다 — check 5")
    void recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck5() {
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish(
                "q",
                List.of(
                        new NewMessage(new byte[] {1}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L))),
                InstantSource.fixed(T0));
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(4L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 0L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .size());
        assertionValue5Argument1 = new QueueCounts(1L, 1L, 0L);
        assertionValue5Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 1")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck1() {
        final int assertionValue1Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        assertionValue1Argument2 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size();
        Assertions.assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 1L));
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 2")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck2() {
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size());
        assertionValue2Argument1 = new QueueCounts(0L, 1L, 2L);
        assertionValue2Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 1L));
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 3")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck3() {
        final boolean assertionValue3Argument1;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size());
        evaluate(new QueueCounts(0L, 1L, 2L));
        assertionValue3Argument1 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty();
        Assertions.assertTrue(assertionValue3Argument1, "Messages should be empty.");
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 1L));
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 4")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck4() {
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty());
        assertionValue4Argument1 = new QueueCounts(0L, 1L, 2L);
        assertionValue4Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size());
        evaluate(new QueueCounts(1L, 1L, 1L));
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 5")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck5() {
        final int assertionValue5Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 1L, 2L));
        assertionValue5Argument2 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size();
        Assertions.assertEquals(1, assertionValue5Argument2, "Size should match the expected value.");
        evaluate(new QueueCounts(1L, 1L, 1L));
    }

    @Test
    @DisplayName("단일 활성 리스가 꺼내기를 막으면 끝난 지연과 만료 리스 집계를 유지한다 — check 6")
    void ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck6() {
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish(
                "q",
                List.of(
                        message(1),
                        new NewMessage(new byte[] {2}, Map.of(), Duration.ofSeconds(5L)),
                        new NewMessage(new byte[] {3}, Map.of(), Duration.ofSeconds(20L))),
                InstantSource.fixed(T0));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0))
                .messages()
                .size());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(5L)))
                .messages()
                .isEmpty());
        evaluate(new QueueCounts(0L, 1L, 2L));
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(10L)))
                .messages()
                .size());
        assertionValue6Argument1 = new QueueCounts(1L, 1L, 1L);
        assertionValue6Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 1")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck1() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final List<MessageId> assertionValue1Argument1;
        final List<MessageId> assertionValue1Argument2;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        assertionValue1Argument1 = expectedOld.messages();
        assertionValue1Argument2 = oldLease.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Messages should match the expected value.");
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 2")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck2() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        assertionValue2Argument1 = model.counts();
        assertionValue2Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 3")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck3() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        assertionValue3Argument1 = expectedRedelivery.messages();
        assertionValue3Argument2 =
                redelivery.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 4")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck4() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        assertionValue4Argument1 = model.counts();
        assertionValue4Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 5")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck5() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final Outcome assertionValue5Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        assertionValue5Argument2 = model.settle(oldLease.messages().getFirst().id(), expectedOld.generation());
        Assertions.assertEquals(Outcome.STALE, assertionValue5Argument2, "Settlement should match the expected value.");
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 6")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck6() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final List<Result> stale;
        final Outcome assertionValue6Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        stale = this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        assertionValue6Argument2 = stale.getFirst().outcome();
        Assertions.assertEquals(Outcome.STALE, assertionValue6Argument2, "Outcome should match the expected value.");
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 7")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck7() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final Outcome assertionValue7Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        assertionValue7Argument2 = model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation());
        Assertions.assertEquals(Outcome.OK, assertionValue7Argument2, "Settlement should match the expected value.");
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 8")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck8() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final List<Result> current;
        final Outcome assertionValue8Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        current = this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        assertionValue8Argument2 = current.getFirst().outcome();
        Assertions.assertEquals(Outcome.OK, assertionValue8Argument2, "Outcome should match the expected value.");
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 9")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck9() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final QueueCounts assertionValue9Argument1;
        final QueueCounts assertionValue9Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        assertionValue9Argument1 = model.counts();
        assertionValue9Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue9Argument1, assertionValue9Argument2, "Counts should match the expected value.");
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline)));
        evaluate(model.pull(atDeadline, LEASE).messages());
    }

    @Test
    @DisplayName("가상 시계 명령열은 독립 리스 모델과 만료 정각에도 일치한다 — check 10")
    void theVirtualClockCommandQueueAlsoMatchesTheIndependentLeaseModelAndTheExactExpirationTimeCheck10() {
        final StorageContractTest.LeaseReferenceModel model;
        final List<MessageId> published;
        final PullResult oldLease;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedOld;
        final Instant oldDeadline;
        final Instant atDeadline;
        final PullResult redelivery;
        final StorageContractTest.LeaseReferenceModel.ExpectedPull expectedRedelivery;
        final PullResult finalPull;
        final List<MessageId> assertionValue10Argument1;
        final List<MessageId> assertionValue10Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        model = new StorageContractTest.LeaseReferenceModel();
        published = this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        model.publish(published);
        oldLease = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        expectedOld = model.pull(T0, LEASE);
        oldDeadline = T0.plus(LEASE);
        atDeadline = oldDeadline;
        redelivery = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        expectedRedelivery = model.pull(atDeadline, LEASE);
        evaluate(this.storage.settle(
                oldLease.token(),
                List.of(new Disposition(oldLease.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(oldLease.messages().getFirst().id(), expectedOld.generation()));
        evaluate(this.storage.settle(
                redelivery.token(),
                List.of(new Disposition(redelivery.messages().getFirst().id(), Kind.ACK)),
                InstantSource.fixed(atDeadline)));
        evaluate(model.settle(redelivery.messages().getFirst().id(), expectedRedelivery.generation()));
        finalPull = this.storage.pull("q", 1, LEASE, InstantSource.fixed(atDeadline));
        assertionValue10Argument1 = model.pull(atDeadline, LEASE).messages();
        assertionValue10Argument2 =
                finalPull.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue10Argument1, assertionValue10Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("리스 만료 정각의 재꺼내기와 정산 경합은 직렬 순서 중 하나와 일치한다 — check 1")
    void theReFetchAndSettlementRaceAtTheExactTimeOfLeaseExpirationCorrespondToOneOfTheSerialOrders() throws Exception {
        final PullResult old;
        final CountDownLatch start;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        old = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<List<Result>> settle;
            final Future<PullResult> pull;
            final Result settled;
            final PullResult repulled;
            settle = executor.submit(() -> {
                start.await();
                return this.storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            pull = executor.submit(() -> {
                start.await();
                return this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            settled = settle.get(5L, TimeUnit.SECONDS).getFirst();
            repulled = pull.get(5L, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                final var assertionValue5Argument1 = repulled.messages().isEmpty();
                Assertions.assertTrue(assertionValue5Argument1, "정산이 먼저 커밋되면 다시 꺼낼 메시지가 없다");
                final var assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue4Argument2);
            } else {
                final var assertionValue3Argument2 = settled.outcome();
                evaluatePmdArgument(assertionValue3Argument2);
                final var assertionValue2Argument1 = old.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue2Argument2 =
                        repulled.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument2);
                final var assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
                evaluatePmdArgument(assertionValue1Argument1);
                final var assertionValue1Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue1Argument2);
            }
        }
    }

    @Test
    @DisplayName("리스 만료 정각의 재꺼내기와 정산 경합은 직렬 순서 중 하나와 일치한다 — check 2")
    void theReFetchAndSettlementRaceAtTheExactTimeOfLeaseExpirationCorrespondToOneOfTheSerialOrdersCheck2()
            throws Exception {
        final PullResult old;
        final CountDownLatch start;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        old = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<List<Result>> settle;
            final Future<PullResult> pull;
            final Result settled;
            final PullResult repulled;
            settle = executor.submit(() -> {
                start.await();
                return this.storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            pull = executor.submit(() -> {
                start.await();
                return this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            settled = settle.get(5L, TimeUnit.SECONDS).getFirst();
            repulled = pull.get(5L, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                final var assertionValue5Argument1 = repulled.messages().isEmpty();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
                final var assertionValue4Argument2 = this.storage.counts("q");
                Assertions.assertEquals(
                        assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
            } else {
                final var assertionValue3Argument2 = settled.outcome();
                evaluatePmdArgument(assertionValue3Argument2);
                final var assertionValue2Argument1 = old.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue2Argument2 =
                        repulled.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument2);
                final var assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
                evaluatePmdArgument(assertionValue1Argument1);
                final var assertionValue1Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue1Argument2);
            }
        }
    }

    @Test
    @DisplayName("리스 만료 정각의 재꺼내기와 정산 경합은 직렬 순서 중 하나와 일치한다 — check 3")
    void theReFetchAndSettlementRaceAtTheExactTimeOfLeaseExpirationCorrespondToOneOfTheSerialOrdersCheck3()
            throws Exception {
        final PullResult old;
        final CountDownLatch start;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        old = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<List<Result>> settle;
            final Future<PullResult> pull;
            final Result settled;
            final PullResult repulled;
            settle = executor.submit(() -> {
                start.await();
                return this.storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            pull = executor.submit(() -> {
                start.await();
                return this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            settled = settle.get(5L, TimeUnit.SECONDS).getFirst();
            repulled = pull.get(5L, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                final var assertionValue5Argument1 = repulled.messages().isEmpty();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue4Argument2);
            } else {
                final var assertionValue3Argument2 = settled.outcome();
                Assertions.assertEquals(
                        Outcome.STALE, assertionValue3Argument2, "Outcome should match the expected value.");
                final var assertionValue2Argument1 = old.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue2Argument2 =
                        repulled.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument2);
                final var assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
                evaluatePmdArgument(assertionValue1Argument1);
                final var assertionValue1Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue1Argument2);
            }
        }
    }

    @Test
    @DisplayName("리스 만료 정각의 재꺼내기와 정산 경합은 직렬 순서 중 하나와 일치한다 — check 4")
    void theReFetchAndSettlementRaceAtTheExactTimeOfLeaseExpirationCorrespondToOneOfTheSerialOrdersCheck4()
            throws Exception {
        final PullResult old;
        final CountDownLatch start;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        old = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<List<Result>> settle;
            final Future<PullResult> pull;
            final Result settled;
            final PullResult repulled;
            settle = executor.submit(() -> {
                start.await();
                return this.storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            pull = executor.submit(() -> {
                start.await();
                return this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            settled = settle.get(5L, TimeUnit.SECONDS).getFirst();
            repulled = pull.get(5L, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                final var assertionValue5Argument1 = repulled.messages().isEmpty();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue4Argument2);
            } else {
                final var assertionValue3Argument2 = settled.outcome();
                evaluatePmdArgument(assertionValue3Argument2);
                final var assertionValue2Argument1 = old.messages().getFirst().id();
                final var assertionValue2Argument2 =
                        repulled.messages().getFirst().id();
                Assertions.assertEquals(
                        assertionValue2Argument1,
                        assertionValue2Argument2,
                        "Message ID should match the expected value.");
                final var assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
                evaluatePmdArgument(assertionValue1Argument1);
                final var assertionValue1Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue1Argument2);
            }
        }
    }

    @Test
    @DisplayName("리스 만료 정각의 재꺼내기와 정산 경합은 직렬 순서 중 하나와 일치한다 — check 5")
    void theReFetchAndSettlementRaceAtTheExactTimeOfLeaseExpirationCorrespondToOneOfTheSerialOrdersCheck5()
            throws Exception {
        final PullResult old;
        final CountDownLatch start;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        old = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<List<Result>> settle;
            final Future<PullResult> pull;
            final Result settled;
            final PullResult repulled;
            settle = executor.submit(() -> {
                start.await();
                return this.storage.settle(
                        old.token(),
                        List.of(new Disposition(old.messages().getFirst().id(), Kind.ACK)),
                        InstantSource.fixed(T0.plus(LEASE)));
            });
            pull = executor.submit(() -> {
                start.await();
                return this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
            });
            start.countDown();
            settled = settle.get(5L, TimeUnit.SECONDS).getFirst();
            repulled = pull.get(5L, TimeUnit.SECONDS);
            if (settled.outcome() == Outcome.OK) {
                final var assertionValue5Argument1 = repulled.messages().isEmpty();
                evaluatePmdArgument(assertionValue5Argument1);
                final var assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = this.storage.counts("q");
                evaluatePmdArgument(assertionValue4Argument2);
            } else {
                final var assertionValue3Argument2 = settled.outcome();
                evaluatePmdArgument(assertionValue3Argument2);
                final var assertionValue2Argument1 = old.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument1);
                final var assertionValue2Argument2 =
                        repulled.messages().getFirst().id();
                evaluatePmdArgument(assertionValue2Argument2);
                final var assertionValue1Argument1 = new QueueCounts(0L, 1L, 0L);
                final var assertionValue1Argument2 = this.storage.counts("q");
                Assertions.assertEquals(
                        assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
            }
        }
    }

    @Test
    @DisplayName("lock 대기 마감이 끝나면 꺼내기는 상태를 바꾸지 않는다 — check 1")
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    void onceTheLockWaitTimeoutExpiresTheFetchOperationDoesNotChangeTheState() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<?> holder;
            final Future<PullResult> pull;
            final PullResult result;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue3Argument1 = locked.await(5L, TimeUnit.SECONDS);
            Assertions.assertTrue(assertionValue3Argument1, "The operation should complete before the timeout.");
            pull = executor.submit(
                    () -> this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofMillis(50L)));
            try {
                result = pull.get(1L, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue2Argument1 = result.messages().isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock 대기 마감이 끝나면 꺼내기는 상태를 바꾸지 않는다 — check 2")
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    void onceTheLockWaitTimeoutExpiresTheFetchOperationDoesNotChangeTheStateCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<?> holder;
            final Future<PullResult> pull;
            final PullResult result;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue3Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue3Argument1);
            pull = executor.submit(
                    () -> this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofMillis(50L)));
            try {
                result = pull.get(1L, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue2Argument1 = result.messages().isEmpty();
            Assertions.assertTrue(assertionValue2Argument1, "Messages should be empty.");
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock 대기 마감이 끝나면 꺼내기는 상태를 바꾸지 않는다 — check 3")
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    void onceTheLockWaitTimeoutExpiresTheFetchOperationDoesNotChangeTheStateCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<?> holder;
            final Future<PullResult> pull;
            final PullResult result;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue3Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue3Argument1);
            pull = executor.submit(
                    () -> this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofMillis(50L)));
            try {
                result = pull.get(1L, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue2Argument1 = result.messages().isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            final var assertionValue1Argument2 = this.storage.counts("q");
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 1")
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
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeased() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread pull;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue5Argument1 = locked.await(5L, TimeUnit.SECONDS);
            Assertions.assertTrue(assertionValue5Argument1, "The operation should complete before the timeout.");
            pull = new Thread(() -> {
                try {
                    this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30L));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                pull.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (pull.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                final var assertionValue4Argument2 = pull.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                pull.interrupt();
                pull.join(5000L);
                final var assertionValue3Argument1 = pull.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument2 = failure.get();
                evaluatePmdArgument(assertionValue2Argument2);
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 2")
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
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck2() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread pull;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue5Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue5Argument1);
            pull = new Thread(() -> {
                try {
                    this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30L));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                pull.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (pull.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                final var assertionValue4Argument2 = pull.getState();
                Assertions.assertEquals(
                        State.TIMED_WAITING, assertionValue4Argument2, "State should match the expected value.");
                pull.interrupt();
                pull.join(5000L);
                final var assertionValue3Argument1 = pull.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument2 = failure.get();
                evaluatePmdArgument(assertionValue2Argument2);
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 3")
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
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck3() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread pull;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue5Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue5Argument1);
            pull = new Thread(() -> {
                try {
                    this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30L));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                pull.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (pull.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                final var assertionValue4Argument2 = pull.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                pull.interrupt();
                pull.join(5000L);
                final var assertionValue3Argument1 = pull.isAlive();
                Assertions.assertFalse(assertionValue3Argument1, "Pull should not be alive.");
                final var assertionValue2Argument2 = failure.get();
                evaluatePmdArgument(assertionValue2Argument2);
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 4")
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
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck4() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread pull;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue5Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue5Argument1);
            pull = new Thread(() -> {
                try {
                    this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30L));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                pull.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (pull.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                final var assertionValue4Argument2 = pull.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                pull.interrupt();
                pull.join(5000L);
                final var assertionValue3Argument1 = pull.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument2 = failure.get();
                Assertions.assertInstanceOf(
                        InterruptedException.class,
                        assertionValue2Argument2,
                        "Value should be an instance of InterruptedException.");
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = this.storage.counts("q");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("lock을 기다리는 꺼내기를 interrupt하면 리스하지 않는다 — check 5")
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
    void ifAFetchOperationWaitingForALockIsInterruptedItWillNotBeLeasedCheck5() throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicReference<Throwable> failure;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> holder;
            final Thread pull;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            final var assertionValue5Argument1 = locked.await(5L, TimeUnit.SECONDS);
            evaluatePmdArgument(assertionValue5Argument1);
            pull = new Thread(() -> {
                try {
                    this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0), Duration.ofSeconds(30L));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            try {
                final long deadline;
                pull.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
                while (pull.getState() != State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                final var assertionValue4Argument2 = pull.getState();
                evaluatePmdArgument(assertionValue4Argument2);
                pull.interrupt();
                pull.join(5000L);
                final var assertionValue3Argument1 = pull.isAlive();
                evaluatePmdArgument(assertionValue3Argument1);
                final var assertionValue2Argument2 = failure.get();
                evaluatePmdArgument(assertionValue2Argument2);
            } finally {
                pull.interrupt();
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            final var assertionValue1Argument1 = new QueueCounts(1L, 0L, 0L);
            final var assertionValue1Argument2 = this.storage.counts("q");
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
        }
    }

    @Test
    @DisplayName("정산은 lock 안에서 시각을 읽는다 — check 1")
    void settlementReadsTheTimestampWithinTheLockCheck1() throws Exception {
        final MessageId id;
        final PullResult pulled;
        final boolean assertionValue1Argument1;
        this.storage.declare("q", QueueOptions.defaults());
        id = this.storage
                .publish("q", List.of(message(1)), InstantSource.fixed(T0))
                .getFirst();
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        this.afterLockWait(clock -> this.storage.settle(
                pulled.token(), List.of(new Disposition(id, Kind.NACK, Duration.ofSeconds(5L))), clock));
        assertionValue1Argument1 = this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64L)))
                .messages()
                .isEmpty();
        Assertions.assertTrue(assertionValue1Argument1, "Messages should be empty.");
        evaluate(this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65L)))
                .messages()
                .getFirst()
                .id());
    }

    @Test
    @DisplayName("정산은 lock 안에서 시각을 읽는다 — check 2")
    void settlementReadsTheTimestampWithinTheLockCheck2() throws Exception {
        final MessageId id;
        final PullResult pulled;
        final MessageId assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        id = this.storage
                .publish("q", List.of(message(1)), InstantSource.fixed(T0))
                .getFirst();
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        this.afterLockWait(clock -> this.storage.settle(
                pulled.token(), List.of(new Disposition(id, Kind.NACK, Duration.ofSeconds(5L))), clock));
        evaluate(this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(64L)))
                .messages()
                .isEmpty());
        assertionValue2Argument2 = this.storage
                .pull("q", 10, LEASE, InstantSource.fixed(T0.plusSeconds(65L)))
                .messages()
                .getFirst()
                .id();
        Assertions.assertEquals(id, assertionValue2Argument2, "Message ID should match the expected value.");
    }

    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    private <T> T afterLockWait(final Function<InstantSource, T> operation) throws Exception {
        final CountDownLatch locked;
        final CountDownLatch release;
        final AtomicInteger reads;
        locked = new CountDownLatch(1);
        release = new CountDownLatch(1);
        reads = new AtomicInteger();
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<?> holder;
            final CountDownLatch started;
            final Future<T> result;
            final Object value;
            holder = executor.submit(() -> this.storage.publish("q", List.of(), () -> {
                locked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return T0;
            }));
            Assertions.assertTrue(
                    locked.await(5L, TimeUnit.SECONDS), "The operation should complete before the timeout.");
            started = new CountDownLatch(1);
            result = executor.submit(() -> {
                started.countDown();
                return operation.apply(() -> {
                    reads.incrementAndGet();
                    return T0.plusSeconds(60L);
                });
            });
            try {
                Assertions.assertTrue(
                        started.await(5L, TimeUnit.SECONDS), "The operation should complete before the timeout.");
                Assertions.assertThrows(
                        TimeoutException.class,
                        () -> result.get(100L, TimeUnit.MILLISECONDS),
                        "Get should throw TimeoutException.");
                Assertions.assertEquals(0, reads.get(), "Reads should match the expected value.");
            } finally {
                release.countDown();
            }
            holder.get(5L, TimeUnit.SECONDS);
            value = result.get(5L, TimeUnit.SECONDS);
            Assertions.assertTrue(reads.get() > 0, "Reads should be greater than 0.");
            return (T) value;
        }
    }

    @Test
    @DisplayName("발행은 저장소가 읽은 시각을 사용한다")
    void publishUsesTheTimeReadByStorage() {
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        Assertions.assertEquals(
                T0,
                this.storage
                        .pull("q", 1, LEASE, InstantSource.fixed(T0))
                        .messages()
                        .getFirst()
                        .publishedAt(),
                "Published at should match the expected value.");
    }

    @Test
    @DisplayName("꺼내기가 정확히 상한만큼 옮기면 모두 옮기고 남았다고 하지 않는다 — exact Move Returns No Messages")
    void exactMoveReturnsNoMessages() {
        final int cap;
        final PullResult exact;
        cap = 100;
        exact = this.pullAfterExhausting("exact", cap);
        Assertions.assertTrue(exact.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("꺼내기가 정확히 상한만큼 옮기면 모두 옮기고 남았다고 하지 않는다 — exact Move Reports No Remaining Dead Letters")
    void exactMoveReportsNoRemainingDeadLetters() {
        final int cap;
        final PullResult exact;
        cap = 100;
        exact = this.pullAfterExhausting("exact", cap);
        Assertions.assertFalse(exact.deadLettersRemain(), "No dead letters should remain.");
    }

    @Test
    @DisplayName("꺼내기가 정확히 상한만큼 옮기면 모두 옮기고 남았다고 하지 않는다 — exact Move Reports Moved Dead Letters")
    void exactMoveReportsMovedDeadLetters() {
        final int cap;
        final PullResult exact;
        cap = 100;
        exact = this.pullAfterExhausting("exact", cap);
        Assertions.assertTrue(exact.deadLettersMoved(), "Dead letters should be moved.");
    }

    @Test
    @DisplayName("꺼내기가 정확히 상한만큼 옮기면 모두 옮기고 남았다고 하지 않는다 — exact Move Updates Dead Queue Count")
    void exactMoveUpdatesDeadQueueCount() {
        final int cap;
        cap = 100;
        this.pullAfterExhausting("exact", cap);
        Assertions.assertEquals(
                cap, this.storage.counts("dead-exact").ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("꺼내기가 정확히 상한만큼 옮기면 모두 옮기고 남았다고 하지 않는다 — exact Move Empties Source Queue")
    void exactMoveEmptiesSourceQueue() {
        final int cap;
        cap = 100;
        this.pullAfterExhausting("exact", cap);
        Assertions.assertEquals(0L, this.storage.counts("exact").ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("상한보다 하나 많으면 상한만큼 옮기고 남았다고 알린다 — over Limit Move Returns No Messages")
    void overLimitMoveReturnsNoMessages() {
        final int cap;
        final PullResult over;
        cap = 100;
        over = this.pullAfterExhausting("over", cap + 1);
        Assertions.assertTrue(over.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("상한보다 하나 많으면 상한만큼 옮기고 남았다고 알린다 — over Limit Move Reports Remaining Dead Letters")
    void overLimitMoveReportsRemainingDeadLetters() {
        final int cap;
        final PullResult over;
        cap = 100;
        over = this.pullAfterExhausting("over", cap + 1);
        Assertions.assertTrue(over.deadLettersRemain(), "Dead letters should remain.");
    }

    @Test
    @DisplayName("상한보다 하나 많으면 상한만큼 옮기고 남았다고 알린다 — over Limit Move Reports Moved Dead Letters")
    void overLimitMoveReportsMovedDeadLetters() {
        final int cap;
        final PullResult over;
        cap = 100;
        over = this.pullAfterExhausting("over", cap + 1);
        Assertions.assertTrue(over.deadLettersMoved(), "Dead letters should be moved.");
    }

    @Test
    @DisplayName("상한보다 하나 많으면 상한만큼 옮기고 남았다고 알린다 — over Limit Move Updates Dead Queue Count")
    void overLimitMoveUpdatesDeadQueueCount() {
        final int cap;
        cap = 100;
        this.pullAfterExhausting("over", cap + 1);
        Assertions.assertEquals(
                cap, this.storage.counts("dead-over").ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("남았다는 표시 뒤의 다음 꺼내기가 나머지를 옮긴다 — remaining Move Returns No Messages")
    void remainingMoveReturnsNoMessages() {
        final int cap;
        final PullResult next;
        cap = 100;
        this.pullAfterExhausting("over", cap + 1);
        next = this.storage.pull("over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        Assertions.assertTrue(next.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("남았다는 표시 뒤의 다음 꺼내기가 나머지를 옮긴다 — remaining Move Reports No Remaining Dead Letters")
    void remainingMoveReportsNoRemainingDeadLetters() {
        final int cap;
        final PullResult next;
        cap = 100;
        this.pullAfterExhausting("over", cap + 1);
        next = this.storage.pull("over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        Assertions.assertFalse(next.deadLettersRemain(), "No dead letters should remain.");
    }

    @Test
    @DisplayName("남았다는 표시 뒤의 다음 꺼내기가 나머지를 옮긴다 — remaining Move Transfers All Dead Letters")
    void remainingMoveTransfersAllDeadLetters() {
        final int cap;
        cap = 100;
        this.pullAfterExhausting("over", cap + 1);
        this.storage.pull("over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        Assertions.assertEquals(
                cap + 1, this.storage.counts("dead-over").ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("메시지를 꺼내면서 이동하면 이동 표시가 참이다 — check 1")
    void ifYouMoveWhileTakingOutAMessageTheMoveIndicatorIsTrueCheck1() {
        final boolean assertionValue1Argument1;
        final PullResult result;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        assertionValue1Argument1 =
                this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).deadLettersMoved();
        Assertions.assertFalse(assertionValue1Argument1, "No dead letters should be moved.");
        this.storage.publish("q", List.of(message(2)), InstantSource.fixed(T0));
        result = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        evaluate(result.deadLettersMoved());
    }

    @Test
    @DisplayName("메시지를 꺼내면서 이동하면 이동 표시가 참이다 — check 2")
    void ifYouMoveWhileTakingOutAMessageTheMoveIndicatorIsTrueCheck2() {
        final PullResult result;
        final int assertionValue2Argument2;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).deadLettersMoved());
        this.storage.publish("q", List.of(message(2)), InstantSource.fixed(T0));
        result = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertionValue2Argument2 = result.messages().size();
        Assertions.assertEquals(1, assertionValue2Argument2, "Size should match the expected value.");
        evaluate(result.deadLettersMoved());
    }

    @Test
    @DisplayName("메시지를 꺼내면서 이동하면 이동 표시가 참이다 — check 3")
    void ifYouMoveWhileTakingOutAMessageTheMoveIndicatorIsTrueCheck3() {
        final PullResult result;
        final boolean assertionValue3Argument1;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).deadLettersMoved());
        this.storage.publish("q", List.of(message(2)), InstantSource.fixed(T0));
        result = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertionValue3Argument1 = result.deadLettersMoved();
        Assertions.assertTrue(assertionValue3Argument1, "Dead letters should be moved.");
    }

    @Test
    @DisplayName("옮겨진 메시지는 새 메시지라 전달 횟수를 처음부터 센다")
    void sinceTheMovedMessageIsANewMessageTheNumberOfTimesItIsForwardedIsCountedFromTheBeginning() {
        final Message moved;
        this.pullAfterExhausting("over", 101);
        moved = this.storage
                .pull("dead-over", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .messages()
                .getFirst();
        Assertions.assertEquals(1, moved.deliveryCount(), "Delivery count should match the expected value.");
    }

    @Test
    @DisplayName("발행 배치 중간에 실패하면 배치 전체가 반영되지 않는다 — check 1")
    void failedPublishBatchIsNotAppliedCheck1() {
        final NewMessage ok;
        final NewMessage bad;
        final Throwable assertionValue1;
        this.storage.declare("q", QueueOptions.defaults());
        ok = message(1);
        bad = new NewMessage(null, Map.of());
        assertionValue1 = captureThrowable(() -> this.storage.publish("q", List.of(ok, bad), InstantSource.fixed(T0)));
        Assertions.assertInstanceOf(RuntimeException.class, assertionValue1, "Publish should throw RuntimeException.");
    }

    @Test
    @DisplayName("발행 배치 중간에 실패하면 배치 전체가 반영되지 않는다 — check 2")
    void failedPublishBatchIsNotAppliedCheck2() {
        final NewMessage ok;
        final NewMessage bad;
        final long assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        ok = message(1);
        bad = new NewMessage(null, Map.of());
        evaluate(captureThrowable(() -> this.storage.publish("q", List.of(ok, bad), InstantSource.fixed(T0))));
        assertionValue2Argument2 = this.storage.counts("q").ready();
        Assertions.assertEquals(0L, assertionValue2Argument2, "Ready should match the expected value.");
    }

    @Test
    @DisplayName("빈 꺼내기는 토큰과 기한이 비고 남았다는 표시가 거짓이다 — empty Pull Has No Messages")
    void emptyPullHasNoMessages() {
        final PullResult empty;
        this.storage.declare("q", QueueOptions.defaults());
        empty = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertTrue(empty.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("빈 꺼내기는 토큰과 기한이 비고 남았다는 표시가 거짓이다 — empty Pull Has No Token")
    void emptyPullHasNoToken() {
        final PullResult empty;
        this.storage.declare("q", QueueOptions.defaults());
        empty = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertNull(empty.token(), "Token should be null.");
    }

    @Test
    @DisplayName("빈 꺼내기는 토큰과 기한이 비고 남았다는 표시가 거짓이다 — empty Pull Has No Deadline")
    void emptyPullHasNoDeadline() {
        final PullResult empty;
        this.storage.declare("q", QueueOptions.defaults());
        empty = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertNull(empty.leaseDeadline(), "Lease deadline should be null.");
    }

    @Test
    @DisplayName("빈 꺼내기는 토큰과 기한이 비고 남았다는 표시가 거짓이다 — empty Pull Has No Remaining Dead Letters")
    void emptyPullHasNoRemainingDeadLetters() {
        final PullResult empty;
        this.storage.declare("q", QueueOptions.defaults());
        empty = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertFalse(empty.deadLettersRemain(), "No dead letters should remain.");
    }

    @Test
    @DisplayName("빈 꺼내기는 토큰과 기한이 비고 남았다는 표시가 거짓이다 — empty Pull Has No Moved Dead Letters")
    void emptyPullHasNoMovedDeadLetters() {
        final PullResult empty;
        this.storage.declare("q", QueueOptions.defaults());
        empty = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertFalse(empty.deadLettersMoved(), "No dead letters should be moved.");
    }

    @Test
    @DisplayName("꺼낼 때마다 새 리스 토큰이 나온다 — first Pull Has Lease Token")
    void firstPullHasLeaseToken() {
        final LeaseToken first;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        Assertions.assertNotNull(first, "First should be present.");
    }

    @Test
    @DisplayName("꺼낼 때마다 새 리스 토큰이 나온다 — second Pull Has Lease Token")
    void secondPullHasLeaseToken() {
        final LeaseToken second;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        Assertions.assertNotNull(second, "Second should be present.");
    }

    @Test
    @DisplayName("꺼낼 때마다 새 리스 토큰이 나온다 — consecutive Pulls Have Distinct Lease Tokens")
    void consecutivePullsHaveDistinctLeaseTokens() {
        final LeaseToken first;
        final LeaseToken second;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        Assertions.assertNotEquals(first, second, "Second should differ from the unexpected value.");
    }

    @Test
    @DisplayName("리스가 만료된 뒤 같은 큐에서 다시 꺼내도 새 리스 토큰이 나온다 — expired Message Is Redelivered")
    void expiredMessageIsRedelivered() {
        final PullResult again;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        again = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        Assertions.assertEquals(1, again.messages().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("리스가 만료된 뒤 같은 큐에서 다시 꺼내도 새 리스 토큰이 나온다 — expired Message Receives New Lease Token")
    void expiredMessageReceivesNewLeaseToken() {
        final LeaseToken first;
        final PullResult again;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0)).token();
        again = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        Assertions.assertNotEquals(first, again.token(), "Token should differ from the unexpected value.");
    }

    @Test
    @DisplayName("리스 기한 정확히에는 다시 꺼낼 수 있고 1ns 앞에는 꺼낼 수 없다 — lease Cannot Be Pulled Before Deadline")
    void leaseCannotBePulledBeforeDeadline() {
        final Instant deadline;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        deadline = T0.plus(LEASE);
        Assertions.assertTrue(
                this.storage
                        .pull("q", 1, LEASE, InstantSource.fixed(deadline.minusNanos(1L)))
                        .messages()
                        .isEmpty(),
                "Messages should be empty.");
    }

    @Test
    @DisplayName("리스 기한 정확히에는 다시 꺼낼 수 있고 1ns 앞에는 꺼낼 수 없다 — lease Can Be Pulled At Deadline")
    void leaseCanBePulledAtDeadline() {
        final Instant deadline;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        deadline = T0.plus(LEASE);
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(deadline.minusNanos(1L)));
        Assertions.assertEquals(
                1,
                this.storage
                        .pull("q", 1, LEASE, InstantSource.fixed(deadline))
                        .messages()
                        .size(),
                "Size should match the expected value.");
    }

    @Test
    @DisplayName("지연 시각 정확히에는 꺼낼 수 있고 1ns 앞에는 꺼낼 수 없다 — delayed Message Cannot Be Pulled Before Deadline")
    void delayedMessageCannotBePulledBeforeDeadline() {
        final Duration delay;
        final Instant availableAt;
        this.storage.declare("q", QueueOptions.defaults());
        delay = Duration.ofSeconds(5L);
        this.storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), delay)), InstantSource.fixed(T0));
        availableAt = T0.plus(delay);
        Assertions.assertTrue(
                this.storage
                        .pull("q", 1, LEASE, InstantSource.fixed(availableAt.minusNanos(1L)))
                        .messages()
                        .isEmpty(),
                "Messages should be empty.");
    }

    @Test
    @DisplayName("지연 시각 정확히에는 꺼낼 수 있고 1ns 앞에는 꺼낼 수 없다 — delayed Message Can Be Pulled At Deadline")
    void delayedMessageCanBePulledAtDeadline() {
        final Duration delay;
        final Instant availableAt;
        this.storage.declare("q", QueueOptions.defaults());
        delay = Duration.ofSeconds(5L);
        this.storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), delay)), InstantSource.fixed(T0));
        availableAt = T0.plus(delay);
        this.storage.pull("q", 1, LEASE, InstantSource.fixed(availableAt.minusNanos(1L)));
        Assertions.assertEquals(
                1,
                this.storage
                        .pull("q", 1, LEASE, InstantSource.fixed(availableAt))
                        .messages()
                        .size(),
                "Size should match the expected value.");
    }

    @Test
    @DisplayName("메시지가 전부 낡은 연장도 기한은 now 더하기 d이고 메시지별 결과는 STALE이다 — stale Extension Reports Calculated Deadline")
    void staleExtensionReportsCalculatedDeadline() {
        final PullResult pulled;
        final MessageId id;
        final Instant now;
        final Duration d;
        final Extended extended;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        this.storage.settle(pulled.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(T0));
        now = T0.plusSeconds(1L);
        d = Duration.ofSeconds(30L);
        extended = this.storage.extend(pulled.token(), List.of(id), d, InstantSource.fixed(now));
        Assertions.assertEquals(
                now.plus(d), extended.leaseDeadline(), "Lease deadline should match the expected value.");
    }

    @Test
    @DisplayName("메시지가 전부 낡은 연장도 기한은 now 더하기 d이고 메시지별 결과는 STALE이다 — stale Extension Reports Stale Results")
    void staleExtensionReportsStaleResults() {
        final PullResult pulled;
        final MessageId id;
        final Instant now;
        final Duration d;
        final Extended extended;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        this.storage.settle(pulled.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(T0));
        now = T0.plusSeconds(1L);
        d = Duration.ofSeconds(30L);
        extended = this.storage.extend(pulled.token(), List.of(id), d, InstantSource.fixed(now));
        Assertions.assertEquals(
                List.of(new Result(id, Outcome.STALE)), extended.results(), "Results should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐에서 다른 청크가 꺼낸 뒤 늦게 도착한 연장은 만료된 리스를 되살리지 못한다 — check 1")
    void
            inASingleActiveQueueAnExtensionThatArrivesLateAfterAnotherChunkHasBeenDequeuedCannotReviveAnExpiredLeaseCheck1() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final PullResult second;
        final MessageId assertionValue1Argument2;
        final Extended late;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        assertionValue1Argument2 = second.messages().getFirst().id();
        Assertions.assertEquals(a, assertionValue1Argument2, "Message ID should match the expected value.");
        late = this.storage.extend(first.token(), List.of(a, b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(List.of(new Result(a, Outcome.STALE), new Result(b, Outcome.STALE)));
        evaluate(late.results());
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(this.storage.pull(
                "q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE).plus(LEASE))));
    }

    @Test
    @DisplayName("단일 활성 큐에서 다른 청크가 꺼낸 뒤 늦게 도착한 연장은 만료된 리스를 되살리지 못한다 — check 2")
    void
            inASingleActiveQueueAnExtensionThatArrivesLateAfterAnotherChunkHasBeenDequeuedCannotReviveAnExpiredLeaseCheck2() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final Extended late;
        final List<Result> assertionValue2Argument1;
        final List<Result> assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE))));
        late = this.storage.extend(first.token(), List.of(a, b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        assertionValue2Argument1 = List.of(new Result(a, Outcome.STALE), new Result(b, Outcome.STALE));
        assertionValue2Argument2 = late.results();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Results should match the expected value.");
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(this.storage.pull(
                "q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE).plus(LEASE))));
    }

    @Test
    @DisplayName("단일 활성 큐에서 다른 청크가 꺼낸 뒤 늦게 도착한 연장은 만료된 리스를 되살리지 못한다 — check 3")
    void
            inASingleActiveQueueAnExtensionThatArrivesLateAfterAnotherChunkHasBeenDequeuedCannotReviveAnExpiredLeaseCheck3() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final Extended late;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE))));
        late = this.storage.extend(first.token(), List.of(a, b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(List.of(new Result(a, Outcome.STALE), new Result(b, Outcome.STALE)));
        evaluate(late.results());
        assertionValue3Argument1 = new QueueCounts(0L, 2L, 0L);
        assertionValue3Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(this.storage.pull(
                "q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE).plus(LEASE))));
    }

    @Test
    @DisplayName("단일 활성 큐에서 다른 청크가 꺼낸 뒤 늦게 도착한 연장은 만료된 리스를 되살리지 못한다 — check 4")
    void
            inASingleActiveQueueAnExtensionThatArrivesLateAfterAnotherChunkHasBeenDequeuedCannotReviveAnExpiredLeaseCheck4() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final Extended late;
        final PullResult third;
        final List<MessageId> assertionValue4Argument1;
        final List<MessageId> assertionValue4Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE))));
        late = this.storage.extend(first.token(), List.of(a, b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(List.of(new Result(a, Outcome.STALE), new Result(b, Outcome.STALE)));
        evaluate(late.results());
        evaluate(new QueueCounts(0L, 2L, 0L));
        third = this.storage.pull(
                "q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE).plus(LEASE)));
        assertionValue4Argument1 = List.of(a, b);
        assertionValue4Argument2 = third.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐에서 둘째 청크가 정산한 뒤에 도착한 연장도 만료된 리스를 되살리지 못한다 — check 1")
    void
            anExtensionThatArrivesAfterTheSecondChunkHasBeenSettledInASingleActiveQueueAlsoCannotReviveAnExpiredLeaseCheck1() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final PullResult second;
        final Extended late;
        final List<Result> assertionValue1Argument1;
        final List<Result> assertionValue1Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        this.storage.settle(
                second.token(), List.of(new Disposition(a, Kind.ACK)), InstantSource.fixed(T0.plusSeconds(11L)));
        late = this.storage.extend(first.token(), List.of(b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        assertionValue1Argument1 = List.of(new Result(b, Outcome.STALE));
        assertionValue1Argument2 = late.results();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Results should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L))));
    }

    @Test
    @DisplayName("단일 활성 큐에서 둘째 청크가 정산한 뒤에 도착한 연장도 만료된 리스를 되살리지 못한다 — check 2")
    void
            anExtensionThatArrivesAfterTheSecondChunkHasBeenSettledInASingleActiveQueueAlsoCannotReviveAnExpiredLeaseCheck2() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final PullResult second;
        final Extended late;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        this.storage.settle(
                second.token(), List.of(new Disposition(a, Kind.ACK)), InstantSource.fixed(T0.plusSeconds(11L)));
        late = this.storage.extend(first.token(), List.of(b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(List.of(new Result(b, Outcome.STALE)));
        evaluate(late.results());
        assertionValue2Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue2Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L))));
    }

    @Test
    @DisplayName("단일 활성 큐에서 둘째 청크가 정산한 뒤에 도착한 연장도 만료된 리스를 되살리지 못한다 — check 3")
    void
            anExtensionThatArrivesAfterTheSecondChunkHasBeenSettledInASingleActiveQueueAlsoCannotReviveAnExpiredLeaseCheck3() {
        final PullResult first;
        final MessageId a;
        final MessageId b;
        final PullResult second;
        final Extended late;
        final PullResult third;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        a = first.messages().get(0).id();
        b = first.messages().get(1).id();
        second = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
        this.storage.settle(
                second.token(), List.of(new Disposition(a, Kind.ACK)), InstantSource.fixed(T0.plusSeconds(11L)));
        late = this.storage.extend(first.token(), List.of(b), LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(List.of(new Result(b, Outcome.STALE)));
        evaluate(late.results());
        evaluate(new QueueCounts(0L, 1L, 0L));
        third = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L)));
        assertionValue3Argument1 = List.of(b);
        assertionValue3Argument2 = third.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐에서 상한에 막혀 옮겨지지 못한 소진 메시지도 늦은 연장이 되살리지 못한다 — check 1")
    void
            evenExpiredMessagesThatCouldNotBeMovedDueToReachingTheUpperLimitInASingleActiveQueueCannotBeRevivedByALateExtensionCheck1() {
        final int cap;
        final List<NewMessage> batch;
        final PullResult first;
        final List<MessageId> ids;
        final boolean assertionValue1Argument1;
        final Extended late;
        final PullResult third;
        cap = 100;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare(
                "q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        batch = new ArrayList<>();
        for (int i = 0; i < cap + 1; i++) {
            batch.add(message(i));
        }
        this.storage.publish("q", batch, InstantSource.fixed(T0));
        first = this.storage.pull("q", cap + 1, LEASE, InstantSource.fixed(T0));
        ids = first.messages().stream().map(Message::id).toList();
        assertionValue1Argument1 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .deadLettersRemain();
        Assertions.assertTrue(assertionValue1Argument1, "Dead letters should remain.");
        late = this.storage.extend(first.token(), ids, LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(late.results().stream().allMatch(r -> r.outcome() == Outcome.STALE));
        third = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L)));
        evaluate(third.deadLettersRemain());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("단일 활성 큐에서 상한에 막혀 옮겨지지 못한 소진 메시지도 늦은 연장이 되살리지 못한다 — check 2")
    void
            evenExpiredMessagesThatCouldNotBeMovedDueToReachingTheUpperLimitInASingleActiveQueueCannotBeRevivedByALateExtensionCheck2() {
        final int cap;
        final List<NewMessage> batch;
        final PullResult first;
        final List<MessageId> ids;
        final Extended late;
        final boolean assertionValue2Argument1;
        final PullResult third;
        cap = 100;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare(
                "q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        batch = new ArrayList<>();
        for (int i = 0; i < cap + 1; i++) {
            batch.add(message(i));
        }
        this.storage.publish("q", batch, InstantSource.fixed(T0));
        first = this.storage.pull("q", cap + 1, LEASE, InstantSource.fixed(T0));
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .deadLettersRemain());
        late = this.storage.extend(first.token(), ids, LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        assertionValue2Argument1 = late.results().stream().allMatch(r -> r.outcome() == Outcome.STALE);
        Assertions.assertTrue(assertionValue2Argument1, "All late settlements should be stale.");
        third = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L)));
        evaluate(third.deadLettersRemain());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("단일 활성 큐에서 상한에 막혀 옮겨지지 못한 소진 메시지도 늦은 연장이 되살리지 못한다 — check 3")
    void
            evenExpiredMessagesThatCouldNotBeMovedDueToReachingTheUpperLimitInASingleActiveQueueCannotBeRevivedByALateExtensionCheck3() {
        final int cap;
        final List<NewMessage> batch;
        final PullResult first;
        final List<MessageId> ids;
        final Extended late;
        final PullResult third;
        final boolean assertionValue3Argument1;
        cap = 100;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare(
                "q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        batch = new ArrayList<>();
        for (int i = 0; i < cap + 1; i++) {
            batch.add(message(i));
        }
        this.storage.publish("q", batch, InstantSource.fixed(T0));
        first = this.storage.pull("q", cap + 1, LEASE, InstantSource.fixed(T0));
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .deadLettersRemain());
        late = this.storage.extend(first.token(), ids, LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(late.results().stream().allMatch(r -> r.outcome() == Outcome.STALE));
        third = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L)));
        assertionValue3Argument1 = third.deadLettersRemain();
        Assertions.assertFalse(assertionValue3Argument1, "No dead letters should remain.");
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("단일 활성 큐에서 상한에 막혀 옮겨지지 못한 소진 메시지도 늦은 연장이 되살리지 못한다 — check 4")
    void
            evenExpiredMessagesThatCouldNotBeMovedDueToReachingTheUpperLimitInASingleActiveQueueCannotBeRevivedByALateExtensionCheck4() {
        final int cap;
        final List<NewMessage> batch;
        final PullResult first;
        final List<MessageId> ids;
        final Extended late;
        final PullResult third;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        cap = 100;
        this.storage.declare("dead", QueueOptions.defaults());
        this.storage.declare(
                "q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        batch = new ArrayList<>();
        for (int i = 0; i < cap + 1; i++) {
            batch.add(message(i));
        }
        this.storage.publish("q", batch, InstantSource.fixed(T0));
        first = this.storage.pull("q", cap + 1, LEASE, InstantSource.fixed(T0));
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .deadLettersRemain());
        late = this.storage.extend(first.token(), ids, LEASE, InstantSource.fixed(T0.plusSeconds(12L)));
        evaluate(late.results().stream().allMatch(r -> r.outcome() == Outcome.STALE));
        third = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0.plusSeconds(15L)));
        evaluate(third.deadLettersRemain());
        assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue4Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("일반 큐는 기한이 지났어도 아무도 다시 꺼내지 않았다면 연장이 OK이고 리스를 되살린다 — expired General Queue Extension Succeeds")
    void expiredGeneralQueueExtensionSucceeds() {
        final PullResult pulled;
        final MessageId id;
        final Instant now;
        final Extended extended;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        now = T0.plus(LEASE).plusSeconds(5L);
        extended = this.storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(now));
        Assertions.assertEquals(
                List.of(new Result(id, Outcome.OK)), extended.results(), "Results should match the expected value.");
    }

    @Test
    @DisplayName("일반 큐는 기한이 지났어도 아무도 다시 꺼내지 않았다면 연장이 OK이고 리스를 되살린다 — expired General Queue Extension Restores Lease")
    void expiredGeneralQueueExtensionRestoresLease() {
        final PullResult pulled;
        final MessageId id;
        final Instant now;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        now = T0.plus(LEASE).plusSeconds(5L);
        this.storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(now));
        Assertions.assertEquals(1L, this.storage.counts("q").leased(), "Leased should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐의 연장은 기한 정확히에는 STALE이고 1ns 앞에는 OK다 — single Active Extension Succeeds Just Before Deadline")
    void singleActiveExtensionSucceedsJustBeforeDeadline() {
        final PullResult pulled;
        final MessageId id;
        final Instant justBefore;
        final Extended before;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        justBefore = T0.plus(LEASE).minusNanos(1L);
        before = this.storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(justBefore));
        Assertions.assertEquals(
                List.of(new Result(id, Outcome.OK)), before.results(), "Results should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐의 연장은 기한 정확히에는 STALE이고 1ns 앞에는 OK다 — single Active Extension Is Stale At Deadline")
    void singleActiveExtensionIsStaleAtDeadline() {
        final PullResult pulled;
        final MessageId id;
        final Instant justBefore;
        final Extended before;
        final Extended exactly;
        this.storage.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = pulled.messages().getFirst().id();
        justBefore = T0.plus(LEASE).minusNanos(1L);
        before = this.storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(justBefore));
        exactly = this.storage.extend(pulled.token(), List.of(id), LEASE, InstantSource.fixed(before.leaseDeadline()));
        Assertions.assertEquals(
                List.of(new Result(id, Outcome.STALE)), exactly.results(), "Results should match the expected value.");
    }

    @Test
    @DisplayName("낡은 리스 토큰으로 정산하면 STALE이고 저장소 상태가 바뀌지 않는다 — check 1")
    void ifSettlementIsPerformedWithAnOldLeaseTokenItIsConsideredSTALEAndTheStorageStatusDoesNotChangeCheck1() {
        final PullResult first;
        final MessageId id;
        final Instant second;
        final PullResult current;
        final int assertionValue1Argument2;
        final Instant expired;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = first.messages().getFirst().id();
        second = T0.plus(LEASE);
        current = this.storage.pull("q", 1, LEASE, InstantSource.fixed(second));
        assertionValue1Argument2 = current.messages().size();
        Assertions.assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
        evaluate(this.storage.settle(
                first.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(second)));
        evaluate(List.of(new Result(id, Outcome.STALE)));
        evaluate(new QueueCounts(0L, 1L, 0L));
        expired = second.plus(LEASE);
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(expired))
                .messages()
                .size());
    }

    @Test
    @DisplayName("낡은 리스 토큰으로 정산하면 STALE이고 저장소 상태가 바뀌지 않는다 — check 2")
    void ifSettlementIsPerformedWithAnOldLeaseTokenItIsConsideredSTALEAndTheStorageStatusDoesNotChangeCheck2() {
        final PullResult first;
        final MessageId id;
        final Instant second;
        final List<Result> results;
        final List<Result> assertionValue2Argument1;
        final Instant expired;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = first.messages().getFirst().id();
        second = T0.plus(LEASE);
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(second)));
        results =
                this.storage.settle(first.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(second));
        assertionValue2Argument1 = List.of(new Result(id, Outcome.STALE));
        Assertions.assertEquals(assertionValue2Argument1, results, "Results should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        expired = second.plus(LEASE);
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(expired))
                .messages()
                .size());
    }

    @Test
    @DisplayName("낡은 리스 토큰으로 정산하면 STALE이고 저장소 상태가 바뀌지 않는다 — check 3")
    void ifSettlementIsPerformedWithAnOldLeaseTokenItIsConsideredSTALEAndTheStorageStatusDoesNotChangeCheck3() {
        final PullResult first;
        final MessageId id;
        final Instant second;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        final Instant expired;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = first.messages().getFirst().id();
        second = T0.plus(LEASE);
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(second)));
        evaluate(this.storage.settle(
                first.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(second)));
        evaluate(List.of(new Result(id, Outcome.STALE)));
        assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue3Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        expired = second.plus(LEASE);
        evaluate(this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(expired))
                .messages()
                .size());
    }

    @Test
    @DisplayName("낡은 리스 토큰으로 정산하면 STALE이고 저장소 상태가 바뀌지 않는다 — check 4")
    void ifSettlementIsPerformedWithAnOldLeaseTokenItIsConsideredSTALEAndTheStorageStatusDoesNotChangeCheck4() {
        final PullResult first;
        final MessageId id;
        final Instant second;
        final Instant expired;
        final int assertionValue4Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1)), InstantSource.fixed(T0));
        first = this.storage.pull("q", 1, LEASE, InstantSource.fixed(T0));
        id = first.messages().getFirst().id();
        second = T0.plus(LEASE);
        evaluate(this.storage.pull("q", 1, LEASE, InstantSource.fixed(second)));
        evaluate(this.storage.settle(
                first.token(), List.of(new Disposition(id, Kind.ACK)), InstantSource.fixed(second)));
        evaluate(List.of(new Result(id, Outcome.STALE)));
        evaluate(new QueueCounts(0L, 1L, 0L));
        expired = second.plus(LEASE);
        assertionValue4Argument2 = this.storage
                .pull("q", 1, LEASE, InstantSource.fixed(expired))
                .messages()
                .size();
        Assertions.assertEquals(1, assertionValue4Argument2, "Size should match the expected value.");
    }

    @Test
    @DisplayName("정산 안에 dead letter 큐 없는 reject가 있으면 정산 전체가 반영되지 않는다 — check 1")
    void ifThereIsARejectWithoutADeadLetterQueueInTheSettlementTheEntireSettlementIsNotReflectedCheck1() {
        final PullResult pulled;
        final MessageId acked;
        final MessageId rejected;
        final Throwable assertionValue1;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        acked = pulled.messages().get(0).id();
        rejected = pulled.messages().get(1).id();
        assertionValue1 = captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(new Disposition(acked, Kind.ACK), new Disposition(rejected, Kind.REJECT)),
                InstantSource.fixed(T0)));
        Assertions.assertInstanceOf(RuntimeException.class, assertionValue1, "Settle should throw RuntimeException.");
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(this.storage
                .pull("q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .messages()
                .size());
    }

    @Test
    @DisplayName("정산 안에 dead letter 큐 없는 reject가 있으면 정산 전체가 반영되지 않는다 — check 2")
    void ifThereIsARejectWithoutADeadLetterQueueInTheSettlementTheEntireSettlementIsNotReflectedCheck2() {
        final PullResult pulled;
        final MessageId acked;
        final MessageId rejected;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        acked = pulled.messages().get(0).id();
        rejected = pulled.messages().get(1).id();
        evaluate(captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(new Disposition(acked, Kind.ACK), new Disposition(rejected, Kind.REJECT)),
                InstantSource.fixed(T0))));
        assertionValue2Argument1 = new QueueCounts(0L, 2L, 0L);
        assertionValue2Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .pull("q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .messages()
                .size());
    }

    @Test
    @DisplayName("정산 안에 dead letter 큐 없는 reject가 있으면 정산 전체가 반영되지 않는다 — check 3")
    void ifThereIsARejectWithoutADeadLetterQueueInTheSettlementTheEntireSettlementIsNotReflectedCheck3() {
        final PullResult pulled;
        final MessageId acked;
        final MessageId rejected;
        final int assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        acked = pulled.messages().get(0).id();
        rejected = pulled.messages().get(1).id();
        evaluate(captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(new Disposition(acked, Kind.ACK), new Disposition(rejected, Kind.REJECT)),
                InstantSource.fixed(T0))));
        evaluate(new QueueCounts(0L, 2L, 0L));
        assertionValue3Argument2 = this.storage
                .pull("q", 2, LEASE, InstantSource.fixed(T0.plus(LEASE)))
                .messages()
                .size();
        Assertions.assertEquals(2, assertionValue3Argument2, "Size should match the expected value.");
    }

    @Test
    @DisplayName("잘못된 NACK 시각은 앞선 ACK도 반영하지 않는다 — check 1")
    void anIncorrectNACKTimestampDoesNotReflectThePreviousACKEitherCheck1() {
        final PullResult pulled;
        final Instant nearMaximum;
        final Throwable assertionValue1;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        nearMaximum = Instant.MAX.minusSeconds(2L);
        assertionValue1 = captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(
                        new Disposition(pulled.messages().get(0).id(), Kind.ACK),
                        new Disposition(pulled.messages().get(1).id(), Kind.NACK, Duration.ofSeconds(3L))),
                InstantSource.fixed(nearMaximum)));
        Assertions.assertInstanceOf(
                DateTimeException.class, assertionValue1, "Settle should throw java.time.DateTimeException.");
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(this.storage
                .settle(
                        pulled.token(),
                        pulled.messages().stream()
                                .map(message -> new Disposition(message.id(), Kind.ACK))
                                .toList(),
                        InstantSource.fixed(T0))
                .stream()
                .map(Result::outcome)
                .toList());
    }

    @Test
    @DisplayName("잘못된 NACK 시각은 앞선 ACK도 반영하지 않는다 — check 2")
    void anIncorrectNACKTimestampDoesNotReflectThePreviousACKEitherCheck2() {
        final PullResult pulled;
        final Instant nearMaximum;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        nearMaximum = Instant.MAX.minusSeconds(2L);
        evaluate(captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(
                        new Disposition(pulled.messages().get(0).id(), Kind.ACK),
                        new Disposition(pulled.messages().get(1).id(), Kind.NACK, Duration.ofSeconds(3L))),
                InstantSource.fixed(nearMaximum))));
        assertionValue2Argument1 = new QueueCounts(0L, 2L, 0L);
        assertionValue2Argument2 = this.storage.counts("q");
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(this.storage
                .settle(
                        pulled.token(),
                        pulled.messages().stream()
                                .map(message -> new Disposition(message.id(), Kind.ACK))
                                .toList(),
                        InstantSource.fixed(T0))
                .stream()
                .map(Result::outcome)
                .toList());
    }

    @Test
    @DisplayName("잘못된 NACK 시각은 앞선 ACK도 반영하지 않는다 — check 3")
    void anIncorrectNACKTimestampDoesNotReflectThePreviousACKEitherCheck3() {
        final PullResult pulled;
        final Instant nearMaximum;
        final List<Outcome> assertionValue3Argument1;
        final List<Outcome> assertionValue3Argument2;
        this.storage.declare("q", QueueOptions.defaults());
        this.storage.publish("q", List.of(message(1), message(2)), InstantSource.fixed(T0));
        pulled = this.storage.pull("q", 2, LEASE, InstantSource.fixed(T0));
        nearMaximum = Instant.MAX.minusSeconds(2L);
        evaluate(captureThrowable(() -> this.storage.settle(
                pulled.token(),
                List.of(
                        new Disposition(pulled.messages().get(0).id(), Kind.ACK),
                        new Disposition(pulled.messages().get(1).id(), Kind.NACK, Duration.ofSeconds(3L))),
                InstantSource.fixed(nearMaximum))));
        evaluate(new QueueCounts(0L, 2L, 0L));
        assertionValue3Argument1 = List.of(Outcome.OK, Outcome.OK);
        assertionValue3Argument2 = this.storage
                .settle(
                        pulled.token(),
                        pulled.messages().stream()
                                .map(message -> new Disposition(message.id(), Kind.ACK))
                                .toList(),
                        InstantSource.fixed(T0))
                .stream()
                .map(Result::outcome)
                .toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Settlement should match the expected value.");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    @DisplayName("dead letter 이동 전 전체 메시지에 리스를 부여한다")
    void initialPullLeasesAllMessagesBeforeExhaustion(final int aboveLimit) {
        final int count;
        count = 100 + aboveLimit;
        Assertions.assertEquals(
                count,
                this.leaseMessagesBeforeExhaustion("prepared", count).messages().size(),
                "The initial pull should lease every message.");
    }

    private PullResult pullAfterExhausting(final String queue, final int count) {
        this.leaseMessagesBeforeExhaustion(queue, count);
        return this.storage.pull(queue, 1, LEASE, InstantSource.fixed(T0.plus(LEASE)));
    }

    private PullResult leaseMessagesBeforeExhaustion(final String queue, final int count) {
        final List<NewMessage> batch;
        this.storage.declare("dead-" + queue, QueueOptions.defaults());
        this.storage.declare(queue, QueueOptions.defaults().maxDeliveries(1, "dead-" + queue));
        batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(message(i));
        }
        this.storage.publish(queue, batch, InstantSource.fixed(T0));
        return this.storage.pull(queue, count, LEASE, InstantSource.fixed(T0));
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private static final class LeaseReferenceModel {

        private final List<MessageId> ready = new ArrayList<>();

        private final Map<MessageId, StorageContractTest.LeaseReferenceModel.Lease> leased = new LinkedHashMap<>();

        private long generation;

        void publish(final List<MessageId> ids) {
            this.ready.addAll(ids);
        }

        StorageContractTest.LeaseReferenceModel.ExpectedPull pull(final Instant now, final Duration lease) {
            final MessageId id;
            final long leaseGeneration;
            for (final MessageId expiredId : this.leased.entrySet().stream()
                    .filter(e -> !e.getValue().deadline().isAfter(now))
                    .map(Entry::getKey)
                    .toList()) {
                this.leased.remove(expiredId);
                this.ready.add(expiredId);
            }
            this.ready.sort(MessageId::compareTo);
            if (this.ready.isEmpty()) {
                return new StorageContractTest.LeaseReferenceModel.ExpectedPull(List.of(), -1L);
            }
            id = this.ready.removeFirst();
            this.generation++;
            leaseGeneration = this.generation;
            this.leased.put(id, new StorageContractTest.LeaseReferenceModel.Lease(leaseGeneration, now.plus(lease)));
            return new StorageContractTest.LeaseReferenceModel.ExpectedPull(List.of(id), leaseGeneration);
        }

        Outcome settle(final MessageId id, final long leaseGeneration) {
            final StorageContractTest.LeaseReferenceModel.Lease lease;
            lease = this.leased.get(id);
            if (lease != null && lease.generation() == leaseGeneration) {
                this.leased.remove(id);
                return Outcome.OK;
            } else {
                return Outcome.STALE;
            }
        }

        QueueCounts counts() {
            return new QueueCounts(this.ready.size(), this.leased.size(), 0L);
        }

        private record ExpectedPull(List<MessageId> messages, long generation) {}

        private record Lease(long generation, Instant deadline) {}
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
