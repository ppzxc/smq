package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import io.github.ppzxc.smq.benchmark.random.InvariantChecker.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InvariantCheckerTest {

    private static final long T0 = 1_000_000L;
    private static final long LEASE = 5_000;
    private static final Config PLAIN = new Config(false, 0);
    private static final Config MAX3 = new Config(false, 3);
    private static final Config SINGLE = new Config(true, 0);

    private static Tag tag(long n) {
        return new Tag(7, 0, n);
    }

    private static Content content(long n) {
        return new Content(1000 + n, Map.of("tag", tag(n).toString()));
    }

    private static Published pub(long n) {
        return new Published(tag(n), content(n), T0, 0);
    }

    private static Published pubDelayed(long n, long delay) {
        return new Published(tag(n), content(n), T0, delay);
    }

    /** 대상 큐 전달. 청크 번호는 메시지마다 달라 서로 겹치지 않게 둔다. */
    private static Delivery target(long n, long id, int count, long at, Settle settle) {
        return new Delivery(
                QueueKind.TARGET, tag(n), id, count, content(n), new Pull(n * 100 + count, 0, at, at + LEASE), settle);
    }

    private static Delivery targetIn(
            long n, long id, int count, long chunk, int position, long at, long lease, Settle settle) {
        return new Delivery(
                QueueKind.TARGET, tag(n), id, count, content(n), new Pull(chunk, position, at, lease), settle);
    }

    private static Map<String, String> dlHeaders(long n, String reason, long originalId, int lastCount) {
        return Map.of(
                "tag",
                tag(n).toString(),
                "smq-reason",
                reason,
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                Long.toString(originalId),
                "smq-last-delivery-count",
                Integer.toString(lastCount),
                "smq-moved-at",
                "x");
    }

    private static Delivery deadLetter(long n, long id, Map<String, String> headers, long at, Settle settle) {
        return new Delivery(
                QueueKind.DEAD_LETTER,
                tag(n),
                id,
                1,
                new Content(1000 + n, headers),
                new Pull(900_000 + id, 0, at, at + LEASE),
                settle);
    }

    private static List<CountsSample> drained() {
        return List.of(
                new CountsSample(QueueKind.TARGET, true, 0, 0, 0),
                new CountsSample(QueueKind.DEAD_LETTER, true, 0, 0, 0));
    }

    private static List<Violation> check(Config cfg, List<Published> p, List<Delivery> d) {
        return InvariantChecker.check(cfg, p, d, drained());
    }

    private static List<Violation> only(Invariant kind, List<Violation> all) {
        List<Violation> out = all.stream().filter(v -> v.invariant() == kind).toList();
        assertEquals(all, out, "다른 불변식 위반이 섞였다");
        return out;
    }

    private static void assertSingle(List<Violation> all, Invariant kind, List<Tag> tags, List<Long> ids) {
        List<Violation> v = only(kind, all);
        assertEquals(1, v.size(), v.toString());
        assertEquals(tags, v.getFirst().tags());
        assertEquals(ids, v.getFirst().messageIds());
    }

    // ---- 정상 기록

    @Test
    void cleanRunWithRedeliveryDeadLetterAndDelaysPasses() {
        List<Published> p = List.of(pub(1), pub(2), pub(3), pub(4), pubDelayed(5, 1_000));
        List<Delivery> d = new ArrayList<>();
        d.add(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)));
        // 2: NACK(지연 500ms) 뒤 재전달, 그 뒤 ACK
        d.add(target(2, 12, 1, T0 + 10, Settle.nack(T0 + 20, 500)));
        d.add(target(2, 12, 2, T0 + 520, Settle.ack(T0 + 530)));
        // 3: 정산 안 함, 리스 만료 뒤 재전달 뒤 reject -> dead letter
        d.add(target(3, 13, 1, T0 + 10, Settle.NONE));
        d.add(target(3, 13, 2, T0 + 10 + LEASE, Settle.reject(T0 + LEASE + 20)));
        d.add(deadLetter(3, 1, dlHeaders(3, "rejected", 13, 2), T0 + LEASE + 30, Settle.ack(T0 + LEASE + 40)));
        // 4: 최대 전달 3회 소진 -> dead letter
        d.add(target(4, 14, 1, T0 + 10, Settle.nack(T0 + 20, 0)));
        d.add(target(4, 14, 2, T0 + 30, Settle.nack(T0 + 40, 0)));
        d.add(target(4, 14, 3, T0 + 50, Settle.nack(T0 + 60, 0)));
        d.add(deadLetter(4, 2, dlHeaders(4, "max-deliveries", 14, 3), T0 + 70, Settle.ack(T0 + 80)));
        // 5: 발행 지연 1s 뒤 전달
        d.add(target(5, 15, 1, T0 + 1_010, Settle.ack(T0 + 1_020)));

        assertEquals(List.of(), check(MAX3, p, d));
    }

    // ---- 1. 유실 없음

    @Test
    void publishedTagNeverDeliveredIsLoss() {
        List<Violation> v =
                check(PLAIN, List.of(pub(1), pub(2)), List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20))));
        assertSingle(v, Invariant.LOSS, List.of(tag(2)), List.of());
    }

    @Test
    void deliveredButNeverAckedIsLoss() {
        List<Violation> v = check(PLAIN, List.of(pub(1)), List.of(target(1, 11, 1, T0 + 10, Settle.NONE)));
        assertSingle(v, Invariant.LOSS, List.of(tag(1)), List.of(11L));
    }

    @Test
    void staleAckDoesNotCountAsAck() {
        Settle stale = new Settle(Delivery.Action.ACK, false, T0 + 20, 0);
        List<Violation> v = check(PLAIN, List.of(pub(1)), List.of(target(1, 11, 1, T0 + 10, stale)));
        assertSingle(v, Invariant.LOSS, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deadLetteredButNeverAckedThereIsLoss() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.NONE));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.LOSS, List.of(tag(1)), List.of(1L));
    }

    @Test
    void ackedTwiceIsReportedAsDuplicate() {
        // 같은 태그가 서로 다른 메시지 id로 두 번 들어와 둘 다 ACK됐다
        List<Delivery> d =
                List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 12, 1, T0 + 10, Settle.ack(T0 + 20)));
        List<Violation> v = check(PLAIN, List.of(pub(1)), d);
        assertTrue(v.stream().allMatch(x -> x.invariant() == Invariant.DUPLICATE), v.toString());
        assertFalse(v.isEmpty());
        assertEquals(List.of(tag(1)), v.getFirst().tags());
    }

    // ---- 2. 유령 없음

    @Test
    void unpublishedTagIsGhost() {
        List<Delivery> d =
                List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(9, 19, 1, T0 + 10, Settle.ack(T0 + 20)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.GHOST, List.of(tag(9)), List.of(19L));
    }

    @Test
    void payloadChecksumMismatchIsGhost() {
        Delivery d = new Delivery(
                QueueKind.TARGET,
                tag(1),
                11,
                1,
                new Content(5, content(1).headers()),
                new Pull(1, 0, T0 + 10, T0 + 10 + LEASE),
                Settle.ack(T0 + 20));
        assertSingle(check(PLAIN, List.of(pub(1)), List.of(d)), Invariant.GHOST, List.of(tag(1)), List.of(11L));
    }

    @Test
    void userHeaderMismatchIsGhost() {
        Delivery d = new Delivery(
                QueueKind.TARGET,
                tag(1),
                11,
                1,
                new Content(1001, Map.of("tag", tag(1).toString(), "extra", "x")),
                new Pull(1, 0, T0 + 10, T0 + 10 + LEASE),
                Settle.ack(T0 + 20));
        assertSingle(check(PLAIN, List.of(pub(1)), List.of(d)), Invariant.GHOST, List.of(tag(1)), List.of(11L));
    }

    // ---- 3. at-least-once 범위의 중복

    @Test
    void redeliveryAfterOkAckIsDuplicate() {
        List<Delivery> d =
                List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 11, 2, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryAfterStaleAckWithoutLeaseExpiryIsDuplicate() {
        Settle stale = new Settle(Delivery.Action.ACK, false, T0 + 20, 0);
        List<Delivery> d = List.of(target(1, 11, 1, T0 + 10, stale), target(1, 11, 2, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryBeforeLeaseExpiryWithoutSettlementIsDuplicate() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.NONE), target(1, 11, 2, T0 + 10 + LEASE - 51, Settle.ack(T0 + LEASE)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryJustInsideLeaseToleranceIsAccepted() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.NONE), target(1, 11, 2, T0 + 10 + LEASE - 50, Settle.ack(T0 + LEASE)));
        assertEquals(List.of(), check(PLAIN, List.of(pub(1)), d));
    }

    @Test
    void deliveryCountGapIsDuplicate() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)), target(1, 11, 3, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryCountRepeatedIsDuplicate() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                targetIn(1, 11, 1, 2, 0, T0 + 30, T0 + 5_030, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryCountNotStartingAtOneIsDuplicate() {
        List<Delivery> d = List.of(target(1, 11, 2, T0 + 10, Settle.ack(T0 + 20)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryBeyondMaxDeliveriesIsDuplicate() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                target(1, 11, 2, T0 + 30, Settle.nack(T0 + 40, 0)),
                target(1, 11, 3, T0 + 50, Settle.nack(T0 + 60, 0)),
                target(1, 11, 4, T0 + 70, Settle.ack(T0 + 80)));
        assertSingle(check(MAX3, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    // ---- 4. dead letter 이동 정확성

    @Test
    void rejectWithoutMoveIsDeadLetterViolation() {
        List<Delivery> d = List.of(target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L));
    }

    @Test
    void exhaustionWithoutMoveIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                target(1, 11, 2, T0 + 30, Settle.nack(T0 + 40, 0)),
                target(1, 11, 3, T0 + 50, Settle.nack(T0 + 60, 0)));
        assertSingle(check(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L));
    }

    @Test
    void moveWithoutRejectOrExhaustionIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void maxDeliveriesReasonBeforeExhaustionIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                deadLetter(1, 1, dlHeaders(1, "max-deliveries", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongReasonHeaderIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "max-deliveries", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongOriginalMessageIdHeaderIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 99, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongLastDeliveryCountHeaderIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 2), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void missingDeadLetterHeaderIsDeadLetterViolation() {
        Map<String, String> h = Map.of("tag", tag(1).toString(), "smq-reason", "rejected");
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)), deadLetter(1, 1, h, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void movedTwiceIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)),
                deadLetter(1, 2, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        List<Violation> v = only(Invariant.DEAD_LETTER, check(PLAIN, List.of(pub(1)), d));
        assertEquals(List.of(tag(1)), v.getFirst().tags());
    }

    @Test
    void ackedAndMovedIsDeadLetterViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        List<Violation> v = only(Invariant.DEAD_LETTER, check(PLAIN, List.of(pub(1)), d));
        assertEquals(List.of(tag(1)), v.getFirst().tags());
    }

    // ---- 5. 지연 준수

    @Test
    void publishDelayShortOfToleranceIsViolation() {
        List<Delivery> d = List.of(target(1, 11, 1, T0 + 1_000 - 51, Settle.ack(T0 + 1_000)));
        assertSingle(check(PLAIN, List.of(pubDelayed(1, 1_000)), d), Invariant.DELAY, List.of(tag(1)), List.of(11L));
    }

    @Test
    void publishDelayWithinToleranceIsAccepted() {
        List<Delivery> d = List.of(target(1, 11, 1, T0 + 1_000 - 50, Settle.ack(T0 + 1_000)));
        assertEquals(List.of(), check(PLAIN, List.of(pubDelayed(1, 1_000)), d));
    }

    @Test
    void nackDelayShortOfToleranceIsViolation() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 1_000)),
                target(1, 11, 2, T0 + 20 + 1_000 - 51, Settle.ack(T0 + 1_100)));
        assertSingle(check(PLAIN, List.of(pub(1)), d), Invariant.DELAY, List.of(tag(1)), List.of(11L));
    }

    @Test
    void nackDelayWithinToleranceIsAccepted() {
        List<Delivery> d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 1_000)),
                target(1, 11, 2, T0 + 20 + 1_000 - 50, Settle.ack(T0 + 1_100)));
        assertEquals(List.of(), check(PLAIN, List.of(pub(1)), d));
    }

    // ---- 6. 단일 활성 컨슈머

    @Test
    void overlappingChunksOnSingleActiveQueueIsViolation() {
        // 청크 1은 리스가 T0+5010까지 살아 있는데 청크 2가 T0+1000에 꺼내졌다
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 1_000, T0 + 6_000, Settle.ack(T0 + 1_100)),
                target(1, 11, 2, T0 + 5_010, Settle.ack(T0 + 5_020)));
        assertSingle(
                check(SINGLE, List.of(pub(1), pub(2)), d),
                Invariant.SINGLE_ACTIVE,
                List.of(tag(1), tag(2)),
                List.of(11L, 12L));
    }

    @Test
    void sameOverlapOnPlainQueueIsAccepted() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(2, 12, 1, 2, 0, T0 + 1_000, T0 + 6_000, Settle.ack(T0 + 1_100)));
        assertEquals(List.of(), check(PLAIN, List.of(pub(1), pub(2)), d));
    }

    @Test
    void nextChunkAfterFullSettlementIsAccepted() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 100)),
                targetIn(2, 12, 1, 2, 0, T0 + 200, T0 + 5_200, Settle.ack(T0 + 300)));
        assertEquals(List.of(), check(SINGLE, List.of(pub(1), pub(2)), d));
    }

    @Test
    void nextChunkAtLeaseExpiryWithoutSettlementIsAccepted() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertEquals(List.of(), check(SINGLE, List.of(pub(1), pub(2)), d));
    }

    @Test
    void nextChunkJustInsideToleranceBeforeLeaseExpiryIsAccepted() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010 - 50, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertEquals(List.of(), check(SINGLE, List.of(pub(1), pub(2)), d));
    }

    @Test
    void nextChunkBeyondToleranceBeforeLeaseExpiryIsViolation() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010 - 51, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertSingle(
                check(SINGLE, List.of(pub(1), pub(2)), d),
                Invariant.SINGLE_ACTIVE,
                List.of(tag(1), tag(2)),
                List.of(11L, 12L));
    }

    // ---- 7. counts 일관성

    @Test
    void nonZeroCountsAfterDrainIsViolation() {
        List<CountsSample> s = List.of(
                new CountsSample(QueueKind.TARGET, true, 0, 1, 0),
                new CountsSample(QueueKind.DEAD_LETTER, true, 0, 0, 0));
        List<Violation> v = only(Invariant.COUNTS, InvariantChecker.check(PLAIN, List.of(), List.of(), s));
        assertEquals(1, v.size());
        assertTrue(v.getFirst().detail().contains("TARGET"), v.getFirst().detail());
    }

    @Test
    void negativeSampleDuringLoadIsViolation() {
        List<CountsSample> s = new ArrayList<>(drained());
        s.add(new CountsSample(QueueKind.DEAD_LETTER, false, 3, -1, 0));
        List<Violation> v = only(Invariant.COUNTS, InvariantChecker.check(PLAIN, List.of(), List.of(), s));
        assertEquals(1, v.size());
        assertTrue(v.getFirst().detail().contains("DEAD_LETTER"), v.getFirst().detail());
    }

    @Test
    void nonZeroSampleDuringLoadIsAccepted() {
        List<CountsSample> s = new ArrayList<>(drained());
        s.add(new CountsSample(QueueKind.TARGET, false, 10, 5, 2));
        assertEquals(List.of(), InvariantChecker.check(PLAIN, List.of(), List.of(), s));
    }

    @Test
    void missingDrainedSampleIsViolation() {
        List<CountsSample> s = List.of(new CountsSample(QueueKind.TARGET, true, 0, 0, 0));
        List<Violation> v = only(Invariant.COUNTS, InvariantChecker.check(PLAIN, List.of(), List.of(), s));
        assertEquals(1, v.size());
        assertTrue(v.getFirst().detail().contains("DEAD_LETTER"), v.getFirst().detail());
    }

    // ---- 8. 청크 안 메시지 id 순서

    @Test
    void descendingIdsInsideChunkIsViolation() {
        List<Delivery> d = List.of(
                targetIn(1, 12, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(2, 11, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertSingle(
                check(PLAIN, List.of(pub(1), pub(2)), d),
                Invariant.CHUNK_ORDER,
                List.of(tag(1), tag(2)),
                List.of(12L, 11L));
    }

    @Test
    void positionDecidesOrderNotRecordOrder() {
        List<Delivery> d = List.of(
                targetIn(2, 12, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(1, 11, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertEquals(List.of(), check(PLAIN, List.of(pub(1), pub(2)), d));
    }

    @Test
    void sameIdTwiceInsideChunkIsViolation() {
        List<Delivery> d = List.of(
                targetIn(1, 11, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(1, 11, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertTrue(check(PLAIN, List.of(pub(1)), d).stream().anyMatch(v -> v.invariant() == Invariant.CHUNK_ORDER));
    }
}
