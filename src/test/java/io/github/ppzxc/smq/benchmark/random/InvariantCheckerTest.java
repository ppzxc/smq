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

    private static Tag tag(final long n) {
        return new Tag(7, n);
    }

    private static Content content(final long n) {
        return new Content(1000 + n, Map.of("tag", tag(n).toString()));
    }

    private static Published pub(final long n) {
        return new Published(tag(n), content(n), T0, 0);
    }

    private static Published pubDelayed(final long n, final long delay) {
        return new Published(tag(n), content(n), T0, delay);
    }

    private static Delivery target(final long n, final long id, final int count, final long at, final Settle settle) {
        return new Delivery(
                QueueKind.TARGET, tag(n), id, count, content(n), new Pull(n * 100 + count, 0, at, at + LEASE), settle);
    }

    private static Delivery targetIn(
            final long n,
            final long id,
            final int count,
            final long chunk,
            final int position,
            final long at,
            final long lease,
            final Settle settle) {
        return new Delivery(
                QueueKind.TARGET, tag(n), id, count, content(n), new Pull(chunk, position, at, lease), settle);
    }

    private static Map<String, String> dlHeaders(
            final long n, final String reason, final long originalId, final int lastCount) {
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

    private static Delivery deadLetter(
            final long n, final long id, final Map<String, String> headers, final long at, final Settle settle) {
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

    private static List<Violation> violations(final Config cfg, final List<Published> p, final List<Delivery> d) {
        return InvariantChecker.findViolations(cfg, p, d, drained());
    }

    private static void assertSingle(
            final List<Violation> all, final Invariant kind, final List<Tag> tags, final List<Long> ids) {
        assertEquals(
                List.of(List.of(kind, tags, ids)),
                all.stream()
                        .map(v -> List.of(v.invariant(), v.tags(), v.messageIds()))
                        .toList(),
                "A single violation should contain the expected invariant, tags, and IDs.");
    }

    @Test
    void cleanRunWithRedeliveryDeadLetterAndDelaysPasses() {
        final List<Published> p;
        final List<Delivery> d;
        p = List.of(pub(1), pub(2), pub(3), pub(4), pubDelayed(5, 1_000));
        d = new ArrayList<>();
        d.add(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)));

        d.add(target(2, 12, 1, T0 + 10, Settle.nack(T0 + 20, 500)));
        d.add(target(2, 12, 2, T0 + 520, Settle.ack(T0 + 530)));

        d.add(target(3, 13, 1, T0 + 10, Settle.NONE));
        d.add(target(3, 13, 2, T0 + 10 + LEASE, Settle.reject(T0 + LEASE + 20)));
        d.add(deadLetter(3, 1, dlHeaders(3, "rejected", 13, 2), T0 + LEASE + 30, Settle.ack(T0 + LEASE + 40)));

        d.add(target(4, 14, 1, T0 + 10, Settle.nack(T0 + 20, 0)));
        d.add(target(4, 14, 2, T0 + 30, Settle.nack(T0 + 40, 0)));
        d.add(target(4, 14, 3, T0 + 50, Settle.nack(T0 + 60, 0)));
        d.add(deadLetter(4, 2, dlHeaders(4, "max-deliveries", 14, 3), T0 + 70, Settle.ack(T0 + 80)));

        d.add(target(5, 15, 1, T0 + 1_010, Settle.ack(T0 + 1_020)));

        assertEquals(List.of(), violations(MAX3, p, d), "Check should match the expected value.");
    }

    @Test
    void publishedTagNeverDeliveredIsLoss() {
        final List<Violation> v;
        v = violations(PLAIN, List.of(pub(1), pub(2)), List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20))));
        assertSingle(v, Invariant.LOSS, List.of(tag(2)), List.of());
    }

    @Test
    void deliveredButNeverAckedIsLoss() {
        final List<Violation> v;
        v = violations(PLAIN, List.of(pub(1)), List.of(target(1, 11, 1, T0 + 10, Settle.NONE)));
        assertSingle(v, Invariant.LOSS, List.of(tag(1)), List.of(11L));
    }

    @Test
    void staleAckDoesNotCountAsAck() {
        final Settle stale;
        final List<Violation> v;
        stale = new Settle(Delivery.Action.ACK, false, T0 + 20, 0);
        v = violations(PLAIN, List.of(pub(1)), List.of(target(1, 11, 1, T0 + 10, stale)));
        assertSingle(v, Invariant.LOSS, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deadLetteredButNeverAckedThereIsLoss() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.NONE));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.LOSS, List.of(tag(1)), List.of(1L));
    }

    @Test
    void duplicateAcksProduceOnlyDuplicateViolations() {
        final List<Delivery> d;
        final List<Violation> v;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 12, 1, T0 + 10, Settle.ack(T0 + 20)));
        v = violations(PLAIN, List.of(pub(1)), d);
        assertTrue(v.stream().allMatch(x -> x.invariant() == Invariant.DUPLICATE), v.toString());
    }

    @Test
    void duplicateAcksProduceViolations() {
        final List<Delivery> d;
        final List<Violation> v;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 12, 1, T0 + 10, Settle.ack(T0 + 20)));
        v = violations(PLAIN, List.of(pub(1)), d);
        assertFalse(v.isEmpty(), "V should not be empty.");
    }

    @Test
    void duplicateAcksIdentifyTheTag() {
        final List<Delivery> d;
        final List<Violation> v;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 12, 1, T0 + 10, Settle.ack(T0 + 20)));
        v = violations(PLAIN, List.of(pub(1)), d);
        assertEquals(List.of(tag(1)), v.getFirst().tags(), "Tags should match the expected value.");
    }

    @Test
    void unpublishedTagIsGhost() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(9, 19, 1, T0 + 10, Settle.ack(T0 + 20)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.GHOST, List.of(tag(9)), List.of(19L));
    }

    @Test
    void payloadChecksumMismatchIsGhost() {
        final Delivery d;
        d = new Delivery(
                QueueKind.TARGET,
                tag(1),
                11,
                1,
                new Content(5, content(1).headers()),
                new Pull(1, 0, T0 + 10, T0 + 10 + LEASE),
                Settle.ack(T0 + 20));
        assertSingle(violations(PLAIN, List.of(pub(1)), List.of(d)), Invariant.GHOST, List.of(tag(1)), List.of(11L));
    }

    @Test
    void userHeaderMismatchIsGhost() {
        final Delivery d;
        d = new Delivery(
                QueueKind.TARGET,
                tag(1),
                11,
                1,
                new Content(1001, Map.of("tag", tag(1).toString(), "extra", "x")),
                new Pull(1, 0, T0 + 10, T0 + 10 + LEASE),
                Settle.ack(T0 + 20));
        assertSingle(violations(PLAIN, List.of(pub(1)), List.of(d)), Invariant.GHOST, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryAfterOkAckIsDuplicate() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)), target(1, 11, 2, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryAfterStaleAckWithoutLeaseExpiryIsDuplicate() {
        final Settle stale;
        final List<Delivery> d;
        stale = new Settle(Delivery.Action.ACK, false, T0 + 20, 0);
        d = List.of(target(1, 11, 1, T0 + 10, stale), target(1, 11, 2, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryBeforeLeaseExpiryWithoutSettlementIsDuplicate() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.NONE), target(1, 11, 2, T0 + 10 + LEASE - 51, Settle.ack(T0 + LEASE)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void redeliveryJustInsideLeaseToleranceIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.NONE), target(1, 11, 2, T0 + 10 + LEASE - 50, Settle.ack(T0 + LEASE)));
        assertEquals(List.of(), violations(PLAIN, List.of(pub(1)), d), "Check should match the expected value.");
    }

    @Test
    void deliveryCountGapIsDuplicate() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)), target(1, 11, 3, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryCountRepeatedIsDuplicate() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                targetIn(1, 11, 1, 2, 0, T0 + 30, T0 + 5_030, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryCountNotStartingAtOneIsDuplicate() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 2, T0 + 10, Settle.ack(T0 + 20)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void deliveryBeyondMaxDeliveriesIsDuplicate() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                target(1, 11, 2, T0 + 30, Settle.nack(T0 + 40, 0)),
                target(1, 11, 3, T0 + 50, Settle.nack(T0 + 60, 0)),
                target(1, 11, 4, T0 + 70, Settle.ack(T0 + 80)));
        assertSingle(violations(MAX3, List.of(pub(1)), d), Invariant.DUPLICATE, List.of(tag(1)), List.of(11L));
    }

    @Test
    void rejectWithoutMoveIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L));
    }

    @Test
    void exhaustionWithoutMoveIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                target(1, 11, 2, T0 + 30, Settle.nack(T0 + 40, 0)),
                target(1, 11, 3, T0 + 50, Settle.nack(T0 + 60, 0)));
        assertSingle(violations(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L));
    }

    @Test
    void moveWithoutRejectOrExhaustionIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void maxDeliveriesReasonBeforeExhaustionIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 0)),
                deadLetter(1, 1, dlHeaders(1, "max-deliveries", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongReasonHeaderIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "max-deliveries", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(MAX3, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongOriginalMessageIdHeaderIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 99, 1), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void wrongLastDeliveryCountHeaderIsDeadLetterViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 2), T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void missingDeadLetterHeaderIsDeadLetterViolation() {
        final Map<String, String> h;
        final List<Delivery> d;
        h = Map.of("tag", tag(1).toString(), "smq-reason", "rejected");
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)), deadLetter(1, 1, h, T0 + 30, Settle.ack(T0 + 40)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DEAD_LETTER, List.of(tag(1)), List.of(11L, 1L));
    }

    @Test
    void movedTwiceIsDeadLetterViolation() {
        final List<Delivery> d;
        final List<Violation> v;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.reject(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)),
                deadLetter(1, 2, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        v = violations(PLAIN, List.of(pub(1)), d);
        assertTrue(
                !v.isEmpty()
                        && v.stream().allMatch(violation -> violation.invariant() == Invariant.DEAD_LETTER)
                        && v.getFirst().tags().equals(List.of(tag(1))),
                "All violations should be dead-letter violations and the first should identify the moved tag.");
    }

    @Test
    void ackedAndMovedIsDeadLetterViolation() {
        final List<Delivery> d;
        final List<Violation> v;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.ack(T0 + 20)),
                deadLetter(1, 1, dlHeaders(1, "rejected", 11, 1), T0 + 30, Settle.ack(T0 + 40)));
        v = violations(PLAIN, List.of(pub(1)), d);
        assertTrue(
                !v.isEmpty()
                        && v.stream().allMatch(violation -> violation.invariant() == Invariant.DEAD_LETTER)
                        && v.getFirst().tags().equals(List.of(tag(1))),
                "All violations should be dead-letter violations and the first should identify the moved tag.");
    }

    @Test
    void publishDelayShortOfToleranceIsViolation() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 1_000 - 51, Settle.ack(T0 + 1_000)));
        assertSingle(
                violations(PLAIN, List.of(pubDelayed(1, 1_000)), d), Invariant.DELAY, List.of(tag(1)), List.of(11L));
    }

    @Test
    void publishDelayWithinToleranceIsAccepted() {
        final List<Delivery> d;
        d = List.of(target(1, 11, 1, T0 + 1_000 - 50, Settle.ack(T0 + 1_000)));
        assertEquals(
                List.of(),
                violations(PLAIN, List.of(pubDelayed(1, 1_000)), d),
                "Check should match the expected value.");
    }

    @Test
    void nackDelayShortOfToleranceIsViolation() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 1_000)),
                target(1, 11, 2, T0 + 20 + 1_000 - 51, Settle.ack(T0 + 1_100)));
        assertSingle(violations(PLAIN, List.of(pub(1)), d), Invariant.DELAY, List.of(tag(1)), List.of(11L));
    }

    @Test
    void nackDelayWithinToleranceIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                target(1, 11, 1, T0 + 10, Settle.nack(T0 + 20, 1_000)),
                target(1, 11, 2, T0 + 20 + 1_000 - 50, Settle.ack(T0 + 1_100)));
        assertEquals(List.of(), violations(PLAIN, List.of(pub(1)), d), "Check should match the expected value.");
    }

    @Test
    void overlappingChunksOnSingleActiveQueueIsViolation() {

        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 1_000, T0 + 6_000, Settle.ack(T0 + 1_100)),
                target(1, 11, 2, T0 + 5_010, Settle.ack(T0 + 5_020)));
        assertSingle(
                violations(SINGLE, List.of(pub(1), pub(2)), d),
                Invariant.SINGLE_ACTIVE,
                List.of(tag(1), tag(2)),
                List.of(11L, 12L));
    }

    @Test
    void sameOverlapOnPlainQueueIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(2, 12, 1, 2, 0, T0 + 1_000, T0 + 6_000, Settle.ack(T0 + 1_100)));
        assertEquals(
                List.of(), violations(PLAIN, List.of(pub(1), pub(2)), d), "Check should match the expected value.");
    }

    @Test
    void nextChunkAfterFullSettlementIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 100)),
                targetIn(2, 12, 1, 2, 0, T0 + 200, T0 + 5_200, Settle.ack(T0 + 300)));
        assertEquals(
                List.of(), violations(SINGLE, List.of(pub(1), pub(2)), d), "Check should match the expected value.");
    }

    @Test
    void nextChunkAtLeaseExpiryWithoutSettlementIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertEquals(
                List.of(), violations(SINGLE, List.of(pub(1), pub(2)), d), "Check should match the expected value.");
    }

    @Test
    void nextChunkJustInsideToleranceBeforeLeaseExpiryIsAccepted() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010 - 50, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertEquals(
                List.of(), violations(SINGLE, List.of(pub(1), pub(2)), d), "Check should match the expected value.");
    }

    @Test
    void nextChunkBeyondToleranceBeforeLeaseExpiryIsViolation() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 1, 0, T0 + 10, T0 + 5_010, Settle.NONE),
                targetIn(2, 12, 1, 2, 0, T0 + 5_010 - 51, T0 + 10_010, Settle.ack(T0 + 5_100)),
                target(1, 11, 2, T0 + 10_100, Settle.ack(T0 + 10_200)));
        assertSingle(
                violations(SINGLE, List.of(pub(1), pub(2)), d),
                Invariant.SINGLE_ACTIVE,
                List.of(tag(1), tag(2)),
                List.of(11L, 12L));
    }

    @Test
    void nonZeroCountsAfterDrainIsViolation() {
        final List<CountsSample> s;
        final List<Violation> v;
        s = List.of(
                new CountsSample(QueueKind.TARGET, true, 0, 1, 0),
                new CountsSample(QueueKind.DEAD_LETTER, true, 0, 0, 0));
        v = InvariantChecker.findViolations(PLAIN, List.of(), List.of(), s);
        assertEquals(
                List.of(List.of(Invariant.COUNTS, true)),
                v.stream()
                        .map(violation -> List.of(
                                violation.invariant(), violation.detail().contains("TARGET")))
                        .toList(),
                "A single counts violation should identify the target queue.");
    }

    @Test
    void negativeSampleDuringLoadIsViolation() {
        final List<CountsSample> s;
        final List<Violation> v;
        s = new ArrayList<>(drained());
        s.add(new CountsSample(QueueKind.DEAD_LETTER, false, 3, -1, 0));
        v = InvariantChecker.findViolations(PLAIN, List.of(), List.of(), s);
        assertEquals(
                List.of(List.of(Invariant.COUNTS, true)),
                v.stream()
                        .map(violation -> List.of(
                                violation.invariant(), violation.detail().contains("DEAD_LETTER")))
                        .toList(),
                "A single counts violation should identify the dead-letter queue.");
    }

    @Test
    void nonZeroSampleDuringLoadIsAccepted() {
        final List<CountsSample> s;
        s = new ArrayList<>(drained());
        s.add(new CountsSample(QueueKind.TARGET, false, 10, 5, 2));
        assertEquals(
                List.of(),
                InvariantChecker.findViolations(PLAIN, List.of(), List.of(), s),
                "Check should match the expected value.");
    }

    @Test
    void missingDrainedSampleIsViolation() {
        final List<CountsSample> s;
        final List<Violation> v;
        s = List.of(new CountsSample(QueueKind.TARGET, true, 0, 0, 0));
        v = InvariantChecker.findViolations(PLAIN, List.of(), List.of(), s);
        assertEquals(
                List.of(List.of(Invariant.COUNTS, true)),
                v.stream()
                        .map(violation -> List.of(
                                violation.invariant(), violation.detail().contains("DEAD_LETTER")))
                        .toList(),
                "A single counts violation should identify the dead-letter queue.");
    }

    @Test
    void descendingIdsInsideChunkIsViolation() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 12, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(2, 11, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertSingle(
                violations(PLAIN, List.of(pub(1), pub(2)), d),
                Invariant.CHUNK_ORDER,
                List.of(tag(1), tag(2)),
                List.of(12L, 11L));
    }

    @Test
    void positionDecidesOrderNotRecordOrder() {
        final List<Delivery> d;
        d = List.of(
                targetIn(2, 12, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(1, 11, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertEquals(
                List.of(), violations(PLAIN, List.of(pub(1), pub(2)), d), "Check should match the expected value.");
    }

    @Test
    void sameIdTwiceInsideChunkIsViolation() {
        final List<Delivery> d;
        d = List.of(
                targetIn(1, 11, 1, 5, 0, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)),
                targetIn(1, 11, 1, 5, 1, T0 + 10, T0 + 5_010, Settle.ack(T0 + 20)));
        assertTrue(
                violations(PLAIN, List.of(pub(1)), d).stream().anyMatch(v -> v.invariant() == Invariant.CHUNK_ORDER),
                "A chunk order violation should be reported.");
    }
}
