package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScriptTest {

    private static final Limits FULL = Limits.full(10_000);

    private static Script script(final long seed) {
        return new Script(RunParams.fromSeed(seed, FULL));
    }

    @Test
    void sameSeedPreservesPayload() {
        final Tag tag;
        final Script.Planned a;
        final Script.Planned b;
        tag = new Tag(9, 17);
        a = script(9).message(tag);
        b = script(9).message(tag);
        assertArrayEquals(a.message().payload(), b.message().payload(), "Payload should match the expected value.");
    }

    @Test
    void sameSeedPreservesHeaders() {
        final Tag tag;
        final Script.Planned a;
        final Script.Planned b;
        tag = new Tag(9, 17);
        a = script(9).message(tag);
        b = script(9).message(tag);
        assertEquals(a.message().headers(), b.message().headers(), "Headers should match the expected value.");
    }

    @Test
    void sameSeedPreservesDelay() {
        final Tag tag;
        final Script.Planned a;
        final Script.Planned b;
        tag = new Tag(9, 17);
        a = script(9).message(tag);
        b = script(9).message(tag);
        assertEquals(a.message().delay(), b.message().delay(), "Delay should match the expected value.");
    }

    @Test
    void sameSeedPreservesContent() {
        final Tag tag;
        final Script.Planned a;
        final Script.Planned b;
        tag = new Tag(9, 17);
        a = script(9).message(tag);
        b = script(9).message(tag);
        assertEquals(a.content(), b.content(), "Content should match the expected value.");
    }

    @Test
    void differentTagsProduceDifferentContent() {
        final Tag tag;
        final Script.Planned a;
        tag = new Tag(9, 17);
        a = script(9).message(tag);
        assertNotEquals(
                a.content(),
                script(9).message(new Tag(9, 18)).content(),
                "Content should differ from the unexpected value.");
    }

    @Test
    void messageContentPreservesPayloadChecksum() {
        for (long seq = 0; seq < 200; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(5, seq);
            p = script(5).message(tag);
            assertEquals(
                    Script.payloadChecksum(p.message().payload()),
                    p.content().payloadChecksum(),
                    "Payload checksum should match the expected value.");
        }
    }

    @Test
    void messageContentPreservesHeaders() {
        for (long seq = 0; seq < 200; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(5, seq);
            p = script(5).message(tag);
            assertEquals(p.message().headers(), p.content().headers(), "Headers should match the expected value.");
        }
    }

    @Test
    void messageContentIncludesTag() {
        for (long seq = 0; seq < 200; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(5, seq);
            p = script(5).message(tag);
            assertEquals(
                    tag.toString(),
                    p.content().headers().get(Script.TAG_HEADER),
                    "Headers should match the expected value.");
        }
    }

    @Test
    void messageContentPreservesDelay() {
        for (long seq = 0; seq < 200; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(5, seq);
            p = script(5).message(tag);
            assertEquals(p.delayMillis(), p.message().delay().toMillis(), "To millis should match the expected value.");
        }
    }

    @Test
    void messagePayloadStaysWithinLimit() {
        for (long seq = 0; seq < 200; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(5, seq);
            p = script(5).message(tag);
            assertTrue(
                    p.message().payload().length <= RunParams.fromSeed(5, FULL).maxMessageBytes(),
                    "Payload size should stay within the configured maximum.");
        }
    }

    @Test
    void settlementDependsOnlyOnSeedTagAndCount() {
        final Script s;
        s = script(11);
        for (long seq = 0; seq < 100; seq++) {
            final Tag tag;
            tag = new Tag(11, seq);
            assertEquals(s.settle(tag, 2), script(11).settle(tag, 2), "Settlement should match the expected value.");
        }
    }

    @Test
    void processingDependsOnlyOnSeedTagAndCount() {
        final Script s;
        s = script(11);
        for (long seq = 0; seq < 100; seq++) {
            final Tag tag;
            tag = new Tag(11, seq);
            assertEquals(
                    s.processing(tag, 1), script(11).processing(tag, 1), "Processing should match the expected value.");
        }
    }

    private static final int DECISION_COUNT = 20_000;

    private record SettlementCounts(RunParams params, Map<Action, Integer> counts) {}

    private static SettlementCounts settlementCounts() {
        final RunParams params;
        final Script script;
        final Map<Action, Integer> counts;
        params = RunParams.fromSeed(3, FULL);
        script = new Script(params);
        counts = new EnumMap<>(Action.class);
        for (long seq = 0; seq < DECISION_COUNT; seq++) {
            counts.merge(script.settle(new Tag(3, seq), 1).action(), 1, Integer::sum);
        }
        return new SettlementCounts(params, counts);
    }

    @Test
    void settlementAckFrequencyMatchesConfiguredRatio() {
        final SettlementCounts sample;
        sample = settlementCounts();
        assertEquals(
                sample.params().ackRatio(),
                sample.counts().get(Action.ACK) / (double) DECISION_COUNT,
                0.02,
                "ACK frequency should match the configured ratio.");
    }

    @Test
    void settlementAckActionsAreTheMajority() {
        assertTrue(
                settlementCounts().counts().get(Action.ACK) > DECISION_COUNT / 2,
                "ACK actions should form the majority.");
    }

    @Test
    void settlementRejectFrequencyMatchesConfiguredRatio() {
        final SettlementCounts sample;
        sample = settlementCounts();
        assertEquals(
                sample.params().rejectRatio(),
                sample.counts().getOrDefault(Action.REJECT, 0) / (double) DECISION_COUNT,
                0.02,
                "Reject frequency should match the configured ratio.");
    }

    @Test
    void onlyNackDecisionsHaveDelay() {
        final Script script;
        script = new Script(RunParams.fromSeed(3, FULL));
        for (long seq = 0; seq < DECISION_COUNT; seq++) {
            final Script.Decision decision;
            decision = script.settle(new Tag(3, seq), 1);
            assertTrue(
                    decision.nackDelayMillis() == 0 || decision.action() == Action.NACK,
                    "Only NACK decisions may have a delay.");
        }
    }

    @Test
    void nackDelayStaysWithinConfiguredLimit() {
        final RunParams params;
        final Script script;
        params = RunParams.fromSeed(3, FULL);
        script = new Script(params);
        for (long seq = 0; seq < DECISION_COUNT; seq++) {
            assertTrue(
                    script.settle(new Tag(3, seq), 1).nackDelayMillis() <= params.nackDelayMaxMillis(),
                    "NACK delay should stay within the configured limit.");
        }
    }

    @Test
    void processingStaysWithinOneAndAHalfLeases() {
        final RunParams p;
        final Script s;
        long longest;
        p = RunParams.fromSeed(8, FULL);
        s = new Script(p);
        longest = 0;
        for (long seq = 0; seq < 5_000; seq++) {
            longest = Math.max(longest, s.processing(new Tag(8, seq), 1).millis());
        }
        assertTrue(longest <= p.leaseMillis() * 1.5, "The longest lease should stay within the expected bound.");
    }

    @Test
    @DisplayName("고정 속도 모드의 메시지 크기는 최소와 최대 사이다")
    void fixedRateMessageSizesStayWithinBounds() {
        final Script script;
        script = new Script(RunParams.fromSeed(5, FULL.withRate(500)));
        for (long seq = 0; seq < 2_000; seq++) {
            final int size;
            size = script.message(new Tag(5, seq)).message().payload().length;
            assertTrue(size >= 512 && size <= 1024, "Message size should stay within bounds: " + size);
        }
    }

    @Test
    @DisplayName("고정 속도 모드의 메시지 크기는 고르게 퍼진다")
    void fixedRateMessageSizesHaveExpectedMean() {
        final Script script;
        long total;
        final double mean;
        script = new Script(RunParams.fromSeed(5, FULL.withRate(500)));
        total = 0;
        for (long seq = 0; seq < 2_000; seq++) {
            total += script.message(new Tag(5, seq)).message().payload().length;
        }
        mean = total / 2_000.0;
        assertTrue(mean > 720 && mean < 820, "Mean message size should be near 768: " + mean);
    }

    @Test
    @DisplayName("무작위 모드는 작은 메시지가 많아 크기가 최소로 묶이지 않는다")
    void inRandomModeTheSizeIsNotMinimizedBecauseThereAreManySmallMessages() {
        final Script random;
        boolean small;
        random = new Script(RunParams.fromSeed(5, FULL));
        small = false;
        for (long seq = 0; seq < 500 && !small; seq++) {
            small = random.message(new Tag(5, seq)).message().payload().length < 100;
        }
        assertTrue(small, "로그 균등이라 0에 가까운 크기도 나온다");
    }
}
