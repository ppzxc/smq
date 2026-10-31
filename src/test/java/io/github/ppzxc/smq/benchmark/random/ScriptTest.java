package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScriptTest {

    private static final Limits FULL = Limits.full(10_000);

    private static Script script(long seed) {
        return new Script(RunParams.fromSeed(seed, FULL));
    }

    @Test
    void sameSeedAndTagMakeTheSameMessage() {
        Tag tag = new Tag(9, 0, 17);
        Script.Planned a = script(9).message(tag);
        Script.Planned b = script(9).message(tag);
        assertArrayEquals(a.message().payload(), b.message().payload());
        assertEquals(a.message().headers(), b.message().headers());
        assertEquals(a.message().delay(), b.message().delay());
        assertEquals(a.content(), b.content());
        assertNotEquals(a.content(), script(9).message(new Tag(9, 0, 18)).content());
    }

    @Test
    void contentMatchesTheMessageAndCarriesTheTag() {
        for (long seq = 0; seq < 200; seq++) {
            Tag tag = new Tag(5, 0, seq);
            Script.Planned p = script(5).message(tag);
            assertEquals(Script.checksum(p.message().payload()), p.content().payloadChecksum());
            assertEquals(p.message().headers(), p.content().headers());
            assertEquals(tag.toString(), p.content().headers().get(Script.TAG_HEADER));
            assertEquals(p.delayMillis(), p.message().delay().toMillis());
            assertTrue(
                    p.message().payload().length <= RunParams.fromSeed(5, FULL).maxMessageBytes());
        }
    }

    @Test
    void decisionsDependOnlyOnSeedTagAndDeliveryCount() {
        Script s = script(11);
        for (long seq = 0; seq < 100; seq++) {
            Tag tag = new Tag(11, 0, seq);
            assertEquals(s.settle(tag, 2), script(11).settle(tag, 2));
            assertEquals(s.processing(tag, 1), script(11).processing(tag, 1));
        }
    }

    @Test
    void settleDecisionsFollowTheRatiosWithAckInTheMajority() {
        RunParams p = RunParams.fromSeed(3, FULL);
        Script s = new Script(p);
        Map<Action, Integer> counts = new EnumMap<>(Action.class);
        int n = 20_000;
        for (long seq = 0; seq < n; seq++) {
            counts.merge(s.settle(new Tag(3, 0, seq), 1).action(), 1, Integer::sum);
        }
        assertEquals(p.ackRatio(), counts.get(Action.ACK) / (double) n, 0.02);
        assertTrue(counts.get(Action.ACK) > n / 2);
        assertEquals(p.rejectRatio(), counts.getOrDefault(Action.REJECT, 0) / (double) n, 0.02);
        for (long seq = 0; seq < n; seq++) {
            Script.Decision d = s.settle(new Tag(3, 0, seq), 1);
            assertTrue(d.nackDelayMillis() == 0 || d.action() == Action.NACK);
            assertTrue(d.nackDelayMillis() <= p.nackDelayMaxMillis());
        }
    }

    @Test
    void processingStaysWithinOneAndAHalfLeases() {
        RunParams p = RunParams.fromSeed(8, FULL);
        Script s = new Script(p);
        long longest = 0;
        for (long seq = 0; seq < 5_000; seq++) {
            longest = Math.max(longest, s.processing(new Tag(8, 0, seq), 1).millis());
        }
        assertTrue(longest <= p.leaseMillis() * 1.5);
    }
}
