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

    @Test
    void 고정_속도_모드의_메시지_크기는_최소와_최대_사이에_고르게_퍼진다() {
        Script soak = new Script(RunParams.fromSeed(5, FULL.withRate(500)));
        long total = 0;
        for (long seq = 0; seq < 2_000; seq++) {
            int size = soak.message(new Tag(5, 0, seq)).message().payload().length;
            assertTrue(size >= 512 && size <= 1024, "크기: " + size);
            total += size;
        }
        double mean = total / 2_000.0;
        assertTrue(mean > 720 && mean < 820, "균등이면 평균은 768 근처다: " + mean);
    }

    @Test
    void 무작위_모드는_작은_메시지가_많아_크기가_최소로_묶이지_않는다() {
        Script random = new Script(RunParams.fromSeed(5, FULL));
        boolean small = false;
        for (long seq = 0; seq < 500 && !small; seq++) {
            small = random.message(new Tag(5, 0, seq)).message().payload().length < 100;
        }
        assertTrue(small, "로그 균등이라 0에 가까운 크기도 나온다");
    }
}
