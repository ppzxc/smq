package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RunParamsTest {

    private static final Limits FULL = Limits.full(10_000);

    @Test
    void sameSeedGivesSameParams() {
        assertEquals(RunParams.fromSeed(42, FULL), RunParams.fromSeed(42, FULL));
        assertNotEquals(RunParams.fromSeed(42, FULL), RunParams.fromSeed(43, FULL));
    }

    @Test
    void everySeedStaysInsideTheSpecTable() {
        int single = 0;
        int withMax = 0;
        int withPrefill = 0;
        Set<Durability> durabilities = EnumSet.noneOf(Durability.class);
        for (long seed = 0; seed < 3_000; seed++) {
            RunParams p = RunParams.fromSeed(seed, FULL);
            assertEquals(1, p.processes());
            assertEquals(p.processes(), p.producers().size());
            assertEquals(p.processes(), p.consumers().size());
            assertTrue(p.maxMessageBytes() >= 0 && p.maxMessageBytes() <= 65_536, "size " + seed);
            assertTrue(p.publishBatch() >= 1 && p.publishBatch() <= 500);
            assertTrue(p.chunkSize() >= 1 && p.chunkSize() <= 500);
            assertTrue(p.producers().stream().allMatch(n -> n >= (p.processes() == 1 ? 1 : 0) && n <= 4));
            assertTrue(p.consumers().stream().allMatch(n -> n >= (p.processes() == 1 ? 1 : 0) && n <= 4));
            assertTrue(p.totalProducers() >= 1 && p.totalConsumers() >= 1, "전체에서 각각 1 이상");
            assertTrue(p.prefillDepth() == 0 || p.prefillDepth() >= 1 && p.prefillDepth() <= 100_000);
            assertTrue(p.ackRatio() > 0.5, "ACK가 다수");
            assertEquals(1.0, p.ackRatio() + p.nackRatio() + p.rejectRatio() + p.noneRatio(), 1e-9);
            assertTrue(p.leaseMillis() >= 200 && p.leaseMillis() <= 10_000);
            assertTrue(p.publishDelayRatio() >= 0 && p.publishDelayRatio() <= 0.3);
            assertTrue(p.nackDelayRatio() >= 0 && p.nackDelayRatio() <= 0.3);
            assertTrue(p.publishDelayMaxMillis() <= 2_000 && p.nackDelayMaxMillis() <= 2_000);
            assertTrue(p.maxDeliveries() >= 0 && p.maxDeliveries() <= 5);
            assertTrue(p.pullWaitMillis() >= 0 && p.pullWaitMillis() <= 500);
            assertTrue(p.slowChunkRatio() <= 0.3);
            durabilities.add(p.durability());
            single += p.singleActiveConsumer() ? 1 : 0;
            withMax += p.maxDeliveries() > 0 ? 1 : 0;
            withPrefill += p.prefillDepth() > 0 ? 1 : 0;
        }
        assertEquals(EnumSet.allOf(Durability.class), durabilities);
        assertTrue(single > 450 && single < 750, "단일 활성 컨슈머는 약 20%: " + single);
        assertTrue(withMax > 1_200 && withMax < 1_800, "최대 전달 횟수는 약 절반: " + withMax);
        assertTrue(withPrefill > 0 && withPrefill < 3_000);
    }

    @Test
    void 고정_속도_모드는_목표_속도를_싣고_운영에_가까운_매개변수로_바꾼다() {
        Limits soak = FULL.withRate(500);
        for (long seed = 0; seed < 300; seed++) {
            RunParams p = RunParams.fromSeed(seed, soak);
            assertEquals(500, p.targetRate(), "seed " + seed);
            assertEquals(0, p.publishDelayRatio(), 0, "지연 발행 없음");
            assertEquals(0, p.slowChunkRatio(), 0, "느린 청크 없음");
            assertEquals(0, p.nackDelayRatio(), 0, "NACK 지연 없음");
            assertFalse(p.singleActiveConsumer(), "단일 활성 큐는 청크 하나가 리스 만료까지 큐를 막아 초당 수십 건이 한계다");
            assertTrue(
                    p.chunkSize() >= RunParams.SOAK_MIN_CHUNK_SIZE, "컨슈머 스레드 하나가 500/s를 받을 만큼의 청크: " + p.chunkSize());
            assertEquals(FULL.maxDepth(), p.prefillDepth(), "한도의 깊이 상한까지 미리 채워 쌓인 큐에서 시작한다");
            assertTrue(p.maxMessageBytes() <= 1024, "메시지 크기 상한 1KB");
            long perThread = Math.max(1, (500 + p.totalProducers() - 1) / p.totalProducers());
            long lowest = Math.min(RunParams.SOAK_MIN_PUBLISH_BATCH, perThread);
            assertTrue(p.publishBatch() >= lowest, "배치는 건당에 가깝지 않게 20 이상(스레드 몫이 작으면 그 몫): " + p.publishBatch());
            assertTrue(
                    p.publishBatch() <= Math.max(lowest, perThread / RunParams.SOAK_BATCH_WINDOW_DIVISOR),
                    "배치는 스레드 몫의 0.25초 분량 이하: " + p.publishBatch());
            assertEquals(p, RunParams.fromArg(p.toArg()), "seed " + seed);
        }
    }

    @Test
    void 속도가_없으면_기존_매개변수와_같고_목표_속도는_0이다() {
        for (long seed = 0; seed < 50; seed++) {
            assertEquals(0, RunParams.fromSeed(seed, FULL).targetRate());
        }
    }

    @Test
    void 같은_시드와_속도는_같은_매개변수다() {
        assertEquals(RunParams.fromSeed(7, FULL.withRate(500)), RunParams.fromSeed(7, FULL.withRate(500)));
    }

    @Test
    void argRoundTripKeepsEveryFieldExactly() {
        for (long seed = 0; seed < 300; seed++) {
            RunParams p = RunParams.fromSeed(seed, FULL);
            assertEquals(p, RunParams.fromArg(p.toArg()), "seed " + seed);
        }
    }

    @Test
    void argRoundTripKeepsSeedsBeyondDoublePrecision() {
        for (long seed : new long[] {Long.MAX_VALUE, Long.MIN_VALUE + 1, 9_007_199_254_740_993L}) {
            assertEquals(
                    seed,
                    RunParams.fromArg(RunParams.fromSeed(seed, FULL).toArg()).seed());
        }
    }

    @Test
    void smokeLimitsCapDepthAndLease() {
        for (long seed = 0; seed < 500; seed++) {
            RunParams p = RunParams.fromSeed(seed, Limits.smoke(2_000));
            assertTrue(p.prefillDepth() <= 2_000);
            assertTrue(p.leaseMillis() <= 1_000);
            assertEquals(2_000, p.loadMillis());
        }
    }

    @Test
    void prefillIsBoundedByEstimatedBytes() {
        for (long seed = 0; seed < 3_000; seed++) {
            RunParams p = RunParams.fromSeed(seed, FULL);
            assertTrue((long) p.prefillDepth() * (p.maxMessageBytes() / 4 + 64) <= 256L * 1024 * 1024);
        }
    }

    @Test
    void describeListsEveryParameterForFailureReports() {
        String text = RunParams.fromSeed(7, FULL).describe();
        assertTrue(text.contains("seed = 7"));
        assertTrue(text.contains("leaseMillis = "));
        assertTrue(text.contains("singleActiveConsumer = "));
    }

    @Test
    void smokeSeedsTogetherCoverTheInterestingAxes() {
        boolean single = false;
        boolean max = false;
        boolean reject = false;
        boolean publishDelay = false;
        boolean nackDelay = false;
        boolean prefill = false;
        Set<Durability> durabilities = EnumSet.noneOf(Durability.class);
        for (long seed : Options.SMOKE_SEEDS) {
            RunParams p = RunParams.fromSeed(seed, Limits.smoke(Options.SMOKE_LOAD.toMillis()));
            single |= p.singleActiveConsumer();
            max |= p.maxDeliveries() > 0;
            reject |= p.rejectRatio() > 0;
            publishDelay |= p.publishDelayRatio() > 0 && p.publishDelayMaxMillis() > 0;
            nackDelay |= p.nackDelayRatio() > 0 && p.nackDelayMaxMillis() > 0;
            prefill |= p.prefillDepth() > 0;
            durabilities.add(p.durability());
        }
        assertTrue(single && max && reject && publishDelay && nackDelay && prefill);
        assertEquals(EnumSet.allOf(Durability.class), durabilities);
    }

    @Test
    void 메시지_크기와_깊이와_깊은_큐_옵션은_고정_속도_모드에서만_먹는다() {
        RunParams def = RunParams.fromSeed(7, FULL.withRate(500));
        assertEquals(1024, def.maxMessageBytes());
        assertEquals(512, def.minMessageBytes());
        assertFalse(def.consumerPaced());

        RunParams big = RunParams.fromSeed(7, FULL.withRate(500).withMessageBytes(4096));
        assertEquals(4096, big.maxMessageBytes());
        assertEquals(2048, big.minMessageBytes());

        RunParams deep = RunParams.fromSeed(7, FULL.withRate(500).withDepth(300_000));
        assertEquals(300_000, deep.prefillDepth(), "256MB 한도 안이면 요청한 깊이 그대로(평균 832바이트)");

        RunParams standing = RunParams.fromSeed(7, FULL.withRate(500).withStandingQueue(true));
        assertTrue(standing.consumerPaced());
        assertEquals(RunParams.fromArg(standing.toArg()), standing);

        RunParams random = RunParams.fromSeed(7, FULL.withStandingQueue(true).withMessageBytes(4096));
        assertFalse(random.consumerPaced(), "목표 속도가 없으면 소비를 조절하지 않는다");
        assertEquals(0, random.minMessageBytes());
    }

    @Test
    void 큰_메시지로_깊이를_늘려도_미리_채우기는_데이터_상한을_넘지_않는다() {
        RunParams p = RunParams.fromSeed(
                7, FULL.withRate(500).withMessageBytes(60_000).withDepth(5_000_000));
        long avg = RunParams.avgMessageBytes(p.minMessageBytes(), p.maxMessageBytes());
        assertTrue(p.prefillDepth() * avg <= 256L * 1024 * 1024, "미리 채운 데이터 추정: " + p.prefillDepth() * avg);
        assertTrue(p.prefillDepth() > 0);
    }

    @Test
    void 메시지_크기_1도_하한이_0이_되지_않아_로그_균등으로_돌아가지_않는다() {
        RunParams p = RunParams.fromSeed(7, FULL.withRate(500).withMessageBytes(1));
        assertEquals(1, p.maxMessageBytes());
        assertEquals(1, p.minMessageBytes(), "0은 '하한 없음'(무작위 모드)이라는 뜻이라 쓰면 안 된다");
        Script script = new Script(p);
        for (long seq = 0; seq < 50; seq++) {
            assertEquals(1, script.message(new Tag(7, 0, seq)).message().payload().length);
        }
    }
}
