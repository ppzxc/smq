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
        Set<Integer> processCounts = new java.util.HashSet<>();
        for (long seed = 0; seed < 3_000; seed++) {
            RunParams p = RunParams.fromSeed(seed, FULL);
            assertTrue(p.processes() >= 1 && p.processes() <= 4);
            assertEquals(p.processes(), p.producers().size());
            assertEquals(p.processes(), p.consumers().size());
            assertTrue(p.maxMessageBytes() >= 0 && p.maxMessageBytes() <= 65_536, "size " + seed);
            assertTrue(p.publishBatch() >= 1 && p.publishBatch() <= 500);
            assertTrue(p.chunkSize() >= 1 && p.chunkSize() <= 500);
            assertTrue(p.producers().stream().allMatch(n -> n >= (p.processes() == 1 ? 1 : 0) && n <= 4));
            assertTrue(p.consumers().stream().allMatch(n -> n >= (p.processes() == 1 ? 1 : 0) && n <= 4));
            assertTrue(p.totalProducers() >= 1 && p.totalConsumers() >= 1, "전체에서 각각 1 이상");
            processCounts.add(p.processes());
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
        assertEquals(Set.of(1, 2, 3, 4), processCounts);
        assertTrue(single > 450 && single < 750, "단일 활성 컨슈머는 약 20%: " + single);
        assertTrue(withMax > 1_200 && withMax < 1_800, "최대 전달 횟수는 약 절반: " + withMax);
        assertTrue(withPrefill > 0 && withPrefill < 3_000);
    }

    @Test
    void argRoundTripKeepsEveryFieldExactly() {
        for (long seed = 0; seed < 300; seed++) {
            RunParams p = RunParams.fromSeed(seed, FULL);
            assertEquals(p, RunParams.fromArg(p.toArg()), "seed " + seed);
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
        boolean multiProcess = false;
        Set<Durability> durabilities = EnumSet.noneOf(Durability.class);
        for (long seed : Options.SMOKE_SEEDS) {
            RunParams p = RunParams.fromSeed(seed, Limits.smoke(Options.SMOKE_LOAD.toMillis()));
            single |= p.singleActiveConsumer();
            max |= p.maxDeliveries() > 0;
            reject |= p.rejectRatio() > 0;
            publishDelay |= p.publishDelayRatio() > 0 && p.publishDelayMaxMillis() > 0;
            nackDelay |= p.nackDelayRatio() > 0 && p.nackDelayMaxMillis() > 0;
            prefill |= p.prefillDepth() > 0;
            multiProcess |= p.processes() > 1;
            durabilities.add(p.durability());
        }
        assertTrue(single && max && reject && publishDelay && nackDelay && prefill);
        assertTrue(multiProcess, "스모크 고정 시드에 다중 프로세스 실행이 하나 이상 있어야 한다");
        assertEquals(EnumSet.allOf(Durability.class), durabilities);
    }
}
