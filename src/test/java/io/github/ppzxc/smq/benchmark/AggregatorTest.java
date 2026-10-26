package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregatorTest {

    private static final long SEC = 1_000_000_000L;

    @Test
    void throughputSumsMessagesOverTheSlowestChild() {
        RunSpec spec = RunSpec.s1(Storage.SQLITE, Profile.P1, 2, Durability.POWER_LOSS, 0, 1000, 0);
        Map<String, Double> m = Aggregator.aggregate(spec, List.of(
                new ChildResult("WORKER", 10_000, 2 * SEC, new long[0]),
                new ChildResult("WORKER", 30_000, 4 * SEC, new long[0])));
        assertEquals(Map.of("sqlite/S1/P1/proc2/POWER_LOSS/empty:msgPerSec", 10_000.0), m);
    }

    @Test
    void s2UsesTheSameThroughputMetric() {
        RunSpec spec = RunSpec.s2(Storage.SQLITE, Profile.P2, 1000, 0);
        Map<String, Double> m = Aggregator.aggregate(spec, List.of(new ChildResult("WORKER", 500, SEC, new long[0])));
        assertEquals(Map.of("sqlite/S2/P2/proc1/POWER_LOSS/empty:msgPerSec", 500.0), m);
    }

    @Test
    void latencyPercentilesComeFromMergedSamplesInMilliseconds() {
        RunSpec spec = RunSpec.s3(Storage.SQLITE, Profile.P1, Topology.CROSS_PROCESS, 1000, 0);
        long[] a = new long[50];
        long[] b = new long[50];
        for (int i = 0; i < 50; i++) {
            a[i] = (2L * i + 1) * 1000; // 1,3,5..99 ms
            b[i] = (2L * i + 2) * 1000; // 2,4,6..100 ms
        }
        Map<String, Double> m = Aggregator.aggregate(spec, List.of(
                new ChildResult("PRODUCER", 100, SEC, new long[0]),
                new ChildResult("CONSUMER", 100, SEC, a),
                new ChildResult("CONSUMER", 100, SEC, b)));
        assertEquals(50.0, m.get("sqlite/S3/P1/cross-process/POWER_LOSS/empty:p50Ms"));
        assertEquals(99.0, m.get("sqlite/S3/P1/cross-process/POWER_LOSS/empty:p99Ms"));
    }

    @Test
    void percentileIsNearestRank() {
        long[] sorted = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertEquals(5, Aggregator.percentile(sorted, 50));
        assertEquals(10, Aggregator.percentile(sorted, 99));
        assertEquals(1, Aggregator.percentile(sorted, 1));
        assertThrows(IllegalArgumentException.class, () -> Aggregator.percentile(new long[0], 50));
    }

    @Test
    void s3WithoutSamplesIsAnError() {
        RunSpec spec = RunSpec.s3(Storage.SQLITE, Profile.P1, Topology.SAME_PROCESS, 1000, 0);
        assertThrows(IllegalStateException.class,
                () -> Aggregator.aggregate(spec, List.of(new ChildResult("BOTH", 0, SEC, new long[0]))));
    }

    @Test
    void childResultRoundTripsThroughOneLineOfJson() {
        ChildResult r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        String line = r.toJson();
        assertFalse(line.contains("\n"));
        ChildResult back = ChildResult.fromJson(line);
        assertEquals("CONSUMER", back.role());
        assertEquals(7, back.messages());
        assertEquals(99, back.elapsedNanos());
        assertArrayEquals(new long[] {5, 6}, back.latenciesMicros());
    }
}
