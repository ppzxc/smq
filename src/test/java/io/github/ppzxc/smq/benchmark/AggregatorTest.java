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
        Map<String, Double> m = Aggregator.aggregate(
                spec,
                List.of(
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
        Map<String, Double> m = Aggregator.aggregate(
                spec,
                List.of(
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
        assertThrows(
                IllegalStateException.class,
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

    @Test
    void roundsAreCollectedPerKeyInRoundOrder() {
        Map<String, List<Double>> samples =
                Aggregator.samples(List.of(Map.of("a", 1.0, "b", 10.0), Map.of("a", 2.0), Map.of("a", 3.0, "b", 30.0)));
        assertEquals(List.of(1.0, 2.0, 3.0), samples.get("a"));
        assertEquals(List.of(10.0, 30.0), samples.get("b"));
    }

    @Test
    void medianOfOddAndEvenSamples() {
        assertEquals(3.0, Aggregator.median(List.of(5.0, 1.0, 3.0)));
        assertEquals(2.5, Aggregator.median(List.of(4.0, 1.0, 2.0, 3.0)));
        assertEquals(7.0, Aggregator.median(List.of(7.0)));
        assertThrows(IllegalArgumentException.class, () -> Aggregator.median(List.of()));
    }

    @Test
    void representativeValuesAreMediansAndOnlyRepeatedEntriesKeepSamples() {
        Map<String, List<Double>> samples = new java.util.LinkedHashMap<>();
        samples.put("rep", List.of(9.0, 1.0, 5.0));
        samples.put("once", List.of(4.0));
        Report r = Aggregator.report(Map.of("os", "x"), samples);
        assertEquals(5.0, r.entries().get("rep"));
        assertEquals(4.0, r.entries().get("once"));
        assertEquals(Map.of("rep", List.of(9.0, 1.0, 5.0)), r.samples());
    }
}
