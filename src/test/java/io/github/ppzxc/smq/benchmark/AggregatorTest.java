package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregatorTest {

    private static final long SEC = 1_000_000_000L;

    @Test
    void queueKindsHaveSeparateThroughputEntries() {
        for (final RunSpec.QueueKind kind : List.of(
                RunSpec.QueueKind.MAX_DELIVERIES,
                RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER,
                RunSpec.QueueKind.LEASED_HEAD)) {
            final RunSpec spec;
            final Map<String, Double> first;
            final String key;
            spec = RunSpec.s1(kind, RunSpec.DEEP, 1000, 0);
            first = Aggregator.aggregate(spec, List.of(new ChildResult("WORKER", 500, SEC, new long[0])));
            key = "sqlite/S1/P1/PROCESS_CRASH/1m/" + kind + ":msgPerSec";
            assertEquals(Map.of(key, 500.0), first, "First should match the expected value.");
        }
    }

    @Test
    void queueKindReportUsesMedianThroughput() {
        for (final RunSpec.QueueKind kind : List.of(
                RunSpec.QueueKind.MAX_DELIVERIES,
                RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER,
                RunSpec.QueueKind.LEASED_HEAD)) {
            final RunSpec spec;
            final Map<String, Double> first;
            final String key;
            final Report report;
            spec = RunSpec.s1(kind, RunSpec.DEEP, 1000, 0);
            first = Aggregator.aggregate(spec, List.of(new ChildResult("WORKER", 500, SEC, new long[0])));
            key = "sqlite/S1/P1/PROCESS_CRASH/1m/" + kind + ":msgPerSec";
            report = Aggregator.report(Map.of(), Aggregator.samples(List.of(first, Map.of(key, 300.0))));
            assertEquals(400.0, report.entries().get(key), "Entries should match the expected value.");
        }
    }

    @Test
    void queueKindReportRetainsSamples() {
        for (final RunSpec.QueueKind kind : List.of(
                RunSpec.QueueKind.MAX_DELIVERIES,
                RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER,
                RunSpec.QueueKind.LEASED_HEAD)) {
            final RunSpec spec;
            final Map<String, Double> first;
            final String key;
            final Report report;
            spec = RunSpec.s1(kind, RunSpec.DEEP, 1000, 0);
            first = Aggregator.aggregate(spec, List.of(new ChildResult("WORKER", 500, SEC, new long[0])));
            key = "sqlite/S1/P1/PROCESS_CRASH/1m/" + kind + ":msgPerSec";
            report = Aggregator.report(Map.of(), Aggregator.samples(List.of(first, Map.of(key, 300.0))));
            assertEquals(List.of(500.0, 300.0), report.samples().get(key), "Samples should match the expected value.");
        }
    }

    @Test
    void s2UsesTheSameThroughputMetric() {
        final RunSpec spec;
        final Map<String, Double> m;
        spec = RunSpec.s2(Storage.SQLITE, Profile.P3, 1000, 0);
        m = Aggregator.aggregate(spec, List.of(new ChildResult("WORKER", 500, SEC, new long[0])));
        assertEquals(
                Map.of("sqlite/S2/P3/POWER_LOSS/empty:msgPerSec", 500.0),
                m,
                "The map entry should match the expected value.");
    }

    @Test
    void s3MedianLatencyIsInMilliseconds() {
        final RunSpec spec;
        final long[] samples;
        final Map<String, Double> m;
        spec = RunSpec.s3(Storage.SQLITE, Profile.P1, 1000, 0);
        samples = new long[100];
        for (int i = 0; i < 100; i++) {
            samples[i] = (i + 1) * 1000L;
        }
        m = Aggregator.aggregate(spec, List.of(new ChildResult("BOTH", 100, SEC, samples)));
        assertEquals(
                50.0, m.get("sqlite/S3/P1/POWER_LOSS/empty:p50Ms"), "The map entry should match the expected value.");
    }

    @Test
    void s3TailLatencyIsInMilliseconds() {
        final RunSpec spec;
        final long[] samples;
        final Map<String, Double> m;
        spec = RunSpec.s3(Storage.SQLITE, Profile.P1, 1000, 0);
        samples = new long[100];
        for (int i = 0; i < 100; i++) {
            samples[i] = (i + 1) * 1000L;
        }
        m = Aggregator.aggregate(spec, List.of(new ChildResult("BOTH", 100, SEC, samples)));
        assertEquals(
                99.0, m.get("sqlite/S3/P1/POWER_LOSS/empty:p99Ms"), "The map entry should match the expected value.");
    }

    @Test
    void medianPercentileUsesNearestRank() {
        final long[] sorted;
        sorted = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertEquals(5, Aggregator.percentile(sorted, 50), "Percentile should match the expected value.");
    }

    @Test
    void highPercentileUsesNearestRank() {
        final long[] sorted;
        sorted = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertEquals(10, Aggregator.percentile(sorted, 99), "Percentile should match the expected value.");
    }

    @Test
    void lowPercentileUsesNearestRank() {
        final long[] sorted;
        sorted = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertEquals(1, Aggregator.percentile(sorted, 1), "Percentile should match the expected value.");
    }

    @Test
    void percentileRejectsEmptySamples() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Aggregator.percentile(new long[0], 50),
                "Percentile should throw IllegalArgumentException.");
    }

    @Test
    void s3WithoutSamplesIsAnError() {
        final RunSpec spec;
        spec = RunSpec.s3(Storage.SQLITE, Profile.P1, 1000, 0);
        assertThrows(
                IllegalStateException.class,
                () -> Aggregator.aggregate(spec, List.of(new ChildResult("BOTH", 0, SEC, new long[0]))),
                "Aggregate should throw IllegalStateException.");
    }

    @Test
    void childResultJsonIsSingleLine() {
        final ChildResult r;
        final String line;
        r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        line = r.toJson();
        assertFalse(line.contains("\n"), "Line should not contain the expected value.");
    }

    @Test
    void childResultJsonPreservesRole() {
        final ChildResult r;
        final String line;
        final ChildResult back;
        r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        line = r.toJson();
        back = ChildResult.fromJson(line);
        assertEquals("CONSUMER", back.role(), "Role should match the expected value.");
    }

    @Test
    void childResultJsonPreservesMessageCount() {
        final ChildResult r;
        final String line;
        final ChildResult back;
        r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        line = r.toJson();
        back = ChildResult.fromJson(line);
        assertEquals(7, back.messages(), "Messages should match the expected value.");
    }

    @Test
    void childResultJsonPreservesElapsedTime() {
        final ChildResult r;
        final String line;
        final ChildResult back;
        r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        line = r.toJson();
        back = ChildResult.fromJson(line);
        assertEquals(99, back.elapsedNanos(), "Elapsed nanos should match the expected value.");
    }

    @Test
    void childResultJsonPreservesLatencies() {
        final ChildResult r;
        final String line;
        final ChildResult back;
        r = new ChildResult("CONSUMER", 7, 99, new long[] {5, 6});
        line = r.toJson();
        back = ChildResult.fromJson(line);
        assertArrayEquals(
                new long[] {5, 6}, back.latenciesMicros(), "Latencies micros should match the expected value.");
    }

    @Test
    void roundsForFirstKeyStayInOrder() {
        final Map<String, List<Double>> samples;
        samples =
                Aggregator.samples(List.of(Map.of("a", 1.0, "b", 10.0), Map.of("a", 2.0), Map.of("a", 3.0, "b", 30.0)));
        assertEquals(List.of(1.0, 2.0, 3.0), samples.get("a"), "Samples should match the expected value.");
    }

    @Test
    void roundsForSecondKeyStayInOrder() {
        final Map<String, List<Double>> samples;
        samples =
                Aggregator.samples(List.of(Map.of("a", 1.0, "b", 10.0), Map.of("a", 2.0), Map.of("a", 3.0, "b", 30.0)));
        assertEquals(List.of(10.0, 30.0), samples.get("b"), "Samples should match the expected value.");
    }

    @Test
    void medianOfOddSample() {
        assertEquals(3.0, Aggregator.median(List.of(5.0, 1.0, 3.0)), "Median should match the expected value.");
    }

    @Test
    void medianOfEvenSample() {
        assertEquals(2.5, Aggregator.median(List.of(4.0, 1.0, 2.0, 3.0)), "Median should match the expected value.");
    }

    @Test
    void medianOfSingleSample() {
        assertEquals(7.0, Aggregator.median(List.of(7.0)), "Median should match the expected value.");
    }

    @Test
    void medianRejectsEmptySample() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Aggregator.median(List.of()),
                "Median should throw IllegalArgumentException.");
    }

    @Test
    void representativeValueUsesMedian() {
        final Map<String, List<Double>> samples;
        final Report r;
        samples = new java.util.LinkedHashMap<>();
        samples.put("rep", List.of(9.0, 1.0, 5.0));
        samples.put("once", List.of(4.0));
        r = Aggregator.report(Map.of("os", "x"), samples);
        assertEquals(5.0, r.entries().get("rep"), "Entries should match the expected value.");
    }

    @Test
    void singleValueRemainsRepresentative() {
        final Map<String, List<Double>> samples;
        final Report r;
        samples = new java.util.LinkedHashMap<>();
        samples.put("rep", List.of(9.0, 1.0, 5.0));
        samples.put("once", List.of(4.0));
        r = Aggregator.report(Map.of("os", "x"), samples);
        assertEquals(4.0, r.entries().get("once"), "Entries should match the expected value.");
    }

    @Test
    void reportRetainsOnlyRepeatedSamples() {
        final Map<String, List<Double>> samples;
        final Report r;
        samples = new java.util.LinkedHashMap<>();
        samples.put("rep", List.of(9.0, 1.0, 5.0));
        samples.put("once", List.of(4.0));
        r = Aggregator.report(Map.of("os", "x"), samples);
        assertEquals(Map.of("rep", List.of(9.0, 1.0, 5.0)), r.samples(), "Samples should match the expected value.");
    }
}
