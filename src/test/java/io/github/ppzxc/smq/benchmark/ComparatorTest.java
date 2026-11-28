package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.Comparator.Check;
import io.github.ppzxc.smq.benchmark.Comparator.Status;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComparatorTest {

    private static final String S1P1 = "sqlite/S1/P1/POWER_LOSS/empty:msgPerSec";

    private static Check find(final List<Check> checks, final String name) {
        return checks.stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("검사 없음: " + name + " in " + checks));
    }

    private static List<Check> compare(final Map<String, Double> r, final Map<String, Double> baseline) {
        final Map<String, List<Double>> samples;
        samples = new LinkedHashMap<>();
        r.forEach((k, v) -> samples.put(k, List.of(v)));
        return Comparator.compare(samples, baseline);
    }

    private static final String EMPTY_P1 = "sqlite/S1/P1/POWER_LOSS/empty:msgPerSec";
    private static final String DEEP_P1 = "sqlite/S1/P1/POWER_LOSS/1m:msgPerSec";
    private static final String DEPTH_P1 = "depth S1 P1 POWER_LOSS 1m >= 70% of empty";

    private static Check ratioCheck(final List<Double> deep, final List<Double> empty) {
        return find(Comparator.compare(Map.of(DEEP_P1, deep, EMPTY_P1, empty), Map.of()), DEPTH_P1);
    }

    @Test
    void queueKindsFailBelowMedianRatio() {
        for (final String kind : List.of("MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD")) {
            final String prefix;
            final Check c;
            prefix = "sqlite/S1/P1/PROCESS_CRASH/";
            c = find(
                    Comparator.compare(
                            Map.of(
                                    prefix + "empty/" + kind + ":msgPerSec",
                                    List.of(100.0, 100.0, 100.0),
                                    prefix + "1m/" + kind + ":msgPerSec",
                                    List.of(10.0, 30.0, 20.0)),
                            Map.of()),
                    "depth S1 P1 PROCESS_CRASH " + kind + " 1m >= 70% of empty");
            assertEquals(Status.FAIL, c.status(), "Status should match the expected value.");
        }
    }

    @Test
    void queueKindsReportMedianRatio() {
        for (final String kind : List.of("MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD")) {
            final String prefix;
            final Check c;
            prefix = "sqlite/S1/P1/PROCESS_CRASH/";
            c = find(
                    Comparator.compare(
                            Map.of(
                                    prefix + "empty/" + kind + ":msgPerSec",
                                    List.of(100.0, 100.0, 100.0),
                                    prefix + "1m/" + kind + ":msgPerSec",
                                    List.of(10.0, 30.0, 20.0)),
                            Map.of()),
                    "depth S1 P1 PROCESS_CRASH " + kind + " 1m >= 70% of empty");
            assertTrue(c.detail().contains("20.0%"), "Detail should contain the expected value.");
        }
    }

    @Test
    void medianRatioPassesAboveFloor() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 7400.0, 8000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertEquals(Status.PASS, c.status(), "Status should match the expected value.");
    }

    @Test
    void medianRatioDetailShowsMedian() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 7400.0, 8000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertTrue(c.detail().contains("74.0%"), c.detail());
    }

    @Test
    void medianRatioFailsBelowFloor() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 6500.0, 9000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertEquals(Status.FAIL, c.status(), "Status should match the expected value.");
    }

    @Test
    void medianRatioDetailShowsBelowFloorValue() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 6500.0, 9000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertTrue(c.detail().contains("65.0%"), c.detail());
    }

    @Test
    void evenRoundMedianPassesAtFloor() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 8000.0), List.of(10_000.0, 10_000.0));
        assertEquals(Status.PASS, c.status(), "Status should match the expected value.");
    }

    @Test
    void evenRoundMedianAveragesMiddleValues() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 8000.0), List.of(10_000.0, 10_000.0));
        assertTrue(c.detail().contains("70.0%"), c.detail());
    }

    @Test
    void exactlySeventyPercentPasses() {
        assertEquals(
                Status.PASS,
                ratioCheck(List.of(7000.0, 7000.0, 7000.0), List.of(10_000.0, 10_000.0, 10_000.0))
                        .status(),
                "Status should match the expected value.");
    }

    @Test
    void pairedRoundMedianPasses() {
        final Check c;
        c = ratioCheck(List.of(10.0, 50.0, 90.0), List.of(100.0, 50.0, 100.0));
        assertEquals(Status.PASS, c.status(), "Status should match the expected value.");
    }

    @Test
    void pairedRoundMedianAppearsInDetail() {
        final Check c;
        c = ratioCheck(List.of(10.0, 50.0, 90.0), List.of(100.0, 50.0, 100.0));
        assertTrue(c.detail().contains("90.0%"), c.detail());
    }

    @Test
    void ratioDetailIncludesFirstRound() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 7400.0, 8000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertTrue(c.detail().contains("6000 / 10000 = 60.0%"), c.detail());
    }

    @Test
    void ratioDetailIncludesSecondRound() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 7400.0, 8000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertTrue(c.detail().contains("7400 / 10000 = 74.0%"), c.detail());
    }

    @Test
    void ratioDetailIncludesThirdRound() {
        final Check c;
        c = ratioCheck(List.of(6000.0, 7400.0, 8000.0), List.of(10_000.0, 10_000.0, 10_000.0));
        assertTrue(c.detail().contains("8000 / 10000 = 80.0%"), c.detail());
    }

    @Test
    void singleSampleKeepsTheOldDetail() {
        final Check c;
        c = ratioCheck(List.of(7000.0), List.of(10_000.0));
        assertEquals("7000 / 10000 = 70.0%", c.detail(), "Detail should match the expected value.");
    }

    @Test
    void mismatchedSampleCountsFail() {
        final Check c;
        c = ratioCheck(List.of(7000.0, 7000.0, 7000.0), List.of(10_000.0, 10_000.0));
        assertEquals(Status.FAIL, c.status(), "Status should match the expected value.");
    }

    @Test
    void mismatchedSampleCountsAppearInDetail() {
        final Check c;
        c = ratioCheck(List.of(7000.0, 7000.0, 7000.0), List.of(10_000.0, 10_000.0));
        assertTrue(c.detail().contains("표본 수"), c.detail());
    }

    @Test
    void singleReferenceSamplePairsWithFirstRound() {
        assertEquals(
                Status.PASS,
                ratioCheck(List.of(7000.0, 1.0), List.of(10_000.0)).status(),
                "Status should match the expected value.");
    }

    @Test
    void singleMeasuredSamplePairsWithFirstRound() {
        assertEquals(
                Status.PASS,
                ratioCheck(List.of(7000.0), List.of(10_000.0, 1.0)).status(),
                "Status should match the expected value.");
    }

    @Test
    void ratioIsSkippedWhenMeasuredSamplesAreMissing() {
        assertEquals(
                Status.SKIP,
                ratioCheck(List.of(), List.of(10_000.0)).status(),
                "Status should match the expected value.");
    }

    @Test
    void ratioIsSkippedWhenReferenceSamplesAreMissing() {
        assertEquals(
                Status.SKIP,
                find(Comparator.compare(Map.of(DEEP_P1, List.of(7000.0)), Map.of()), DEPTH_P1)
                        .status(),
                "Status should match the expected value.");
    }

    @Test
    void medianFloorPasses() {
        final String p1;
        final Map<String, List<Double>> r;
        final List<Check> c;
        p1 = S1P1;
        r = Map.of(p1, List.of(9000.0, 16_000.0, 10_500.0, 20_000.0));
        c = Comparator.compare(r, Map.of(p1, 16_000.0));
        assertEquals(
                Status.PASS,
                find(c, "floor S1 P1 POWER_LOSS >= 10000 msg/s").status(),
                "Status should match the expected value.");
    }

    @Test
    void medianFloorDetailUsesMedian() {
        final String p1;
        final Map<String, List<Double>> r;
        final List<Check> c;
        p1 = S1P1;
        r = Map.of(p1, List.of(9000.0, 16_000.0, 10_500.0, 20_000.0));
        c = Comparator.compare(r, Map.of(p1, 16_000.0));
        assertTrue(
                find(c, "floor S1 P1 POWER_LOSS >= 10000 msg/s").detail().contains("13250"),
                "Detail should contain the expected value.");
    }

    @Test
    void medianRegressionDetailUsesMedian() {
        final String p1;
        final Map<String, List<Double>> r;
        final List<Check> c;
        p1 = S1P1;
        r = Map.of(p1, List.of(9000.0, 16_000.0, 10_500.0, 20_000.0));
        c = Comparator.compare(r, Map.of(p1, 16_000.0));
        assertTrue(find(c, "regression " + p1).detail().contains("13250"), "Detail should contain the expected value.");
    }

    @Test
    void medianRegressionPasses() {
        final String p1;
        final Map<String, List<Double>> r;
        final List<Check> c;
        p1 = S1P1;
        r = Map.of(p1, List.of(9000.0, 16_000.0, 10_500.0, 20_000.0));
        c = Comparator.compare(r, Map.of(p1, 16_000.0));
        assertEquals(Status.PASS, find(c, "regression " + p1).status(), "Status should match the expected value.");
    }

    @Test
    void firstAbsoluteFloorPasses() {
        final Map<String, Double> r;
        final List<Check> c;
        r = new HashMap<>();
        r.put(S1P1, 10_000.0);
        r.put("sqlite/S1/P3/POWER_LOSS/empty:msgPerSec", 7_999.0);
        r.put("sqlite/S1/P7/POWER_LOSS/empty:msgPerSec", 4_000.0);
        c = compare(r, Map.of());
        assertEquals(
                Status.PASS,
                find(c, "floor S1 P1 POWER_LOSS >= 10000 msg/s").status(),
                "Status should match the expected value.");
    }

    @Test
    void secondAbsoluteFloorFails() {
        final Map<String, Double> r;
        final List<Check> c;
        r = new HashMap<>();
        r.put(S1P1, 10_000.0);
        r.put("sqlite/S1/P3/POWER_LOSS/empty:msgPerSec", 7_999.0);
        r.put("sqlite/S1/P7/POWER_LOSS/empty:msgPerSec", 4_000.0);
        c = compare(r, Map.of());
        assertEquals(
                Status.FAIL,
                find(c, "floor S1 P3 POWER_LOSS >= 8000 msg/s").status(),
                "Status should match the expected value.");
    }

    @Test
    void thirdAbsoluteFloorPasses() {
        final Map<String, Double> r;
        final List<Check> c;
        r = new HashMap<>();
        r.put(S1P1, 10_000.0);
        r.put("sqlite/S1/P3/POWER_LOSS/empty:msgPerSec", 7_999.0);
        r.put("sqlite/S1/P7/POWER_LOSS/empty:msgPerSec", 4_000.0);
        c = compare(r, Map.of());
        assertEquals(
                Status.PASS,
                find(c, "floor S1 P7 POWER_LOSS >= 4000 msg/s").status(),
                "Status should match the expected value.");
    }

    @Test
    void unmeasuredChecksAreSkipped() {
        final List<Check> c;
        c = compare(Map.of(), Map.of());
        assertTrue(c.stream().allMatch(x -> x.status() == Status.SKIP), "All checks should be skipped.");
    }

    @Test
    void unmeasuredChecksAreReported() {
        final List<Check> c;
        c = compare(Map.of(), Map.of());
        assertFalse(c.isEmpty(), "Checks should not be empty.");
    }

    @Test
    void depthMustKeepSeventyPercentOfEmpty() {
        final Map<String, Double> r;
        final List<Check> c;
        r = new HashMap<>();
        r.put("sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 1000.0);
        r.put("sqlite/S1/P1/POWER_LOSS/1m:msgPerSec", 700.0);
        c = compare(r, Map.of());
        assertEquals(
                Status.PASS,
                find(c, "depth S1 P1 POWER_LOSS 1m >= 70% of empty").status(),
                "Status should match the expected value.");
    }

    @Test
    void latencyAtCeilingPasses() {
        final Map<String, Double> r;
        r = new HashMap<>();
        r.put("sqlite/S3/P1/POWER_LOSS/empty:p99Ms", 10.0);
        assertEquals(
                Status.PASS,
                find(compare(r, Map.of()), "latency S3 P1 p99 <= 10 ms").status(),
                "Status should match the expected value.");
        r.put("sqlite/S3/P1/POWER_LOSS/empty:p99Ms", 10.1);
    }

    @Test
    void latencyAboveCeilingFails() {
        final Map<String, Double> r;
        r = new HashMap<>();
        r.put("sqlite/S3/P1/POWER_LOSS/empty:p99Ms", 10.0);
        r.put("sqlite/S3/P1/POWER_LOSS/empty:p99Ms", 10.1);
        assertEquals(
                Status.FAIL,
                find(compare(r, Map.of()), "latency S3 P1 p99 <= 10 ms").status(),
                "Status should match the expected value.");
    }

    @Test
    void throughputAtRegressionLimitPasses() {
        final String lat;
        final Map<String, Double> base;
        final Map<String, Double> r;
        final List<Check> c;
        lat = "sqlite/S3/P1/POWER_LOSS/empty:p50Ms";
        base = Map.of(S1P1, 20_000.0, lat, 2.0, "sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 100.0);
        r = new HashMap<>();
        r.put(S1P1, 16_000.0);
        r.put(lat, 2.41);
        r.put("sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 79.0);
        c = compare(r, base);
        assertEquals(Status.PASS, find(c, "regression " + S1P1).status(), "Status should match the expected value.");
    }

    @Test
    void latencyBeyondRegressionLimitFails() {
        final String lat;
        final Map<String, Double> base;
        final Map<String, Double> r;
        final List<Check> c;
        lat = "sqlite/S3/P1/POWER_LOSS/empty:p50Ms";
        base = Map.of(S1P1, 20_000.0, lat, 2.0, "sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 100.0);
        r = new HashMap<>();
        r.put(S1P1, 16_000.0);
        r.put(lat, 2.41);
        r.put("sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 79.0);
        c = compare(r, base);
        assertEquals(Status.FAIL, find(c, "regression " + lat).status(), "Status should match the expected value.");
    }

    @Test
    void throughputBeyondRegressionLimitFails() {
        final String lat;
        final Map<String, Double> base;
        final Map<String, Double> r;
        final List<Check> c;
        lat = "sqlite/S3/P1/POWER_LOSS/empty:p50Ms";
        base = Map.of(S1P1, 20_000.0, lat, 2.0, "sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 100.0);
        r = new HashMap<>();
        r.put(S1P1, 16_000.0);
        r.put(lat, 2.41);
        r.put("sqlite/S2/P1/POWER_LOSS/empty:msgPerSec", 79.0);
        c = compare(r, base);
        assertEquals(
                Status.FAIL,
                find(c, "regression sqlite/S2/P1/POWER_LOSS/empty:msgPerSec").status(),
                "Status should match the expected value.");
    }

    @Test
    void entryWithoutBaselineIsSkipped() {
        final Map<String, Double> r;
        final Check c;
        r = Map.of(S1P1, 20_000.0);
        c = find(compare(r, Map.of()), "regression " + S1P1);
        assertEquals(Status.SKIP, c.status(), "Status should match the expected value.");
    }

    @Test
    void entryWithoutBaselineExplainsSkip() {
        final Map<String, Double> r;
        final Check c;
        r = Map.of(S1P1, 20_000.0);
        c = find(compare(r, Map.of()), "regression " + S1P1);
        assertTrue(c.detail().contains("기준선"), "Detail should contain the expected value.");
    }

    @Test
    void memoryEntriesAreNeverChecked() {
        final Map<String, Double> r;
        r = Map.of("memory/S1/P1/none/empty:msgPerSec", 1.0);
        assertTrue(
                compare(r, r).stream().noneMatch(c -> c.name().contains("memory")),
                "Memory results should be excluded.");
    }

    @Test
    void exitCodeIsZeroWhenNoCheckFailed() {
        assertEquals(
                0,
                Comparator.exitCode(List.of(new Check("a", Status.PASS, ""), new Check("b", Status.SKIP, ""))),
                "Exit code should match the expected value.");
    }

    @Test
    void exitCodeIsNonZeroWhenACheckFailed() {
        assertEquals(
                1,
                Comparator.exitCode(List.of(new Check("a", Status.FAIL, ""))),
                "Exit code should match the expected value.");
    }

    @Test
    void reportIncludesCheckStatuses() {
        final String text;
        text = Comparator.render(List.of(new Check("a", Status.PASS, "x"), new Check("b", Status.FAIL, "y")));
        assertTrue(text.contains("PASS") && text.contains("FAIL"), "Output should include pass and fail statuses.");
    }

    @Test
    void reportRendersOneLinePerCheck() {
        final String text;
        text = Comparator.render(List.of(new Check("a", Status.PASS, "x"), new Check("b", Status.FAIL, "y")));
        assertEquals(
                2,
                text.lines()
                        .filter(l -> l.contains("PASS") || l.contains("FAIL"))
                        .count(),
                "Count should match the expected value.");
    }

    @Test
    void reportRoundTripPreservesEnvironment() {
        final Report r;
        final Report back;
        r = new Report(Map.of("os", "linux"), Map.of(S1P1, 1.5));
        back = Report.fromJson(r.toJson());
        assertEquals(r.environment(), back.environment(), "Environment should match the expected value.");
    }

    @Test
    void reportRoundTripPreservesEntries() {
        final Report r;
        final Report back;
        r = new Report(Map.of("os", "linux"), Map.of(S1P1, 1.5));
        back = Report.fromJson(r.toJson());
        assertEquals(r.entries(), back.entries(), "Entries should match the expected value.");
    }
}
