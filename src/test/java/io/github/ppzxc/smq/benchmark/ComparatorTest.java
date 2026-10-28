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

    private static final String S1P1 = "sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec";

    private static Check find(List<Check> checks, String name) {
        return checks.stream().filter(c -> c.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("검사 없음: " + name + " in " + checks));
    }


    /** 표본이 하나인 입력. 기존 단일 값 테스트가 쓴다. */
    private static List<Check> compare(Map<String, Double> r, Map<String, Double> baseline) {
        Map<String, List<Double>> samples = new LinkedHashMap<>();
        r.forEach((k, v) -> samples.put(k, List.of(v)));
        return Comparator.compare(samples, baseline);
    }

    private static final String P3_1 = "sqlite/S1/P3/proc1/POWER_LOSS/empty:msgPerSec";
    private static final String P3_4 = "sqlite/S1/P3/proc4/POWER_LOSS/empty:msgPerSec";
    private static final String SCALING_P3 = "scaling S1 P3 POWER_LOSS proc4 >= 70% of proc1";

    private static Check scaling(List<Double> proc4, List<Double> proc1) {
        return find(Comparator.compare(Map.of(P3_4, proc4, P3_1, proc1), Map.of()), SCALING_P3);
    }

    @Test
    void ratioIsJudgedByTheMedianOfPerRoundRatios() {
        // 바퀴별 비율 60%, 74%, 80%: 중앙값 74%. 한 바퀴가 하한 아래여도 PASS.
        Check c = scaling(List.of(6000.0, 7400.0, 8000.0), List.of(10000.0, 10000.0, 10000.0));
        assertEquals(Status.PASS, c.status());
        assertTrue(c.detail().contains("74.0%"), c.detail());
    }

    @Test
    void ratioFailsWhenTheMedianIsBelowTheFloorEvenIfOneRoundPasses() {
        Check c = scaling(List.of(6000.0, 6500.0, 9000.0), List.of(10000.0, 10000.0, 10000.0));
        assertEquals(Status.FAIL, c.status());
        assertTrue(c.detail().contains("65.0%"), c.detail());
    }

    @Test
    void ratioMedianOfEvenNumberOfRoundsAveragesTheMiddleTwo() {
        // 비율 60%, 80% -> 중앙값 70% (정확히 하한)
        Check c = scaling(List.of(6000.0, 8000.0), List.of(10000.0, 10000.0));
        assertEquals(Status.PASS, c.status());
        assertTrue(c.detail().contains("70.0%"), c.detail());
    }

    @Test
    void exactlySeventyPercentPasses() {
        assertEquals(Status.PASS, scaling(List.of(7000.0, 7000.0, 7000.0), List.of(10000.0, 10000.0, 10000.0)).status());
    }

    @Test
    void ratioPairsNumeratorAndDenominatorOfTheSameRound() {
        // 바퀴별 비율: 100/100=100%, 50/100=50%, 90/100=90% -> 중앙값 90%.
        // 값끼리 따로 중앙값을 내면 분자 90 / 분모 100도 90%지만, 분모가 흔들리는 경우를 구분한다:
        // 분자 [10, 50, 90], 분모 [100, 50, 100]: 비율 10%, 100%, 90% -> 중앙값 90%. 따로 중앙값이면 50/100=50% FAIL.
        Check c = scaling(List.of(10.0, 50.0, 90.0), List.of(100.0, 50.0, 100.0));
        assertEquals(Status.PASS, c.status());
        assertTrue(c.detail().contains("90.0%"), c.detail());
    }

    @Test
    void ratioDetailListsEveryRound() {
        Check c = scaling(List.of(6000.0, 7400.0, 8000.0), List.of(10000.0, 10000.0, 10000.0));
        assertTrue(c.detail().contains("6000 / 10000 = 60.0%"), c.detail());
        assertTrue(c.detail().contains("7400 / 10000 = 74.0%"), c.detail());
        assertTrue(c.detail().contains("8000 / 10000 = 80.0%"), c.detail());
    }

    @Test
    void singleSampleKeepsTheOldDetail() {
        Check c = scaling(List.of(7000.0), List.of(10000.0));
        assertEquals("7000 / 10000 = 70.0%", c.detail());
    }

    @Test
    void mismatchedSampleCountsAreFailedExplicitlyNotTrimmedToTheShorter() {
        Check c = scaling(List.of(7000.0, 7000.0, 7000.0), List.of(10000.0, 10000.0));
        assertEquals(Status.FAIL, c.status());
        assertTrue(c.detail().contains("표본 수"), c.detail());
    }

    @Test
    void aSingleSampleSideStillPairsWithTheFirstRound() {
        assertEquals(Status.PASS, scaling(List.of(7000.0, 1.0), List.of(10000.0)).status());
        assertEquals(Status.PASS, scaling(List.of(7000.0), List.of(10000.0, 1.0)).status());
    }

    @Test
    void ratioIsSkippedWhenOneSideHasNoSamples() {
        assertEquals(Status.SKIP, scaling(List.of(), List.of(10000.0)).status());
        assertEquals(Status.SKIP, find(Comparator.compare(Map.of(P3_4, List.of(7000.0)), Map.of()), SCALING_P3).status());
    }

    @Test
    void floorAndRegressionUseTheMedianOfSamples() {
        String p1 = S1P1;
        Map<String, List<Double>> r = Map.of(p1, List.of(9000.0, 16000.0, 10500.0, 20000.0)); // 중앙값 13250
        List<Check> c = Comparator.compare(r, Map.of(p1, 16000.0));
        assertEquals(Status.PASS, find(c, "floor S1 P1 proc1 POWER_LOSS >= 10000 msg/s").status());
        assertTrue(find(c, "floor S1 P1 proc1 POWER_LOSS >= 10000 msg/s").detail().contains("13250"));
        assertTrue(find(c, "regression " + p1).detail().contains("13250"));
        assertEquals(Status.PASS, find(c, "regression " + p1).status()); // -17.2%
    }

    @Test
    void absoluteFloorsPassAndFail() {
        Map<String, Double> r = new HashMap<>();
        r.put(S1P1, 10_000.0);
        r.put("sqlite/S1/P2/proc1/POWER_LOSS/empty:msgPerSec", 7_999.0);
        r.put("sqlite/S1/P3/proc1/POWER_LOSS/empty:msgPerSec", 4_000.0);
        List<Check> c = compare(r, Map.of());
        assertEquals(Status.PASS, find(c, "floor S1 P1 proc1 POWER_LOSS >= 10000 msg/s").status());
        assertEquals(Status.FAIL, find(c, "floor S1 P2 proc1 POWER_LOSS >= 8000 msg/s").status());
        assertEquals(Status.PASS, find(c, "floor S1 P3 proc1 POWER_LOSS >= 4000 msg/s").status());
    }

    @Test
    void notMeasuredIsSkippedNotFailed() {
        List<Check> c = compare(Map.of(), Map.of());
        assertTrue(c.stream().allMatch(x -> x.status() == Status.SKIP));
        assertFalse(c.isEmpty());
    }

    @Test
    void fourProcessesMustKeepSeventyPercentOfOne() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S1/P1/proc1/PROCESS_CRASH/empty:msgPerSec", 10_000.0);
        r.put("sqlite/S1/P1/proc4/PROCESS_CRASH/empty:msgPerSec", 7_000.0);
        r.put("sqlite/S1/P2/proc1/PROCESS_CRASH/empty:msgPerSec", 10_000.0);
        r.put("sqlite/S1/P2/proc4/PROCESS_CRASH/empty:msgPerSec", 6_999.0);
        List<Check> c = compare(r, Map.of());
        assertEquals(Status.PASS, find(c, "scaling S1 P1 PROCESS_CRASH proc4 >= 70% of proc1").status());
        assertEquals(Status.FAIL, find(c, "scaling S1 P2 PROCESS_CRASH proc4 >= 70% of proc1").status());
    }

    @Test
    void fourKbProfileHasScalingButNoFloor() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S1/P4/proc1/POWER_LOSS/empty:msgPerSec", 10.0);
        r.put("sqlite/S1/P4/proc4/POWER_LOSS/empty:msgPerSec", 6.0);
        List<Check> c = compare(r, Map.of());
        assertEquals(Status.FAIL, find(c, "scaling S1 P4 POWER_LOSS proc4 >= 70% of proc1").status());
        assertTrue(c.stream().noneMatch(x -> x.name().startsWith("floor S1 P4")), "P4에는 절대 하한이 없다");
    }

    @Test
    void depthMustKeepSeventyPercentOfEmpty() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S1/P1/proc4/POWER_LOSS/empty:msgPerSec", 1000.0);
        r.put("sqlite/S1/P1/proc4/POWER_LOSS/1m:msgPerSec", 699.0);
        r.put("sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec", 1000.0);
        r.put("sqlite/S1/P1/proc1/POWER_LOSS/1m:msgPerSec", 700.0);
        List<Check> c = compare(r, Map.of());
        assertEquals(Status.FAIL, find(c, "depth S1 P1 proc4 POWER_LOSS 1m >= 70% of empty").status());
        assertEquals(Status.PASS, find(c, "depth S1 P1 proc1 POWER_LOSS 1m >= 70% of empty").status());
    }

    @Test
    void latencyCeilings() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S3/P1/same-process/POWER_LOSS/empty:p99Ms", 10.0);
        r.put("sqlite/S3/P1/cross-process/POWER_LOSS/empty:p99Ms", 260.1);
        List<Check> c = compare(r, Map.of());
        assertEquals(Status.PASS, find(c, "latency S3 P1 same-process p99 <= 10 ms").status());
        assertEquals(Status.FAIL, find(c, "latency S3 P1 cross-process p99 <= 260 ms").status());
    }

    @Test
    void regressionOverTwentyPercentFailsForThroughputAndLatency() {
        String lat = "sqlite/S3/P1/same-process/POWER_LOSS/empty:p50Ms";
        Map<String, Double> base = Map.of(S1P1, 20_000.0, lat, 2.0,
                "sqlite/S2/P1/proc1/POWER_LOSS/empty:msgPerSec", 100.0);
        Map<String, Double> r = new HashMap<>();
        r.put(S1P1, 16_000.0); // 정확히 -20%: 통과
        r.put(lat, 2.41); // +20% 초과: 실패
        r.put("sqlite/S2/P1/proc1/POWER_LOSS/empty:msgPerSec", 79.0); // -21%: 실패
        List<Check> c = compare(r, base);
        assertEquals(Status.PASS, find(c, "regression " + S1P1).status());
        assertEquals(Status.FAIL, find(c, "regression " + lat).status());
        assertEquals(Status.FAIL, find(c, "regression sqlite/S2/P1/proc1/POWER_LOSS/empty:msgPerSec").status());
    }

    @Test
    void entryWithoutBaselineIsSkipped() {
        Map<String, Double> r = Map.of(S1P1, 20_000.0);
        Check c = find(compare(r, Map.of()), "regression " + S1P1);
        assertEquals(Status.SKIP, c.status());
        assertTrue(c.detail().contains("기준선"));
    }

    @Test
    void memoryEntriesAreNeverChecked() {
        Map<String, Double> r = Map.of("memory/S1/P1/proc1/none/empty:msgPerSec", 1.0);
        assertTrue(compare(r, r).stream().noneMatch(c -> c.name().contains("memory")));
    }

    @Test
    void exitCodeIsNonZeroOnlyWhenSomethingFailed() {
        assertEquals(0, Comparator.exitCode(List.of(new Check("a", Status.PASS, ""), new Check("b", Status.SKIP, ""))));
        assertEquals(1, Comparator.exitCode(List.of(new Check("a", Status.FAIL, ""))));
    }

    @Test
    void reportRendersOneLinePerCheck() {
        String text = Comparator.render(List.of(new Check("a", Status.PASS, "x"), new Check("b", Status.FAIL, "y")));
        assertTrue(text.contains("PASS") && text.contains("FAIL"));
        assertEquals(2, text.lines().filter(l -> l.contains("PASS") || l.contains("FAIL")).count());
    }

    @Test
    void reportFileRoundTrips() {
        Report r = new Report(Map.of("os", "linux"), Map.of(S1P1, 1.5));
        Report back = Report.fromJson(r.toJson());
        assertEquals(r.environment(), back.environment());
        assertEquals(r.entries(), back.entries());
    }
}
