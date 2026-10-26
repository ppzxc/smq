package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.Comparator.Check;
import io.github.ppzxc.smq.benchmark.Comparator.Status;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComparatorTest {

    private static final String S1P1 = "sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec";

    private static Check find(List<Check> checks, String name) {
        return checks.stream().filter(c -> c.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("검사 없음: " + name + " in " + checks));
    }

    @Test
    void absoluteFloorsPassAndFail() {
        Map<String, Double> r = new HashMap<>();
        r.put(S1P1, 10_000.0);
        r.put("sqlite/S1/P2/proc1/POWER_LOSS/empty:msgPerSec", 7_999.0);
        r.put("sqlite/S1/P3/proc1/POWER_LOSS/empty:msgPerSec", 4_000.0);
        List<Check> c = Comparator.compare(r, Map.of());
        assertEquals(Status.PASS, find(c, "floor S1 P1 proc1 POWER_LOSS >= 10000 msg/s").status());
        assertEquals(Status.FAIL, find(c, "floor S1 P2 proc1 POWER_LOSS >= 8000 msg/s").status());
        assertEquals(Status.PASS, find(c, "floor S1 P3 proc1 POWER_LOSS >= 4000 msg/s").status());
    }

    @Test
    void notMeasuredIsSkippedNotFailed() {
        List<Check> c = Comparator.compare(Map.of(), Map.of());
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
        List<Check> c = Comparator.compare(r, Map.of());
        assertEquals(Status.PASS, find(c, "scaling S1 P1 PROCESS_CRASH proc4 >= 70% of proc1").status());
        assertEquals(Status.FAIL, find(c, "scaling S1 P2 PROCESS_CRASH proc4 >= 70% of proc1").status());
    }

    @Test
    void fourKbProfileHasScalingButNoFloor() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S1/P4/proc1/POWER_LOSS/empty:msgPerSec", 10.0);
        r.put("sqlite/S1/P4/proc4/POWER_LOSS/empty:msgPerSec", 6.0);
        List<Check> c = Comparator.compare(r, Map.of());
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
        List<Check> c = Comparator.compare(r, Map.of());
        assertEquals(Status.FAIL, find(c, "depth S1 P1 proc4 POWER_LOSS 1m >= 70% of empty").status());
        assertEquals(Status.PASS, find(c, "depth S1 P1 proc1 POWER_LOSS 1m >= 70% of empty").status());
    }

    @Test
    void latencyCeilings() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S3/P1/same-process/POWER_LOSS/empty:p99Ms", 10.0);
        r.put("sqlite/S3/P1/cross-process/POWER_LOSS/empty:p99Ms", 260.1);
        List<Check> c = Comparator.compare(r, Map.of());
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
        List<Check> c = Comparator.compare(r, base);
        assertEquals(Status.PASS, find(c, "regression " + S1P1).status());
        assertEquals(Status.FAIL, find(c, "regression " + lat).status());
        assertEquals(Status.FAIL, find(c, "regression sqlite/S2/P1/proc1/POWER_LOSS/empty:msgPerSec").status());
    }

    @Test
    void entryWithoutBaselineIsSkipped() {
        Map<String, Double> r = Map.of(S1P1, 20_000.0);
        Check c = find(Comparator.compare(r, Map.of()), "regression " + S1P1);
        assertEquals(Status.SKIP, c.status());
        assertTrue(c.detail().contains("기준선"));
    }

    @Test
    void memoryEntriesAreNeverChecked() {
        Map<String, Double> r = Map.of("memory/S1/P1/proc1/none/empty:msgPerSec", 1.0);
        assertTrue(Comparator.compare(r, r).stream().noneMatch(c -> c.name().contains("memory")));
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
