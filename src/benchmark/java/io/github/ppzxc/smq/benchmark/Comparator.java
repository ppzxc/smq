package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 측정 결과를 스펙의 하한과 기준선에 견주어 항목별 PASS/FAIL/SKIP으로 보고한다. 검사 대상은 SQLite뿐이고 인메모리 항목은 보지 않는다.
 *
 * <ul>
 *   <li>floor: 1프로세스 빈 큐 POWER_LOSS의 S1 절대 하한(P1 10000, P2 8000, P3 4000 msg/s). P0과 P4(4KB)는 기록만 한다.
 *   <li>scaling: 4프로세스 S1은 1프로세스의 70% 이상(P1~P4, 내구성 수준마다).
 *   <li>depth: P1의 100만 건 깊이 S1은 같은 조건 빈 큐의 70% 이상.
 *   <li>latency: S3 p99는 같은 프로세스 10ms 이하, 다른 프로세스 260ms 이하.
 *   <li>regression: 기준선에 있는 항목이 20% 넘게 나빠지면 FAIL(처리량은 떨어짐, 지연은 늘어남).
 * </ul>
 *
 * 측정하지 않았거나 기준선에 없는 항목은 SKIP이다. SKIP은 실패가 아니다.
 */
final class Comparator {

    static final double REGRESSION = 0.20;
    static final double RATIO = 0.70;
    private static final double EPS = 1e-9;

    enum Status {
        PASS,
        FAIL,
        SKIP
    }

    record Check(String name, Status status, String detail) {}

    private Comparator() {}

    /**
     * @param samples 항목 키 → 바퀴별 표본(바퀴 순서). 표본이 하나면 단발 측정이다.
     * @param baseline 항목 키 → 기준선 값(6회 중앙값이라 하나)
     */
    static List<Check> compare(Map<String, List<Double>> samples, Map<String, Double> baseline) {
        Map<String, Double> results = new TreeMap<>();
        samples.forEach((k, v) -> {
            if (!v.isEmpty()) results.put(k, Aggregator.median(v));
        });
        List<Check> checks = new ArrayList<>();
        floors(results, checks);
        scaling(samples, checks);
        depth(samples, checks);
        latency(results, checks);
        regressions(results, baseline, checks);
        return checks;
    }

    private static String key(Profile p, String shape, Durability d, int depth) {
        return Keys.of(Storage.SQLITE, Scenario.S1, p, shape, d.name(), depth);
    }

    private static void floors(Map<String, Double> r, List<Check> out) {
        Map<Profile, Integer> floor = Map.of(Profile.P1, 10_000, Profile.P2, 8_000, Profile.P3, 4_000);
        for (Profile p : List.of(Profile.P1, Profile.P2, Profile.P3)) {
            String name = "floor S1 " + p + " proc1 POWER_LOSS >= " + floor.get(p) + " msg/s";
            Double v = r.get(Keys.throughput(key(p, "proc1", Durability.POWER_LOSS, 0)));
            out.add(v == null ? notMeasured(name)
                    : v >= floor.get(p) ? pass(name, fmt(v) + " msg/s") : fail(name, fmt(v) + " msg/s"));
        }
    }

    private static void scaling(Map<String, List<Double>> r, List<Check> out) {
        for (Profile p : List.of(Profile.P1, Profile.P2, Profile.P3, Profile.P4)) {
            for (Durability d : Durability.values()) {
                String name = "scaling S1 " + p + " " + d + " proc4 >= 70% of proc1";
                ratio(out, name, r.get(Keys.throughput(key(p, "proc4", d, 0))),
                        r.get(Keys.throughput(key(p, "proc1", d, 0))));
            }
        }
    }

    private static void depth(Map<String, List<Double>> r, List<Check> out) {
        for (String shape : List.of("proc1", "proc4")) {
            for (Durability d : Durability.values()) {
                String name = "depth S1 P1 " + shape + " " + d + " 1m >= 70% of empty";
                ratio(out, name, r.get(Keys.throughput(key(Profile.P1, shape, d, RunSpec.DEEP))),
                        r.get(Keys.throughput(key(Profile.P1, shape, d, 0))));
            }
        }
    }

    /**
     * 바퀴별 비율(같은 바퀴의 분자 / 분모)의 중앙값으로 판정한다. 같은 바퀴의 두 값은 시간상 가까워 장비 부하 변화가 상쇄된다.
     * 표본이 하나면 단발 비율이고 detail도 단발 출력과 같다. 한쪽이라도 표본이 없으면 SKIP이다.
     */
    private static void ratio(List<Check> out, String name, List<Double> values, List<Double> references) {
        int rounds = values == null || references == null ? 0 : Math.min(values.size(), references.size());
        if (rounds == 0) {
            out.add(notMeasured(name));
            return;
        }
        List<Double> ratios = new ArrayList<>();
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            double ratio = values.get(i) / references.get(i);
            ratios.add(ratio);
            parts.add(fmt(values.get(i)) + " / " + fmt(references.get(i)) + " = " + String.format("%.1f%%", 100 * ratio));
        }
        String detail;
        if (rounds == 1) {
            detail = parts.get(0);
        } else {
            StringBuilder sb = new StringBuilder("중앙값 " + String.format("%.1f%%", 100 * Aggregator.median(ratios)) + ", 바퀴별 ");
            for (int i = 0; i < rounds; i++) sb.append(i > 0 ? "; " : "").append(i + 1).append("회 ").append(parts.get(i));
            detail = sb.toString();
        }
        out.add(Aggregator.median(ratios) >= RATIO - EPS ? pass(name, detail) : fail(name, detail));
    }

    private static void latency(Map<String, Double> r, List<Check> out) {
        for (Object[] c : new Object[][] {{Topology.SAME_PROCESS, 10}, {Topology.CROSS_PROCESS, 260}}) {
            Topology t = (Topology) c[0];
            int ceiling = (Integer) c[1];
            String name = "latency S3 P1 " + t.id() + " p99 <= " + ceiling + " ms";
            Double v = r.get(Keys.p99(Keys.of(Storage.SQLITE, Scenario.S3, Profile.P1, t.id(),
                    Durability.POWER_LOSS.name(), 0)));
            out.add(v == null ? notMeasured(name)
                    : v <= ceiling + EPS ? pass(name, fmt(v) + " ms") : fail(name, fmt(v) + " ms"));
        }
    }

    private static void regressions(Map<String, Double> r, Map<String, Double> baseline, List<Check> out) {
        for (Map.Entry<String, Double> e : new TreeMap<>(r).entrySet()) {
            String id = e.getKey();
            if (!id.startsWith(Storage.SQLITE.id() + "/")) continue;
            String name = "regression " + id;
            Double base = baseline.get(id);
            if (base == null) {
                out.add(new Check(name, Status.SKIP, "기준선에 없다"));
                continue;
            }
            double v = e.getValue();
            boolean lowerIsBetter = id.endsWith(":p50Ms") || id.endsWith(":p99Ms");
            boolean bad = lowerIsBetter ? v > base * (1 + REGRESSION) + EPS : v < base * (1 - REGRESSION) - EPS;
            String detail = fmt(v) + " vs 기준선 " + fmt(base) + String.format(" (%+.1f%%)", 100 * (v - base) / base);
            out.add(bad ? fail(name, detail) : pass(name, detail));
        }
    }

    private static Check pass(String name, String detail) {
        return new Check(name, Status.PASS, detail);
    }

    private static Check fail(String name, String detail) {
        return new Check(name, Status.FAIL, detail);
    }

    private static Check notMeasured(String name) {
        return new Check(name, Status.SKIP, "측정하지 않았다");
    }

    private static String fmt(double v) {
        return v >= 100 ? String.format("%.0f", v) : String.format("%.2f", v);
    }

    /** 하나라도 FAIL이면 1, 아니면 0. */
    static int exitCode(List<Check> checks) {
        return checks.stream().anyMatch(c -> c.status() == Status.FAIL) ? 1 : 0;
    }

    static String render(List<Check> checks) {
        StringBuilder sb = new StringBuilder();
        for (Check c : checks) {
            sb.append(String.format("%-4s  %s  [%s]%n", c.status(), c.name(), c.detail()));
        }
        long pass = checks.stream().filter(c -> c.status() == Status.PASS).count();
        long fail = checks.stream().filter(c -> c.status() == Status.FAIL).count();
        long skip = checks.size() - pass - fail;
        sb.append(String.format("합계: 통과 %d, 실패 %d, 건너뜀 %d%n", pass, fail, skip));
        return sb.toString();
    }
}
