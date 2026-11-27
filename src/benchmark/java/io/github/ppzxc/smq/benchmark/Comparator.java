package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 측정 결과를 스펙의 하한과 기준선에 견주어 항목별 PASS/FAIL/SKIP으로 보고한다. 검사 대상은 SQLite뿐이고 인메모리 항목은 보지 않는다.
 *
 * <ul>
 *   <li>floor: 빈 큐 POWER_LOSS의 S1 절대 하한(P1 10000, P3 8000, P7 4000 msg/s). P0과 P5(4KB)는 기록만 한다.
 *   <li>depth: P1의 100만 건 깊이 S1은 같은 조건 빈 큐의 70% 이상.
 *   <li>latency: S3 p99는 10ms 이하.
 *   <li>regression: 기준선에 있는 항목이 20% 넘게 나빠지면 FAIL(처리량은 떨어짐, 지연은 늘어남).
 * </ul>
 *
 * 측정하지 않았거나 기준선에 없는 항목은 SKIP이다. SKIP은 실패가 아니다.
 */
final class Comparator {

    static final double REGRESSION = 0.20;
    static final double RATIO = 0.70;
    private static final Set<RunSpec.QueueKind> ENABLED_QUEUE_KIND_CHECKS = Set.of(
            RunSpec.QueueKind.MAX_DELIVERIES, RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER, RunSpec.QueueKind.LEASED_HEAD);
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
    static List<Check> compare(final Map<String, List<Double>> samples, final Map<String, Double> baseline) {
        final Map<String, Double> results;
        final List<Check> checks;
        results = new TreeMap<>();
        samples.forEach((k, v) -> {
            if (!v.isEmpty()) results.put(k, Aggregator.median(v));
        });
        checks = new ArrayList<>();
        floors(results, checks);
        depth(samples, checks);
        latency(results, checks);
        regressions(results, baseline, checks);
        return checks;
    }

    private static String key(final Profile p, final Durability d, final int depth) {
        return Keys.of(Storage.SQLITE, Scenario.S1, p, d.name(), depth);
    }

    private static void floors(final Map<String, Double> r, final List<Check> out) {
        final Map<Profile, Integer> floor;
        floor = Map.of(Profile.P1, 10_000, Profile.P3, 8_000, Profile.P7, 4_000);
        for (final Profile p : List.of(Profile.P1, Profile.P3, Profile.P7)) {
            final String name;
            final Double v;
            name = "floor S1 " + p + " POWER_LOSS >= " + floor.get(p) + " msg/s";
            v = r.get(Keys.throughput(key(p, Durability.POWER_LOSS, 0)));
            out.add(
                    v == null
                            ? notMeasured(name)
                            : v >= floor.get(p) ? pass(name, fmt(v) + " msg/s") : fail(name, fmt(v) + " msg/s"));
        }
    }

    private static void depth(final Map<String, List<Double>> r, final List<Check> out) {
        for (final Durability d : Durability.values()) {
            final String name;
            name = "depth S1 P1 " + d + " 1m >= 70% of empty";
            ratio(
                    out,
                    name,
                    r.get(Keys.throughput(key(Profile.P1, d, RunSpec.DEEP))),
                    r.get(Keys.throughput(key(Profile.P1, d, 0))),
                    true);
        }
        for (final RunSpec.QueueKind kind : List.of(
                RunSpec.QueueKind.MAX_DELIVERIES,
                RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER,
                RunSpec.QueueKind.LEASED_HEAD)) {
            final String name;
            final String deep;
            final String empty;
            name = "depth S1 P1 PROCESS_CRASH " + kind + " 1m >= 70% of empty";
            deep = RunSpec.s1(kind, RunSpec.DEEP, 0, 0).key();
            empty = RunSpec.s1(kind, 0, 0, 0).key();
            ratio(
                    out,
                    name,
                    r.get(Keys.throughput(deep)),
                    r.get(Keys.throughput(empty)),
                    ENABLED_QUEUE_KIND_CHECKS.contains(kind));
        }
    }

    /**
     * 바퀴별 비율(같은 바퀴의 분자 / 분모)의 중앙값으로 판정한다. 같은 바퀴의 두 값은 시간상 가까워 장비 부하 변화가 상쇄된다.
     * 표본이 하나면 단발 비율이고 detail도 단발 출력과 같다. 한쪽이라도 표본이 없으면 SKIP이다. 양쪽이 둘 이상인데 개수가 다르면
     * 바퀴가 어긋난 것이므로 FAIL이다.
     */
    private static void ratio(
            final List<Check> out,
            final String name,
            final List<Double> values,
            final List<Double> references,
            final boolean enabled) {
        final boolean singleSide;
        final int rounds;
        final List<Double> ratios;
        final List<String> parts;
        final String detail;
        if (missing(values) || missing(references)) {
            out.add(notMeasured(name));
            return;
        }
        // 한쪽이 단일 표본(단발 측정)이면 첫 바퀴끼리 짝짓는 것이 합법이다. 그 밖의 길이 불일치는 바퀴가 어긋난 것이라 숨기지 않는다.
        singleSide = values.size() == 1 || references.size() == 1;
        if (!singleSide && values.size() != references.size()) {
            final String mismatchDetail;
            mismatchDetail = "표본 수가 다르다: " + values.size() + "개 / " + references.size() + "개";
            out.add(enabled ? fail(name, mismatchDetail) : new Check(name, Status.SKIP, mismatchDetail));
            return;
        }
        rounds = singleSide ? 1 : values.size();
        ratios = new ArrayList<>();
        parts = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            final double ratio;
            ratio = values.get(i) / references.get(i);
            ratios.add(ratio);
            parts.add(fmt(values.get(i)) + " / " + fmt(references.get(i)) + " = "
                    + String.format(Locale.getDefault(Locale.Category.FORMAT), "%.1f%%", 100 * ratio));
        }
        detail = ratioDetail(rounds, parts, ratios);
        out.add(ratioResult(name, detail, ratios, enabled));
    }

    private static Check ratioResult(
            final String name, final String detail, final List<Double> ratios, final boolean enabled) {
        return !enabled
                ? new Check(name, Status.SKIP, detail)
                : Aggregator.median(ratios) >= RATIO - EPS ? pass(name, detail) : fail(name, detail);
    }

    private static boolean missing(final List<Double> values) {
        return values == null || values.isEmpty();
    }

    private static String ratioDetail(final int rounds, final List<String> parts, final List<Double> ratios) {
        final StringBuilder sb;
        if (rounds == 1) return parts.get(0);
        sb = new StringBuilder("중앙값 "
                + String.format(Locale.getDefault(Locale.Category.FORMAT), "%.1f%%", 100 * Aggregator.median(ratios))
                + ", 바퀴별 ");
        for (int i = 0; i < rounds; i++)
            sb.append(i > 0 ? "; " : "").append(i + 1).append("회 ").append(parts.get(i));
        return sb.toString();
    }

    private static void latency(final Map<String, Double> r, final List<Check> out) {
        final int ceiling;
        final String name;
        final Double v;
        ceiling = 10;
        name = "latency S3 P1 p99 <= " + ceiling + " ms";
        v = r.get(Keys.p99(Keys.of(Storage.SQLITE, Scenario.S3, Profile.P1, Durability.POWER_LOSS.name(), 0)));
        out.add(
                v == null
                        ? notMeasured(name)
                        : v <= ceiling + EPS ? pass(name, fmt(v) + " ms") : fail(name, fmt(v) + " ms"));
    }

    private static void regressions(
            final Map<String, Double> r, final Map<String, Double> baseline, final List<Check> out) {
        for (final Map.Entry<String, Double> e : new TreeMap<>(r).entrySet()) {
            final String id;
            final String name;
            final Double base;
            final double v;
            final boolean lowerIsBetter;
            final boolean bad;
            final String detail;
            id = e.getKey();
            if (!id.startsWith(Storage.SQLITE.id() + "/")) continue;
            name = "regression " + id;
            base = baseline.get(id);
            if (base == null) {
                out.add(new Check(name, Status.SKIP, "기준선에 없다"));
                continue;
            }
            v = e.getValue();
            lowerIsBetter = id.endsWith(":p50Ms") || id.endsWith(":p99Ms");
            bad = lowerIsBetter ? v > base * (1 + REGRESSION) + EPS : v < base * (1 - REGRESSION) - EPS;
            detail = fmt(v) + " vs 기준선 " + fmt(base)
                    + String.format(Locale.getDefault(Locale.Category.FORMAT), " (%+.1f%%)", 100 * (v - base) / base);
            out.add(bad ? fail(name, detail) : pass(name, detail));
        }
    }

    private static Check pass(final String name, final String detail) {
        return new Check(name, Status.PASS, detail);
    }

    private static Check fail(final String name, final String detail) {
        return new Check(name, Status.FAIL, detail);
    }

    private static Check notMeasured(final String name) {
        return new Check(name, Status.SKIP, "측정하지 않았다");
    }

    private static String fmt(final double v) {
        return v >= 100
                ? String.format(Locale.getDefault(Locale.Category.FORMAT), "%.0f", v)
                : String.format(Locale.getDefault(Locale.Category.FORMAT), "%.2f", v);
    }

    /** 하나라도 FAIL이면 1, 아니면 0. */
    static int exitCode(final List<Check> checks) {
        return checks.stream().anyMatch(c -> c.status() == Status.FAIL) ? 1 : 0;
    }

    static String render(final List<Check> checks) {
        final StringBuilder sb;
        final long pass;
        final long fail;
        final long skip;
        sb = new StringBuilder();
        for (final Check c : checks) {
            sb.append(String.format(
                    Locale.getDefault(Locale.Category.FORMAT), "%-4s  %s  [%s]%n", c.status(), c.name(), c.detail()));
        }
        pass = checks.stream().filter(c -> c.status() == Status.PASS).count();
        fail = checks.stream().filter(c -> c.status() == Status.FAIL).count();
        skip = checks.size() - pass - fail;
        sb.append(String.format(
                Locale.getDefault(Locale.Category.FORMAT), "합계: 통과 %d, 실패 %d, 건너뜀 %d%n", pass, fail, skip));
        return sb.toString();
    }
}
