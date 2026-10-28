package io.github.ppzxc.smq.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 자식 결과들을 측정 조건 하나의 항목으로 합산한다. */
public final class Aggregator {

    private Aggregator() {}

    /** 항목 id(키와 지표 이름)에서 값으로. 처리량은 msg/s, 지연은 ms. */
    static Map<String, Double> aggregate(RunSpec spec, List<ChildResult> children) {
        Map<String, Double> out = new LinkedHashMap<>();
        if (spec.scenario() == Scenario.S3) {
            long[] all = children.stream().flatMapToLong(c -> Arrays.stream(c.latenciesMicros())).sorted().toArray();
            if (all.length == 0) throw new IllegalStateException("지연 표본이 없다: " + spec.key());
            out.put(Keys.p50(spec.key()), percentile(all, 50) / 1000.0);
            out.put(Keys.p99(spec.key()), percentile(all, 99) / 1000.0);
            return out;
        }
        long messages = children.stream().mapToLong(ChildResult::messages).sum();
        long slowest = children.stream().mapToLong(ChildResult::elapsedNanos).max().orElse(0);
        if (slowest <= 0) throw new IllegalStateException("측정 구간이 비었다: " + spec.key());
        out.put(Keys.throughput(spec.key()), messages / (slowest / 1e9));
        return out;
    }

    /** 오름차순 표본의 nearest-rank 백분위수. */
    public static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) throw new IllegalArgumentException("표본이 없다");
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(1, Math.min(rank, sorted.length)) - 1];
    }

    /** 바퀴별 결과를 항목 키 → 바퀴 순서의 표본 목록으로 모은다. */
    static Map<String, List<Double>> samples(List<Map<String, Double>> rounds) {
        Map<String, List<Double>> out = new LinkedHashMap<>();
        for (Map<String, Double> round : rounds) {
            round.forEach((k, v) -> out.computeIfAbsent(k, x -> new ArrayList<>()).add(v));
        }
        return out;
    }

    /** 표본의 중앙값. 짝수 개면 가운데 두 값의 평균. */
    static double median(List<Double> samples) {
        if (samples.isEmpty()) throw new IllegalArgumentException("표본이 없다");
        double[] a = samples.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        int n = a.length;
        return n % 2 == 1 ? a[n / 2] : (a[n / 2 - 1] + a[n / 2]) / 2;
    }

    /** 대표값(중앙값)을 항목 값으로 두고, 반복 측정한 항목(표본 둘 이상)만 표본을 덧붙인 보고서. */
    static Report report(Map<String, String> environment, Map<String, List<Double>> samples) {
        Map<String, Double> entries = new LinkedHashMap<>();
        Map<String, List<Double>> repeated = new LinkedHashMap<>();
        samples.forEach((k, v) -> {
            entries.put(k, median(v));
            if (v.size() > 1) repeated.put(k, List.copyOf(v));
        });
        return new Report(environment, entries, repeated);
    }
}
