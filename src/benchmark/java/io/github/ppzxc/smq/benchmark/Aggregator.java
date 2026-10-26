package io.github.ppzxc.smq.benchmark;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 자식 결과들을 측정 조건 하나의 항목으로 합산한다. */
final class Aggregator {

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
    static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) throw new IllegalArgumentException("표본이 없다");
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(1, Math.min(rank, sorted.length)) - 1];
    }
}
