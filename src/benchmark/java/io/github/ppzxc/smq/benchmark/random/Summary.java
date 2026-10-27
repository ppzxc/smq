package io.github.ppzxc.smq.benchmark.random;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/** 세션 끝의 요약 표. 처리량이 낮은 순과 p99가 높은 순으로 정렬해 눈에 띄는 시드를 위로 올린다. 판정에는 쓰지 않는다. */
final class Summary {

    private Summary() {}

    static String render(List<RunResult> results, int rows) {
        if (results.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder("\n");
        sb.append(table("처리량이 낮은 순", results.stream()
                .sorted(Comparator.comparingDouble(RunResult::msgPerSec)).limit(rows).toList()));
        sb.append(table("p99가 높은 순", results.stream()
                .sorted(Comparator.comparing((RunResult r) -> r.p99Ms() == null ? -1.0 : r.p99Ms()).reversed())
                .limit(rows).toList()));
        return sb.toString();
    }

    private static String table(String title, List<RunResult> rows) {
        StringBuilder sb = new StringBuilder(title).append('\n');
        sb.append(String.format("  %-20s %10s %8s %9s %9s  %s%n", "seed", "msg/s", "MB/s", "p50", "p99", "판정"));
        for (RunResult r : rows) {
            sb.append(String.format("  %-20d %10.0f %8.2f %9s %9s  %s%n", r.params().seed(), r.msgPerSec(),
                    r.mbPerSec(), ms(r, RunResult::p50Ms), ms(r, RunResult::p99Ms), r.verdict()));
        }
        return sb.append('\n').toString();
    }

    private static String ms(RunResult r, Function<RunResult, Double> f) {
        Double v = f.apply(r);
        return v == null ? "-" : String.format("%.0fms", v);
    }
}
