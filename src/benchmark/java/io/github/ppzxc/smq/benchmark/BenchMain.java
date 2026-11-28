package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.PolicyException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 부모 JVM 진입점. 옵션:
 *
 * <ul>
 *   <li>{@code --mode smoke|full}: 짧은 스모크(임계값 없음) 또는 스펙 표 전체. 기본은 smoke.
 *   <li>{@code --storage sqlite|memory}: 기본은 sqlite. memory는 코어 오버헤드를 보는 진단값이고 임계값을 적용하지 않는다.
 *   <li>{@code --baseline <파일>}: 기준선과 비교한다(full, sqlite). 파일이 없으면 하한만 검사한다.
 *   <li>{@code --output <파일>}: 결과를 기준선과 같은 형식으로 쓴다.
 *   <li>{@code --work-dir <디렉터리>}: SQLite 파일을 만들 곳. 기준 환경에서는 로컬 NVMe ext4 위여야 한다.
 *   <li>{@code --repeat <횟수>}: full 모드에서 비율 검사의 분자와 분모가 되는 S1 조건을 재는 횟수. 기본 3, 1이면 단발이다.
 *   <li>{@code --filter <문자열>}: 키에 이 문자열이 든 조건만 돈다. 예: {@code S1/P1}.
 * </ul>
 */
public final class BenchMain {

    private BenchMain() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.CyclomaticComplexity",
            reason =
                    "The entry point selects the plan, reports each run, and applies thresholds only for full SQLite runs.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.SystemPrintln", "PMD.CyclomaticComplexity"})
    public static void main(final String[] args) throws Exception {
        final Map<String, String> opts;
        final boolean full;
        final Storage storage;
        final Path workDir;
        final String filter;
        final List<RunSpec> plan;
        final int repeat;
        final List<List<RunSpec>> rounds;
        final int total;
        final Runner runner;
        final List<Map<String, Double>> measured;
        int n;
        final Map<String, List<Double>> samples;
        final Map<String, Double> results;
        final Report report;
        final Map<String, Double> baseline;
        final List<Comparator.Check> checks;
        ModuleGuard.requireSmqModule("부모 JVM");
        opts = parse(args);
        full = opts.getOrDefault("mode", "smoke").equals("full");
        if (!full && !opts.getOrDefault("mode", "smoke").equals("smoke"))
            throw new IllegalArgumentException("--mode는 smoke 또는 full");
        storage = Storage.valueOf(opts.getOrDefault("storage", "sqlite").toUpperCase(Locale.ROOT));
        workDir = Path.of(opts.getOrDefault("work-dir", "build/benchmark/work")).toAbsolutePath();
        Files.createDirectories(workDir);
        filter = opts.getOrDefault("filter", "");

        plan = (full ? Plan.full(storage) : Plan.smoke(storage))
                .stream().filter(s -> s.key().contains(filter)).toList();
        if (plan.isEmpty()) throw new IllegalArgumentException("조건이 하나도 남지 않았다: filter=" + filter);

        repeat = Integer.parseInt(opts.getOrDefault("repeat", String.valueOf(Plan.DEFAULT_REPEATS)));
        rounds = Plan.rounds(plan, full ? repeat : 1);
        total = rounds.stream().mapToInt(List::size).sum();

        runner = new Runner(workDir);
        measured = new ArrayList<>();
        n = 0;
        for (int round = 0; round < rounds.size(); round++) {
            for (final RunSpec spec : rounds.get(round)) {
                final Map<String, Double> m;
                n++;
                System.out.printf(
                        Locale.getDefault(Locale.Category.FORMAT),
                        "[%d/%d] 바퀴 %d/%d %s%n",
                        n,
                        total,
                        round + 1,
                        rounds.size(),
                        spec.key());
                m = runner.run(spec);
                m.forEach((k, v) ->
                        System.out.printf(Locale.getDefault(Locale.Category.FORMAT), "        %s = %.2f%n", k, v));
                measured.add(m);
            }
        }
        // 항목 키 → 바퀴별 표본. 바퀴 순서는 실행 순서와 같고, 한 바퀴에 한 번씩 재므로 같은 바퀴끼리 짝이 맞는다.
        samples = Aggregator.samples(measured);
        results = new LinkedHashMap<>();
        samples.forEach((k, v) -> results.put(k, Aggregator.median(v)));

        report = Aggregator.report(Environment.capture(full ? "full" : "smoke", storage.id(), workDir), samples);
        if (opts.containsKey("output")) {
            final Path out;
            out = Path.of(opts.get("output")).toAbsolutePath();
            Files.createDirectories(out.getParent());
            Files.writeString(out, report.toPrettyJson());
            System.out.println("결과 파일: " + out);
        }

        if (!full || storage != Storage.SQLITE) {
            System.out.println(full ? "인메모리 결과는 진단값이다. 임계값을 적용하지 않는다." : "스모크 실행이 끝났다. 임계값은 적용하지 않는다.");
            return;
        }
        baseline = loadBaseline(opts.get("baseline"));
        checks = Comparator.compare(samples, baseline);
        System.out.println();
        System.out.print(Comparator.render(checks));
        System.out.println();
        System.out.print(ChunkTable.render(results));
        System.exit(Comparator.exitCode(checks));
    }

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.SystemPrintln")
    private static Map<String, Double> loadBaseline(final String path) throws IOException {
        if (path == null || !Files.exists(Path.of(path))) {
            System.out.println("기준선 파일이 없어 하한만 검사한다: " + path);
            return Map.of();
        }
        return Report.fromJson(Files.readString(Path.of(path))).entries();
    }

    private static Map<String, String> parse(final String[] args) {
        final Map<String, String> m;
        m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            final String name;
            if (!args[i].startsWith("--") || i + 1 >= args.length)
                throw new IllegalArgumentException("잘못된 인자: " + args[i]);
            name = args[i].substring(2);
            i++;
            m.put(name, args[i]);
        }
        return m;
    }
}
