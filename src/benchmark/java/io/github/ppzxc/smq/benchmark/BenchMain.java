package io.github.ppzxc.smq.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
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
 *   <li>{@code --filter <문자열>}: 키에 이 문자열이 든 조건만 돈다. 예: {@code S1/P1}.
 * </ul>
 */
public final class BenchMain {

    private BenchMain() {}

    public static void main(String[] args) throws Exception {
        ModuleGuard.requireSmqModule("부모 JVM");
        Map<String, String> opts = parse(args);
        boolean full = opts.getOrDefault("mode", "smoke").equals("full");
        if (!full && !opts.getOrDefault("mode", "smoke").equals("smoke")) throw new IllegalArgumentException("--mode는 smoke 또는 full");
        Storage storage = Storage.valueOf(opts.getOrDefault("storage", "sqlite").toUpperCase());
        Path workDir = Path.of(opts.getOrDefault("work-dir", "build/benchmark/work")).toAbsolutePath();
        Files.createDirectories(workDir);
        String filter = opts.getOrDefault("filter", "");

        List<RunSpec> plan = (full ? Plan.full(storage) : Plan.smoke(storage)).stream()
                .filter(s -> s.key().contains(filter)).toList();
        if (plan.isEmpty()) throw new IllegalArgumentException("조건이 하나도 남지 않았다: filter=" + filter);

        Runner runner = new Runner(workDir);
        Map<String, Double> results = new LinkedHashMap<>();
        int n = 0;
        for (RunSpec spec : plan) {
            System.out.printf("[%d/%d] %s%n", ++n, plan.size(), spec.key());
            Map<String, Double> m = runner.run(spec);
            m.forEach((k, v) -> System.out.printf("        %s = %.2f%n", k, v));
            results.putAll(m);
        }

        Report report = new Report(Environment.capture(full ? "full" : "smoke", storage.id(), workDir), results);
        if (opts.containsKey("output")) {
            Path out = Path.of(opts.get("output")).toAbsolutePath();
            Files.createDirectories(out.getParent());
            Files.writeString(out, report.toPrettyJson());
            System.out.println("결과 파일: " + out);
        }

        if (!full || storage != Storage.SQLITE) {
            System.out.println(full ? "인메모리 결과는 진단값이다. 임계값을 적용하지 않는다."
                    : "스모크 실행이 끝났다. 임계값은 적용하지 않는다.");
            return;
        }
        Map<String, Double> baseline = loadBaseline(opts.get("baseline"));
        List<Comparator.Check> checks = Comparator.compare(results, baseline);
        System.out.println();
        System.out.print(Comparator.render(checks));
        System.out.println();
        System.out.print(ChunkTable.render(results));
        System.exit(Comparator.exitCode(checks));
    }

    private static Map<String, Double> loadBaseline(String path) throws IOException {
        if (path == null || !Files.exists(Path.of(path))) {
            System.out.println("기준선 파일이 없어 하한만 검사한다: " + path);
            return Map.of();
        }
        return Report.fromJson(Files.readString(Path.of(path))).entries();
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) throw new IllegalArgumentException("잘못된 인자: " + args[i]);
            m.put(args[i].substring(2), args[++i]);
        }
        return m;
    }
}
