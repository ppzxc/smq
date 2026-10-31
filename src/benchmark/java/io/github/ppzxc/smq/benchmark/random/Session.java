package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.Json;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 시드를 차례로 실행하는 세션. 실행마다 JSON 한 줄을 쌓고, 통과한 실행의 파일은 지우고, 첫 실패에서 멈춘다. 실패한 실행의 파일과
 * 기록은 {@code failures/<seed>/}에 남긴다. 실제 실행은 {@link Executor}가 하므로 이 흐름은 SQLite 없이 확인할 수 있다.
 */
final class Session {

    interface Executor {
        /** {@code dir}은 이 실행이 쓸 빈 디렉터리다. */
        RunResult execute(RunParams params, Path dir);
    }

    static final int EXIT_OK = 0;
    static final int EXIT_FAILED = 1;
    private static final int MAX_VIOLATIONS_PRINTED = 20;
    private static final int SUMMARY_ROWS = 10;

    private final Path outDir;
    private final Path jsonLines;
    private final Map<String, String> environment;
    private final Limits limits;
    private final Executor executor;
    private final PrintStream out;

    Session(
            Path outDir,
            Path jsonLines,
            Map<String, String> environment,
            Limits limits,
            Executor executor,
            PrintStream out) {
        this.outDir = outDir;
        this.jsonLines = jsonLines;
        this.environment = environment;
        this.limits = limits;
        this.executor = executor;
        this.out = out;
    }

    Path failureDir(long seed) {
        return outDir.resolve("failures").resolve(Long.toString(seed));
    }

    /**
     * @param budgetMillis 새 실행을 시작할 수 있는 시간. 지나면 더 시작하지 않는다(이미 도는 실행은 끝낸다)
     * @return 모두 통과하면 {@link #EXIT_OK}, 첫 실패에서 멈추면 {@link #EXIT_FAILED}
     */
    int run(Iterator<Long> seeds, long budgetMillis) {
        long start = System.nanoTime();
        List<RunResult> results = new ArrayList<>();
        int exit = EXIT_OK;
        int n = 0;
        while (seeds.hasNext()) {
            if (n > 0 && (System.nanoTime() - start) / 1_000_000L >= budgetMillis) {
                break;
            }
            long seed = seeds.next();
            RunParams params = RunParams.fromSeed(seed, limits);
            out.printf("[%d] seed=%d %s 실행%n", ++n, seed, params.durability());
            RunResult result = runOne(params);
            results.add(result);
            if (!result.passed()) {
                reportFailure(result);
                exit = EXIT_FAILED;
                break;
            }
            out.printf(
                    "    PASS  %.0f msg/s, %.2f MB/s, p50 %s, p99 %s%n",
                    result.msgPerSec(), result.mbPerSec(), ms(result.p50Ms()), ms(result.p99Ms()));
            if (result.soak() != null) {
                out.println("    " + soakLine(result.soak()));
            }
        }
        out.print(Summary.render(results, SUMMARY_ROWS));
        out.printf("합계: %d회 실행, %s%n", results.size(), exit == EXIT_OK ? "위반 없음" : "실패");
        return exit;
    }

    private RunResult runOne(RunParams params) {
        Path dir = outDir.resolve("work").resolve(Long.toString(params.seed()));
        RunResult result;
        try {
            deleteTree(dir);
            Files.createDirectories(dir);
            result = executor.execute(params, dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            result = RunResult.error(params, e);
        }
        try {
            append(result);
            if (result.passed()) {
                if (params.isSoak()) {
                    // 고정 속도 실행은 JFR과 메트릭 파일이 결과물이다. 큰 DB와 자식의 발행·전달 기록만 지운다.
                    deleteBulkyFiles(dir);
                } else {
                    deleteTree(dir);
                }
                // 세션은 요약표를 위해 모든 결과를 쌓는다. 통과한 실행의 기록은 여기서 버려야 긴 세션이 힙을 채우지 않는다.
                return result.withoutRecords();
            }
            preserve(result, dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    private void append(RunResult r) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seed", r.params().seed());
        m.put("params", r.params().toMap());
        m.put("msgPerSec", r.msgPerSec());
        m.put("mbPerSec", r.mbPerSec());
        m.put("p50Ms", r.p50Ms());
        m.put("p99Ms", r.p99Ms());
        m.put("verdict", r.verdict().name());
        m.put("violations", r.violations().stream().map(RecordsJson::describe).toList());
        m.put("error", r.error());
        // 아래 둘은 뒤에 덧붙인 필드다. 위의 기존 필드 의미는 그대로다.
        m.put("errorDetail", r.errorDetail() == null ? null : r.errorDetail().toMap());
        m.put("walMaxBytes", r.walMaxBytes());
        m.put("jvmMetrics", r.jvmMetrics() == null ? null : r.jvmMetrics().toMap());
        m.put("soak", r.soak() == null ? null : r.soak().toMap());
        m.put(
                "childMetrics",
                r.childMetrics().stream().map(JvmMetricsSampler.Summary::toMap).toList());
        m.put("workDirFs", environment.get("workDirFs"));
        m.put("caveat", environment.get("caveat"));
        Files.createDirectories(jsonLines.getParent());
        Files.writeString(
                jsonLines,
                Json.write(m) + "\n",
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);
    }

    /** 실행 디렉터리(SQLite 파일 포함)에 기록을 더해 {@code failures/<seed>/}로 옮긴다. */
    private void preserve(RunResult r, Path dir) throws IOException {
        RunResult.Records rec = r.records();
        writeLines(dir.resolve("published.jsonl"), rec.published().stream().map(RecordsJson::line));
        writeLines(dir.resolve("deliveries.jsonl"), rec.deliveries().stream().map(RecordsJson::line));
        writeLines(dir.resolve("counts.jsonl"), rec.samples().stream().map(RecordsJson::line));
        writeLines(
                dir.resolve("violations.txt"),
                Stream.concat(
                        Stream.of(
                                "seed=" + r.params().seed(),
                                "verdict=" + r.verdict(),
                                "error=" + r.error(),
                                r.params().describe()),
                        Stream.concat(r.violations().stream().map(RecordsJson::describe), diagnostics(r))));
        Path target = failureDir(r.params().seed());
        deleteTree(target);
        Files.createDirectories(target.getParent());
        try {
            Files.move(dir, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(dir, target);
        }
    }

    /** {@code violations.txt} 끝에 덧붙이는 진단 줄. */
    private static Stream<String> diagnostics(RunResult r) {
        Stream<String> detail = r.errorDetail() == null
                ? Stream.empty()
                : r.errorDetail().describe().lines();
        Stream<String> wal = r.walMaxBytes() == null ? Stream.empty() : Stream.of("walMaxBytes=" + r.walMaxBytes());
        Stream<String> jvm = r.jvmMetrics() == null ? Stream.empty() : Stream.of("jvmMetrics=" + r.jvmMetrics());
        return Stream.of(detail, wal, jvm).flatMap(s -> s);
    }

    private static void writeLines(Path file, Stream<String> lines) throws IOException {
        try (lines) {
            Files.write(file, (Iterable<String>) lines::iterator);
        }
    }

    private void reportFailure(RunResult r) {
        out.println();
        out.println("실패: " + r.verdict() + " seed=" + r.params().seed());
        out.println("다시 돌리려면: ./gradlew randomBench --seed " + r.params().seed());
        out.println("매개변수:");
        out.println(r.params().describe());
        if (r.error() != null) {
            out.println("원인: " + r.error());
        }
        if (r.errorDetail() != null) {
            out.println(r.errorDetail().describe());
        }
        if (r.walMaxBytes() != null) {
            out.println("smq.db-wal 최대 크기: " + r.walMaxBytes() + " 바이트");
        }
        if (r.jvmMetrics() != null) {
            out.println("JVM 메트릭: " + r.jvmMetrics().toMap());
        }
        if (r.soak() != null) {
            out.println(soakLine(r.soak()));
        }
        for (int i = 0; i < r.childMetrics().size(); i++) {
            out.println("자식 " + i + " 메트릭: " + r.childMetrics().get(i).toMap());
        }
        List<Violation> vs = r.violations();
        out.println("어긴 불변식 " + vs.size() + "개:");
        for (int i = 0; i < Math.min(vs.size(), MAX_VIOLATIONS_PRINTED); i++) {
            out.println("  - " + RecordsJson.describe(vs.get(i)));
        }
        if (vs.size() > MAX_VIOLATIONS_PRINTED) {
            out.println("  ... 외 " + (vs.size() - MAX_VIOLATIONS_PRINTED) + "개(violations.txt와 jsonl 기록을 본다)");
        }
        out.println("보존한 파일: " + failureDir(r.params().seed()));
    }

    private static String soakLine(SoakJudge.Report r) {
        return String.format(
                "목표 %d msg/s, 달성률 %.0f%%, ACK %.0f msg/s, 백로그 기울기 %.1f개/초, 최대 깊이 %d%s",
                r.targetRate(),
                r.achievedRatio() * 100,
                r.ackRate(),
                r.backlogSlopePerSec(),
                r.maxDepth(),
                r.backlogGrowing() ? " (백로그 증가)" : "");
    }

    private static String ms(Double d) {
        return d == null ? "-" : String.format("%.0fms", d);
    }

    /** SQLite 파일(본체, WAL, 공유 메모리)과 자식 JVM이 남긴 발행·전달 기록만 지운다. 긴 실행에서는 둘 다 크다. */
    private static void deleteBulkyFiles(Path dir) throws IOException {
        for (String name : List.of("smq.db", "smq.db-wal", "smq.db-shm")) {
            Files.deleteIfExists(dir.resolve(name));
        }
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s.filter(
                    f -> f.getFileName().toString().matches("p\\d+-(published|deliveries)\\.jsonl"))::iterator) {
                Files.delete(p);
            }
        }
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) s.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(p);
            }
        }
    }
}
