package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.PolicyException;
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
import java.util.Locale;
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

    /** 고정 속도 모드에서 이만큼 연속으로 예외가 나면 멈춘다. */
    static final int MAX_CONSECUTIVE_ERRORS = 3;

    private static final int MAX_VIOLATIONS_PRINTED = 20;
    private static final int SUMMARY_ROWS = 10;

    private final Path outDir;
    private final Path jsonLines;
    private final Map<String, String> environment;
    private final Limits limits;
    private final Executor executor;
    private final PrintStream out;

    Session(
            final Path outDir,
            final Path jsonLines,
            final Map<String, String> environment,
            final Limits limits,
            final Executor executor,
            final PrintStream out) {
        this.outDir = outDir;
        this.jsonLines = jsonLines;
        this.environment = environment;
        this.limits = limits;
        this.executor = executor;
        this.out = out;
    }

    Path failureDir(final long seed) {
        return outDir.resolve("failures").resolve(Long.toString(seed));
    }

    /**
     * @param budgetMillis 새 실행을 시작할 수 있는 시간. 지나면 더 시작하지 않는다(이미 도는 실행은 끝낸다)
     * @return 모두 통과하면 {@link #EXIT_OK}, 실패가 있으면 {@link #EXIT_FAILED}. 고정 속도 모드에서는 백로그 증가({@link Verdict#BACKLOG_GROWING}), 달성률 미달({@link Verdict#UNDER_RATE}), 예외(연속 {@link #MAX_CONSECUTIVE_ERRORS}번 미만)를 기록만 하고 계속 돌고, 다른 실패는 거기서 멈춘다
     */
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The session loop keeps budget, failure continuation, and reporting decisions in execution order.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    int run(final Iterator<Long> seeds, final long budgetMillis) {
        final long start;
        final List<RunResult> results;
        int exit;
        int n;
        int consecutiveErrors;
        start = System.nanoTime();
        results = new ArrayList<>();
        exit = EXIT_OK;
        n = 0;
        consecutiveErrors = 0;
        while (seeds.hasNext()) {
            final long seed;
            final RunParams params;
            final RunResult result;
            if (n > 0 && (System.nanoTime() - start) / 1_000_000L >= budgetMillis) {
                break;
            }
            seed = seeds.next();
            params = RunParams.fromSeed(seed, limits);
            n++;
            out.printf(Locale.getDefault(Locale.Category.FORMAT), "[%d] seed=%d %s 실행%n", n, seed, params.durability());
            result = runOne(params);
            results.add(result);
            if (!result.passed()) {
                reportFailure(result);
                exit = EXIT_FAILED;
                consecutiveErrors = result.verdict() == Verdict.ERROR ? consecutiveErrors + 1 : 0;
                if (continuesAfter(result, consecutiveErrors)) {
                    continue;
                }
                break;
            }
            consecutiveErrors = 0;
            out.printf(
                    Locale.getDefault(Locale.Category.FORMAT),
                    "    PASS  %.0f msg/s, %.2f MB/s, p50 %s, p99 %s%n",
                    result.msgPerSec(),
                    result.mbPerSec(),
                    ms(result.p50Ms()),
                    ms(result.p99Ms()));
            if (result.soak() != null) {
                out.println("    " + soakLine(result.soak()));
            }
        }
        out.print(Summary.render(results, SUMMARY_ROWS));
        out.printf(
                Locale.getDefault(Locale.Category.FORMAT),
                "합계: %d회 실행, %s%n",
                results.size(),
                exit == EXIT_OK ? "위반 없음" : "실패");
        return exit;
    }

    /**
     * 실패한 뒤에도 다음 시드를 돌지. 고정 속도 모드는 긴 실행이라 한 번의 실패로 끊지 않는다. 백로그 증가(용량), 달성률 미달, 예외(예:
     * SmqStorageException)는 기록하고 계속 가되, 예외가 연속 {@link #MAX_CONSECUTIVE_ERRORS}번이면 하네스 문제로 보고 멈춘다. 불변식
     * 위반과 소진 실패는 데이터가 틀렸다는 뜻이라 바로 멈춘다. 무작위 모드는 첫 실패에서 멈춘다.
     */
    private static boolean continuesAfter(final RunResult failed, final int consecutiveErrors) {
        if (!failed.params().isSoak()) {
            return false;
        }
        return failed.verdict() == Verdict.BACKLOG_GROWING
                || failed.verdict() == Verdict.UNDER_RATE
                || (failed.verdict() == Verdict.ERROR && consecutiveErrors < MAX_CONSECUTIVE_ERRORS);
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "An executor runtime failure is recorded as a run result so the session can report it.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.CyclomaticComplexity",
            reason = "Run cleanup depends on pass, soak mode, and verdict to retain the right evidence.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.CyclomaticComplexity"})
    private RunResult runOne(final RunParams params) {
        RunResult result;
        final Path dir;
        dir = outDir.resolve("work").resolve(Long.toString(params.seed()));

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
                    // 고정 속도 실행은 JFR과 메트릭 파일이 결과물이다. 큰 DB와 발행·전달 기록만 지운다.
                    deleteBulkyFiles(dir);
                } else {
                    deleteTree(dir);
                }
                // 세션은 요약표를 위해 모든 결과를 쌓는다. 통과한 실행의 기록은 여기서 버려야 긴 세션이 힙을 채우지 않는다.
                return result.withoutRecords();
            }
            if (params.isSoak() && result.verdict() != Verdict.VIOLATION) {
                // 위반이 아닌 실패(백로그 증가, 소진 시간 초과, 예외)는 원인이 기록이 아니라서 수 GB의 기록 파일은 남기지 않는다.
                deleteSpoolFiles(dir);
            }
            preserve(result, dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    private void append(final RunResult r) throws IOException {
        final Map<String, Object> m;
        m = new LinkedHashMap<>();
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
    private void preserve(final RunResult r, final Path dir) throws IOException {
        final RunResult.Records rec;
        final Path first;
        Path target;
        rec = r.records();
        // 기록을 디스크에 쌓은 실행(고정 속도 모드)은 p<n>-*.bin이 기록이다. 힙에는 없으므로 빈 파일을 만들지 않는다.
        if (!r.params().isSoak()) {
            writeLines(dir.resolve("published.jsonl"), rec.published().stream().map(RecordsJson::line));
            writeLines(
                    dir.resolve("deliveries.jsonl"), rec.deliveries().stream().map(RecordsJson::line));
        }
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
        first = failureDir(r.params().seed());
        target = first;
        for (int attempt = 2; Files.exists(target); attempt++) {
            target = first.resolve("reruns").resolve(Integer.toString(attempt));
        }
        Files.createDirectories(target.getParent());
        try {
            Files.move(dir, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(dir, target);
        }
    }

    /** {@code violations.txt} 끝에 덧붙이는 진단 줄. */
    private static Stream<String> diagnostics(final RunResult r) {
        final Stream<String> detail;
        final Stream<String> wal;
        final Stream<String> jvm;
        detail = r.errorDetail() == null
                ? Stream.empty()
                : r.errorDetail().describe().lines();
        wal = r.walMaxBytes() == null ? Stream.empty() : Stream.of("walMaxBytes=" + r.walMaxBytes());
        jvm = r.jvmMetrics() == null ? Stream.empty() : Stream.of("jvmMetrics=" + r.jvmMetrics());
        return Stream.of(detail, wal, jvm).flatMap(s -> s);
    }

    private static void writeLines(final Path file, final Stream<String> lines) throws IOException {
        try (lines) {
            Files.write(file, (Iterable<String>) lines::iterator);
        }
    }

    private void reportFailure(final RunResult r) {
        final String durabilityOption;
        final List<Violation> vs;
        out.println();
        out.println("실패: " + r.verdict() + " seed=" + r.params().seed());
        durabilityOption =
                limits.durability().map(d -> " --durability " + d.name()).orElse("");
        out.println("다시 돌리려면: ./gradlew randomBench --seed " + r.params().seed() + durabilityOption);
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
        vs = r.violations();
        out.println("어긴 불변식 " + vs.size() + "개:");
        for (int i = 0; i < Math.min(vs.size(), MAX_VIOLATIONS_PRINTED); i++) {
            out.println("  - " + RecordsJson.describe(vs.get(i)));
        }
        if (vs.size() > MAX_VIOLATIONS_PRINTED) {
            out.println("  ... 외 " + (vs.size() - MAX_VIOLATIONS_PRINTED) + "개(violations.txt와 jsonl 기록을 본다)");
        }
        out.println("보존한 파일: " + failureDir(r.params().seed()));
    }

    private static String soakLine(final SoakJudge.Report r) {
        return String.format(
                Locale.getDefault(Locale.Category.FORMAT),
                "목표 %d msg/s, 달성률 %.0f%%, ACK %.0f msg/s, 백로그 기울기 %.1f개/초, 최대 깊이 %d%s",
                r.targetRate(),
                r.achievedRatio() * 100,
                r.ackRate(),
                r.backlogSlopePerSec(),
                r.maxDepth(),
                r.backlogGrowing() ? " (백로그 증가)" : "");
    }

    private static String ms(final Double d) {
        return d == null ? "-" : String.format(Locale.getDefault(Locale.Category.FORMAT), "%.0fms", d);
    }

    /** SQLite 파일(본체, WAL, 공유 메모리)과 발행·전달 기록만 지운다. 긴 실행에서는 둘 다 크다. */
    private static void deleteBulkyFiles(final Path dir) throws IOException {
        for (final String name : List.of("smq.db", "smq.db-wal", "smq.db-shm")) {
            Files.deleteIfExists(dir.resolve(name));
        }
        deleteSpoolFiles(dir);
    }

    /** 발행·전달 기록(jsonl)과 디스크에 쌓은 기록(bin). */
    private static void deleteSpoolFiles(final Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            for (final Path p : (Iterable<Path>) s.filter(
                    f -> f.getFileName().toString().matches("(published|deliveries)\\.(jsonl|bin)"))::iterator) {
                Files.delete(p);
            }
        }
    }

    private static void deleteTree(final Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> s = Files.walk(dir)) {
            for (final Path p : (Iterable<Path>) s.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(p);
            }
        }
    }
}
