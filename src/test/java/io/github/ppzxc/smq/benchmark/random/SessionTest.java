package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 세션의 흐름(JSON 줄, 파일 보존과 삭제, 첫 위반에서 멈춤, 예산, 출력)을 가짜 실행기로 확인한다. SQLite는 쓰지 않는다. */
class SessionTest {

    private static final Limits LIMITS = Limits.smoke(2_000);

    @TempDir
    Path dir;

    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<Long> executed = new ArrayList<>();

    private String output() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private Session session(Session.Executor executor) {
        return new Session(
                dir,
                dir.resolve("runs.jsonl"),
                Map.of("workDirFs", "ext4", "caveat", "테스트"),
                LIMITS,
                executor,
                new PrintStream(bytes, true, StandardCharsets.UTF_8));
    }

    private static RunResult result(
            RunParams p, Verdict v, double rate, Double p99, List<Violation> violations, RunResult.Records records) {
        return new RunResult(
                p,
                v,
                rate,
                rate / 1000,
                p99 == null ? null : p99 / 2,
                p99,
                violations,
                null,
                records,
                null,
                null,
                null,
                null,
                java.util.List.of());
    }

    /** 실행기가 SQLite 파일을 흉내 내 디렉터리에 남긴다. */
    private Session.Executor fake(java.util.function.Function<RunParams, RunResult> make) {
        return (params, runDir) -> {
            executed.add(params.seed());
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return make.apply(params);
        };
    }

    private static final Violation LOSS =
            new Violation(Invariant.LOSS, List.of(new Tag(7, 0, 3)), List.of(99L), "전달됐지만 OK로 ACK되지 않았다");

    @Test
    void passingRunsAppendOneJsonLineEachAndDeleteTheirFiles() throws IOException {
        Session s = session(fake(p -> result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        assertEquals(Session.EXIT_OK, s.run(List.of(1L, 2L).iterator(), Long.MAX_VALUE));

        List<String> lines = Files.readAllLines(dir.resolve("runs.jsonl"));
        assertEquals(2, lines.size());
        assertTrue(lines.getFirst().contains("\"seed\":1"));
        assertTrue(lines.getFirst().contains("\"verdict\":\"PASS\""));
        assertTrue(lines.getFirst().contains("\"workDirFs\":\"ext4\""));
        assertTrue(lines.getFirst().contains("\"caveat\":\"테스트\""));
        assertTrue(lines.getFirst().contains("\"durability\""), "풀어 쓴 매개변수가 들어 있다");
        assertTrue(lines.getFirst().contains("\"p99Ms\":5"));
        assertFalse(Files.exists(dir.resolve("work/1")));
        assertFalse(Files.exists(dir.resolve("work/2")));
        assertFalse(Files.exists(dir.resolve("failures")));
    }

    @Test
    void passedRunsDoNotKeepTheirRecordsAliveForTheRestOfTheSession() {
        List<java.lang.ref.WeakReference<List<Published>>> firstRecords = new ArrayList<>();
        boolean[] collectedWhileSecondRuns = {false};
        Session s = session(fake(p -> {
            if (p.seed() == 2) {
                for (int i = 0; i < 10 && !firstRecords.get(0).refersTo(null); i++) {
                    System.gc();
                }
                collectedWhileSecondRuns[0] = firstRecords.get(0).refersTo(null);
            }
            List<Published> published = new ArrayList<>(
                    List.of(new Published(new Tag(1, 0, 1), new Content(5, Map.of("bench-tag", "1/0/1")), 100, 0)));
            if (p.seed() == 1) {
                firstRecords.add(new java.lang.ref.WeakReference<>(published));
            }
            return result(
                    p,
                    Verdict.PASS,
                    1000,
                    5.0,
                    List.of(),
                    new RunResult.Records(published, new ArrayList<>(), new ArrayList<>()));
        }));

        assertEquals(Session.EXIT_OK, s.run(List.of(1L, 2L).iterator(), Long.MAX_VALUE));

        assertTrue(collectedWhileSecondRuns[0], "통과한 실행의 기록은 다음 실행이 도는 동안 메모리에서 내려가야 한다");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecords() throws IOException {
        RunResult.Records records = new RunResult.Records(
                List.of(new Published(new Tag(7, 0, 3), new Content(5, Map.of("bench-tag", "7/0/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        Session s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));

        assertEquals(Session.EXIT_FAILED, s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));

        assertEquals(List.of(1L, 2L), executed, "3번째 시드는 돌지 않는다");
        Path failed = s.failureDir(2);
        assertEquals("db", Files.readString(failed.resolve("smq.db")));
        assertTrue(Files.exists(failed.resolve("smq.db-wal")));
        assertTrue(Files.readString(failed.resolve("published.jsonl")).contains("7/0/3"));
        assertTrue(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        assertTrue(Files.exists(failed.resolve("deliveries.jsonl")));
        assertTrue(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        assertFalse(Files.exists(dir.resolve("work/2")), "실패한 파일은 옮겨졌다");
        assertFalse(Files.exists(dir.resolve("work/1")), "통과한 파일은 지워졌다");

        String out = output();
        assertTrue(out.contains("seed=2"));
        assertTrue(out.contains("LOSS"), "어긴 불변식");
        assertTrue(out.contains("7/0/3"), "관련 태그");
        assertTrue(out.contains("99"), "관련 메시지 id");
        assertTrue(out.contains("leaseMillis"), "풀어 쓴 매개변수");
        assertTrue(out.contains(failed.toString()));
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserve() {
        Session s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        assertEquals(Session.EXIT_FAILED, s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        assertTrue(Files.exists(s.failureDir(1).resolve("smq.db")));

        Session s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        assertEquals(Session.EXIT_FAILED, s2.run(List.of(5L).iterator(), Long.MAX_VALUE));
        assertTrue(output().contains("harness broke"));
        assertTrue(Files.exists(s2.failureDir(5)));
    }

    @Test
    void budgetStopsStartingNewRunsButAlwaysRunsOne() {
        Session s = session(fake(p -> result(p, Verdict.PASS, 1, 1.0, List.of(), RunResult.Records.EMPTY)));
        assertEquals(Session.EXIT_OK, s.run(List.of(1L, 2L, 3L).iterator(), 0));
        assertEquals(List.of(1L), executed);
    }

    @Test
    void summaryIsSortedByThroughputAndP99() {
        Session s = session(fake(p -> result(
                p, Verdict.PASS, p.seed() * 100.0, p.seed() == 2 ? 900.0 : 5.0, List.of(), RunResult.Records.EMPTY)));
        s.run(List.of(3L, 1L, 2L).iterator(), Long.MAX_VALUE);
        String out = output();
        String byThroughput = out.substring(out.indexOf("처리량이 낮은 순"), out.indexOf("p99가 높은 순"));
        assertTrue(byThroughput.indexOf("\n  1 ") < byThroughput.indexOf("\n  2 "));
        assertTrue(byThroughput.indexOf("\n  2 ") < byThroughput.indexOf("\n  3 "));
        String byP99 = out.substring(out.indexOf("p99가 높은 순"));
        assertTrue(byP99.indexOf("\n  2 ") < byP99.indexOf("\n  1 "), "p99가 가장 큰 시드가 위");
    }

    // ---- ERROR 진단 필드

    private static final ErrorDetail CHILD_OOM = new ErrorDetail(
            "child-2",
            "java.lang.OutOfMemoryError",
            "Java heap space",
            "java.lang.OutOfMemoryError: Java heap space\n\tat a.B.c(B.java:1)\n",
            1,
            "oom 꼬리");

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFile() throws IOException {
        Session s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        assertEquals(Session.EXIT_FAILED, s.run(List.of(8L).iterator(), Long.MAX_VALUE));

        Map<?, ?> line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        assertEquals("ERROR", line.get("verdict"));
        assertEquals("boom", line.get("error"), "기존 필드 의미는 그대로");
        Map<?, ?> detail = (Map<?, ?>) line.get("errorDetail");
        assertEquals("child-2", detail.get("location"));
        assertEquals("java.lang.OutOfMemoryError", detail.get("exceptionClass"));
        assertEquals("Java heap space", detail.get("message"));
        assertTrue(((String) detail.get("stackTrace")).contains("at a.B.c"));
        assertEquals(1.0, detail.get("exitCode"));
        assertEquals("oom 꼬리", detail.get("stderrTail"));
        assertEquals(852_000_000.0, line.get("walMaxBytes"));

        String txt = Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"); // Windows 줄바꿈
        assertTrue(txt.startsWith("seed=8\nverdict=ERROR\nerror=boom\n"), "앞쪽 줄은 그대로");
        assertTrue(txt.contains("errorLocation=child-2"));
        assertTrue(txt.contains("exitCode=1"));
        assertTrue(txt.contains("oom 꼬리"));
        assertTrue(txt.contains("\tat a.B.c(B.java:1)"), "스택 트레이스 전체");
        assertTrue(txt.contains("walMaxBytes=852000000"));

        String out = output();
        assertTrue(out.contains("child-2"));
        assertTrue(out.contains("at a.B.c"));
    }

    @Test
    void passingRunsRecordTheWalSizeAndNoErrorDetail() throws IOException {
        Session s = session(fake(p -> new RunResult(
                p,
                Verdict.PASS,
                100,
                0.1,
                1.0,
                2.0,
                List.of(),
                null,
                RunResult.Records.EMPTY,
                null,
                4096L,
                null,
                null,
                List.of())));
        assertEquals(Session.EXIT_OK, s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        Map<?, ?> line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        assertEquals(4096.0, line.get("walMaxBytes"));
        assertNull(line.get("errorDetail"));
    }

    private Session soakSession(Session.Executor executor) {
        return new Session(
                dir,
                dir.resolve("runs.jsonl"),
                Map.of("workDirFs", "ext4", "caveat", "테스트"),
                LIMITS.withRate(500),
                executor,
                new PrintStream(bytes, true, StandardCharsets.UTF_8));
    }

    private static SoakJudge.Report report(boolean growing) {
        return new SoakJudge.Report(500, 0.93, 460, 12.5, 800, growing);
    }

    private static RunResult soakResult(RunParams p, Verdict v, SoakJudge.Report soak) {
        return new RunResult(
                p,
                v,
                460,
                0.5,
                5.0,
                9.0,
                List.of(),
                null,
                RunResult.Records.EMPTY,
                null,
                null,
                null,
                soak,
                List.of(new JvmMetricsSampler.Summary(1, 2, 3, 4, 5, 6, 7, 8, 0.7, 0.35)));
    }

    @Test
    void 고정_속도_결과는_jsonl에_달성률과_자식_메트릭이_붙는다() throws IOException {
        Session s = soakSession(fake(p -> soakResult(p, Verdict.PASS, report(false))));
        assertEquals(Session.EXIT_OK, s.run(List.of(1L).iterator(), Long.MAX_VALUE));

        String line = Files.readString(dir.resolve("runs.jsonl"));
        assertTrue(line.contains("\"soak\":{\"targetRate\":500"), line);
        assertTrue(line.contains("\"achievedRatio\":0.93"), line);
        assertTrue(line.contains("\"childMetrics\":[{"), line);
        assertTrue(line.contains("\"meanCpuLoad\":0.35"), line);
        assertTrue(output().contains("달성률 93%"), output());
    }

    @Test
    void 고정_속도가_아니면_soak_필드가_null이고_자식_메트릭은_빈_목록이다() throws IOException {
        Session s = session(fake(p -> result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        s.run(List.of(1L).iterator(), Long.MAX_VALUE);
        String line = Files.readString(dir.resolve("runs.jsonl"));
        assertTrue(line.contains("\"soak\":null"), line);
        assertTrue(line.contains("\"childMetrics\":[]"), line);
    }

    @Test
    void 고정_속도_실행은_통과해도_JFR과_메트릭_파일을_남기고_DB와_큰_기록만_지운다() throws IOException {
        Session.Executor exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("p0.jfr"), "jfr");
                Files.writeString(runDir.resolve("p0-jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("p0-published.jsonl"), "{}");
                Files.writeString(runDir.resolve("p0-deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);

        assertFalse(Files.exists(dir.resolve("work/1/smq.db")));
        assertFalse(Files.exists(dir.resolve("work/1/smq.db-wal")));
        assertEquals("jfr", Files.readString(dir.resolve("work/1/p0.jfr")));
        assertTrue(Files.exists(dir.resolve("work/1/p0-jvm-metrics.jsonl")));
        assertFalse(Files.exists(dir.resolve("work/1/p0-published.jsonl")), "큰 기록 파일도 지운다");
        assertFalse(Files.exists(dir.resolve("work/1/p0-deliveries.jsonl")));
    }

    @Test
    void 백로그_증가는_기록하고_계속_돌며_끝에서_실패로_종료한다() throws IOException {
        Session s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        assertEquals(Session.EXIT_FAILED, s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        assertEquals(List.of(1L, 2L, 3L), executed, "용량 문제는 다음 시드를 막지 않는다");
        String out = output();
        assertTrue(out.contains("BACKLOG_GROWING"), out);
        assertTrue(out.contains("백로그"), out);
        assertTrue(out.contains("합계: 3회 실행, 실패"), out);
        assertTrue(Files.exists(s.failureDir(2).resolve("smq.db")), "실패한 실행은 DB까지 보존한다");
        assertEquals(3, Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    void 고정_속도에서도_불변식_위반은_바로_멈춘다() {
        Session s =
                soakSession(fake(p -> soakResult(p, p.seed() == 2 ? Verdict.VIOLATION : Verdict.PASS, report(false))));
        assertEquals(Session.EXIT_FAILED, s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        assertEquals(List.of(1L, 2L), executed);
    }
}
