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
        return new Session(dir, dir.resolve("runs.jsonl"), Map.of("workDirFs", "ext4", "caveat", "테스트"), LIMITS,
                executor, new PrintStream(bytes, true, StandardCharsets.UTF_8));
    }

    private static RunResult result(RunParams p, Verdict v, double rate, Double p99, List<Violation> violations,
            RunResult.Records records) {
        return new RunResult(p, v, rate, rate / 1000, p99 == null ? null : p99 / 2, p99, violations, null, records);
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

    private static final Violation LOSS = new Violation(Invariant.LOSS, List.of(new Tag(7, 0, 3)), List.of(99L),
            "전달됐지만 OK로 ACK되지 않았다");

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
    void firstViolationStopsTheSessionAndPreservesFilesAndRecords() throws IOException {
        RunResult.Records records = new RunResult.Records(
                List.of(new Published(new Tag(7, 0, 3), new Content(5, Map.of("bench-tag", "7/0/3")), 100, 0)),
                List.of(), List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
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
        Session s = session(fake(p -> result(p, Verdict.PASS, p.seed() * 100.0, p.seed() == 2 ? 900.0 : 5.0,
                List.of(), RunResult.Records.EMPTY)));
        s.run(List.of(3L, 1L, 2L).iterator(), Long.MAX_VALUE);
        String out = output();
        String byThroughput = out.substring(out.indexOf("처리량이 낮은 순"), out.indexOf("p99가 높은 순"));
        assertTrue(byThroughput.indexOf("\n  1 ") < byThroughput.indexOf("\n  2 "));
        assertTrue(byThroughput.indexOf("\n  2 ") < byThroughput.indexOf("\n  3 "));
        String byP99 = out.substring(out.indexOf("p99가 높은 순"));
        assertTrue(byP99.indexOf("\n  2 ") < byP99.indexOf("\n  1 "), "p99가 가장 큰 시드가 위");
    }
}
