package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.PolicyException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@PolicyException(
        rule = "PMD.CyclomaticComplexity",
        reason = "Each independent test method stays below the limit; the class total sums all scenarios.",
        approvedBy = "ppzxc",
        reviewBy = "2027-11-28")
@SuppressWarnings("PMD.CyclomaticComplexity")
class SessionTest {

    private static final Limits LIMITS = Limits.smoke(2_000);

    @TempDir
    Path dir;

    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    private final List<Long> executed = new ArrayList<>();

    private String output() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private Session session(final Session.Executor executor) {
        return session(executor, LIMITS);
    }

    private Session session(final Session.Executor executor, final Limits limits) {
        return new Session(
                dir,
                dir.resolve("runs.jsonl"),
                Map.of("workDirFs", "ext4", "caveat", "테스트"),
                limits,
                executor,
                new PrintStream(bytes, true, StandardCharsets.UTF_8));
    }

    private static RunResult result(
            final RunParams p,
            final Verdict v,
            final double rate,
            final Double p99,
            final List<Violation> violations,
            final RunResult.Records records) {
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
                null);
    }

    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    private Session.Executor fake(final java.util.function.Function<RunParams, RunResult> make) {
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
            new Violation(Invariant.LOSS, List.of(new Tag(7, 3)), List.of(99L), "전달됐지만 OK로 ACK되지 않았다");

    private record PassingSession(int exitCode, List<String> lines) {}

    private PassingSession runPassingSession() throws IOException {
        final Session session;
        final int exitCode;
        session = session(fake(p -> result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        exitCode = session.run(List.of(1L, 2L).iterator(), Long.MAX_VALUE);
        return new PassingSession(exitCode, Files.readAllLines(dir.resolve("runs.jsonl")));
    }

    @Test
    void passingRunsExitSuccessfully() throws IOException {
        assertEquals(Session.EXIT_OK, runPassingSession().exitCode(), "Passing runs should exit successfully.");
    }

    @Test
    void passingRunsAppendOneLinePerRun() throws IOException {
        assertEquals(2, runPassingSession().lines().size(), "Each run should append one JSON line.");
    }

    @Test
    void passingRunLineContainsSeed() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"seed\":1"),
                "The JSON line should include the seed.");
    }

    @Test
    void passingRunLineContainsVerdict() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"verdict\":\"PASS\""),
                "The JSON line should include the passing verdict.");
    }

    @Test
    void passingRunLineContainsFilesystem() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"workDirFs\":\"ext4\""),
                "The JSON line should include the filesystem.");
    }

    @Test
    void passingRunLineContainsCaveat() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"caveat\":\"테스트\""),
                "The JSON line should include the caveat.");
    }

    @Test
    void passingRunLineContainsDurability() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"durability\""),
                "The JSON line should include durability.");
    }

    @Test
    void passingRunLineContainsP99() throws IOException {
        assertTrue(
                runPassingSession().lines().getFirst().contains("\"p99Ms\":5"),
                "The JSON line should include p99 latency.");
    }

    @Test
    void passingFirstRunRemovesWorkDirectory() throws IOException {
        runPassingSession();
        assertFalse(Files.exists(dir.resolve("work/1")), "The first work directory should be removed.");
    }

    @Test
    void passingSecondRunRemovesWorkDirectory() throws IOException {
        runPassingSession();
        assertFalse(Files.exists(dir.resolve("work/2")), "The second work directory should be removed.");
    }

    @Test
    void passingRunsDoNotCreateFailuresDirectory() throws IOException {
        runPassingSession();
        assertFalse(Files.exists(dir.resolve("failures")), "Passing runs should not create a failures directory.");
    }

    @Test
    @PolicyException(
            rule = "PMD.DoNotCallGarbageCollectionExplicitly",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.DoNotCallGarbageCollectionExplicitly")
    void passedRunsDoNotKeepTheirRecordsAliveForTheRestOfTheSessionCheck1() {
        final List<java.lang.ref.WeakReference<List<Published>>> firstRecords;
        final boolean[] collectedWhileSecondRuns;
        final Session s;
        firstRecords = new ArrayList<>();
        collectedWhileSecondRuns = new boolean[] {false};
        s = session(fake(p -> {
            final List<Published> published;
            if (p.seed() == 2) {
                for (int i = 0; i < 10 && !firstRecords.get(0).refersTo(null); i++) {
                    System.gc();
                }
                collectedWhileSecondRuns[0] = firstRecords.get(0).refersTo(null);
            }
            published = new ArrayList<>(
                    List.of(new Published(new Tag(1, 1), new Content(5, Map.of("bench-tag", "1/1")), 100, 0)));
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
        final var assertionValue1Argument2 = s.run(List.of(1L, 2L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_OK, assertionValue1Argument2, "Run should match the expected value.");
    }

    @Test
    @PolicyException(
            rule = "PMD.DoNotCallGarbageCollectionExplicitly",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.DoNotCallGarbageCollectionExplicitly")
    void passedRunsDoNotKeepTheirRecordsAliveForTheRestOfTheSessionCheck2() {
        final List<java.lang.ref.WeakReference<List<Published>>> firstRecords;
        final boolean[] collectedWhileSecondRuns;
        final Session s;
        firstRecords = new ArrayList<>();
        collectedWhileSecondRuns = new boolean[] {false};
        s = session(fake(p -> {
            final List<Published> published;
            if (p.seed() == 2) {
                for (int i = 0; i < 10 && !firstRecords.get(0).refersTo(null); i++) {
                    System.gc();
                }
                collectedWhileSecondRuns[0] = firstRecords.get(0).refersTo(null);
            }
            published = new ArrayList<>(
                    List.of(new Published(new Tag(1, 1), new Content(5, Map.of("bench-tag", "1/1")), 100, 0)));
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
        evaluate(s.run(List.of(1L, 2L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = collectedWhileSecondRuns[0];
        assertTrue(assertionValue2Argument1, "통과한 실행의 기록은 다음 실행이 도는 동안 메모리에서 내려가야 한다");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck1() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        final var assertionValue1Argument2 = s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck2() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = List.of(1L, 2L);
        assertEquals(assertionValue2Argument1, executed, "3번째 시드는 돌지 않는다");
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck3() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        final var assertionValue3Argument2 = Files.readString(failed.resolve("smq.db"));
        assertEquals("db", assertionValue3Argument2, "Read string should match the expected value.");
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck4() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        final var assertionValue4Argument1 = Files.exists(failed.resolve("smq.db-wal"));
        assertTrue(assertionValue4Argument1, "The path should exist.");
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck5() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        final var assertionValue5Argument1 =
                Files.readString(failed.resolve("published.jsonl")).contains("7/3");
        assertTrue(assertionValue5Argument1, "Read string should contain the expected value.");
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck6() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        final var assertionValue6Argument1 =
                Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain");
        assertTrue(assertionValue6Argument1, "Read string should contain the expected value.");
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck7() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        final var assertionValue7Argument1 = Files.exists(failed.resolve("deliveries.jsonl"));
        assertTrue(assertionValue7Argument1, "The path should exist.");
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck8() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        final var assertionValue8Argument1 =
                Files.readString(failed.resolve("violations.txt")).contains("LOSS");
        assertTrue(assertionValue8Argument1, "Read string should contain the expected value.");
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck9() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        final var assertionValue9Argument1 = Files.exists(dir.resolve("work/2"));
        assertFalse(assertionValue9Argument1, "실패한 파일은 옮겨졌다");
        evaluate(Files.exists(dir.resolve("work/1")));
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck10() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        final var assertionValue10Argument1 = Files.exists(dir.resolve("work/1"));
        assertFalse(assertionValue10Argument1, "통과한 파일은 지워졌다");
        evaluate(output());
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck11() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue11Argument1 = out.contains("seed=2");
        assertTrue(assertionValue11Argument1, "Out should contain the expected value.");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck12() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue12Argument1 = out.contains("LOSS");
        assertTrue(assertionValue12Argument1, "어긴 불변식");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck13() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue13Argument1 = out.contains("7/3");
        assertTrue(assertionValue13Argument1, "관련 태그");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck14() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue14Argument1 = out.contains("99");
        assertTrue(assertionValue14Argument1, "관련 메시지 id");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck15() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue15Argument1 = out.contains("leaseMillis");
        assertTrue(assertionValue15Argument1, "풀어 쓴 매개변수");
    }

    @Test
    void firstViolationStopsTheSessionAndPreservesFilesAndRecordsCheck16() throws IOException {
        final RunResult.Records records;
        final Session s;
        final Path failed;
        final String out;
        records = new RunResult.Records(
                List.of(new Published(new Tag(7, 3), new Content(5, Map.of("bench-tag", "7/3")), 100, 0)),
                List.of(),
                List.of(new CountsSample(QueueKind.TARGET, true, 1, 0, 0)));
        s = session(fake(p -> p.seed() == 2
                ? result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), records)
                : result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        failed = s.failureDir(2);
        evaluate(Files.readString(failed.resolve("smq.db")));
        evaluate(Files.exists(failed.resolve("smq.db-wal")));
        evaluate(Files.readString(failed.resolve("published.jsonl")).contains("7/3"));
        evaluate(Files.readString(failed.resolve("counts.jsonl")).contains("afterDrain"));
        evaluate(Files.exists(failed.resolve("deliveries.jsonl")));
        evaluate(Files.readString(failed.resolve("violations.txt")).contains("LOSS"));
        evaluate(Files.exists(dir.resolve("work/2")));
        evaluate(Files.exists(dir.resolve("work/1")));
        out = output();
        final var assertionValue16Argument1 = out.contains(failed.toString());
        assertTrue(assertionValue16Argument1, "Out should contain the expected value.");
    }

    @Test
    void rerunningTheSameFailedSeedPreservesTheFirstFailureCheck1() throws IOException {
        final Session s;
        final Path first;
        s = session(fake(p -> RunResult.error(p, new IllegalStateException("attempt-" + executed.size()))));
        final var assertionValue1Argument2 = s.run(List.of(9L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
        first = s.failureDir(9);
        evaluate(Files.readString(first.resolve("violations.txt")));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        evaluate(Files.readString(first.resolve("violations.txt")));
        evaluate(Files.exists(dir.resolve("failures/9/reruns/2/violations.txt")));
    }

    @Test
    void rerunningTheSameFailedSeedPreservesTheFirstFailureCheck2() throws IOException {
        final Session s;
        final Path first;
        s = session(fake(p -> RunResult.error(p, new IllegalStateException("attempt-" + executed.size()))));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        first = s.failureDir(9);
        evaluate(Files.readString(first.resolve("violations.txt")));
        final var assertionValue2Argument2 = s.run(List.of(9L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue2Argument2, "Run should match the expected value.");
        evaluate(Files.readString(first.resolve("violations.txt")));
        evaluate(Files.exists(dir.resolve("failures/9/reruns/2/violations.txt")));
    }

    @Test
    void rerunningTheSameFailedSeedPreservesTheFirstFailureCheck3() throws IOException {
        final Session s;
        final Path first;
        final String firstFailure;
        s = session(fake(p -> RunResult.error(p, new IllegalStateException("attempt-" + executed.size()))));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        first = s.failureDir(9);
        firstFailure = Files.readString(first.resolve("violations.txt"));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        final var assertionValue3Argument2 = Files.readString(first.resolve("violations.txt"));
        assertEquals(firstFailure, assertionValue3Argument2, "Read string should match the expected value.");
        evaluate(Files.exists(dir.resolve("failures/9/reruns/2/violations.txt")));
    }

    @Test
    void rerunningTheSameFailedSeedPreservesTheFirstFailureCheck4() throws IOException {
        final Session s;
        final Path first;
        s = session(fake(p -> RunResult.error(p, new IllegalStateException("attempt-" + executed.size()))));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        first = s.failureDir(9);
        evaluate(Files.readString(first.resolve("violations.txt")));
        evaluate(s.run(List.of(9L).iterator(), Long.MAX_VALUE));
        evaluate(Files.readString(first.resolve("violations.txt")));
        final var assertionValue4Argument1 = Files.exists(dir.resolve("failures/9/reruns/2/violations.txt"));
        assertTrue(assertionValue4Argument1, "The path should exist.");
    }

    @Test
    void rerunHintOmitsDurabilityWhenNotPinned() {
        final Session s;
        s = session(fake(p -> result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), RunResult.Records.EMPTY)));
        s.run(List.of(4L).iterator(), Long.MAX_VALUE);
        assertTrue(
                output().contains("randomBench --seed 4" + System.lineSeparator()),
                "Output should contain the expected value.");
    }

    @Test
    void rerunHintIncludesPinnedDurability() {
        final Session s;
        s = session(
                fake(p -> result(p, Verdict.VIOLATION, 10, null, List.of(LOSS), RunResult.Records.EMPTY)),
                LIMITS.withDurability(io.github.ppzxc.smq.Durability.PROCESS_CRASH));
        s.run(List.of(4L).iterator(), Long.MAX_VALUE);
        assertTrue(output().contains("randomBench --seed 4 --durability PROCESS_CRASH"), output());
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserveCheck1() {
        final Session s;
        final Session s2;
        s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        final var assertionValue1Argument2 = s.run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
        evaluate(Files.exists(s.failureDir(1).resolve("smq.db")));
        s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        evaluate(s2.run(List.of(5L).iterator(), Long.MAX_VALUE));
        evaluate(output().contains("harness broke"));
        evaluate(Files.exists(s2.failureDir(5)));
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserveCheck2() {
        final Session s;
        final Session s2;
        s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = Files.exists(s.failureDir(1).resolve("smq.db"));
        assertTrue(assertionValue2Argument1, "The path should exist.");
        s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        evaluate(s2.run(List.of(5L).iterator(), Long.MAX_VALUE));
        evaluate(output().contains("harness broke"));
        evaluate(Files.exists(s2.failureDir(5)));
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserveCheck3() {
        final Session s;
        final Session s2;
        s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        evaluate(Files.exists(s.failureDir(1).resolve("smq.db")));
        s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        final var assertionValue3Argument2 = s2.run(List.of(5L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue3Argument2, "Run should match the expected value.");
        evaluate(output().contains("harness broke"));
        evaluate(Files.exists(s2.failureDir(5)));
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserveCheck4() {
        final Session s;
        final Session s2;
        s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        evaluate(Files.exists(s.failureDir(1).resolve("smq.db")));
        s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        evaluate(s2.run(List.of(5L).iterator(), Long.MAX_VALUE));
        final var assertionValue4Argument1 = output().contains("harness broke");
        assertTrue(assertionValue4Argument1, "Output should contain the expected value.");
        evaluate(Files.exists(s2.failureDir(5)));
    }

    @Test
    void drainTimeoutAndErrorsAlsoFailAndPreserveCheck5() {
        final Session s;
        final Session s2;
        s = session(fake(p -> p.seed() == 1
                ? result(p, Verdict.DRAIN_TIMEOUT, 10, null, List.of(), RunResult.Records.EMPTY)
                : RunResult.error(p, new IllegalStateException("boom"))));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        evaluate(Files.exists(s.failureDir(1).resolve("smq.db")));
        s2 = session((p, d) -> {
            throw new IllegalStateException("harness broke");
        });
        evaluate(s2.run(List.of(5L).iterator(), Long.MAX_VALUE));
        evaluate(output().contains("harness broke"));
        final var assertionValue5Argument1 = Files.exists(s2.failureDir(5));
        assertTrue(assertionValue5Argument1, "Failure dir should exist.");
    }

    private int runWithZeroBudget() {
        final Session session;
        session = session(fake(p -> result(p, Verdict.PASS, 1, 1.0, List.of(), RunResult.Records.EMPTY)));
        return session.run(List.of(1L, 2L, 3L).iterator(), 0);
    }

    @Test
    void zeroBudgetRunExitsSuccessfully() {
        assertEquals(Session.EXIT_OK, runWithZeroBudget(), "The initial run should finish successfully.");
    }

    @Test
    void zeroBudgetStartsOnlyOneRun() {
        runWithZeroBudget();
        assertEquals(List.of(1L), executed, "The session should start only the first run.");
    }

    private String sortedSummaryOutput() {
        final Session session;
        session = session(fake(p -> result(
                p, Verdict.PASS, p.seed() * 100.0, p.seed() == 2 ? 900.0 : 5.0, List.of(), RunResult.Records.EMPTY)));
        session.run(List.of(3L, 1L, 2L).iterator(), Long.MAX_VALUE);
        return output();
    }

    @Test
    void summaryRanksFirstThroughputBeforeSecond() {
        final String output;
        final String throughput;
        output = sortedSummaryOutput();
        throughput = output.substring(output.indexOf("처리량이 낮은 순"), output.indexOf("p99가 높은 순"));
        assertTrue(
                throughput.indexOf("\n  1 ") < throughput.indexOf("\n  2 "),
                "The first throughput result should precede the second.");
    }

    @Test
    void summaryRanksSecondThroughputBeforeThird() {
        final String output;
        final String throughput;
        output = sortedSummaryOutput();
        throughput = output.substring(output.indexOf("처리량이 낮은 순"), output.indexOf("p99가 높은 순"));
        assertTrue(
                throughput.indexOf("\n  2 ") < throughput.indexOf("\n  3 "),
                "The second throughput result should precede the third.");
    }

    @Test
    void summaryRanksHighestP99First() {
        final String output;
        final String p99;
        output = sortedSummaryOutput();
        p99 = output.substring(output.indexOf("p99가 높은 순"));
        assertTrue(p99.indexOf("\n  2 ") < p99.indexOf("\n  1 "), "The highest p99 should appear first.");
    }

    private static final ErrorDetail CHILD_OOM = new ErrorDetail(
            "child-2",
            "java.lang.OutOfMemoryError",
            "Java heap space",
            "java.lang.OutOfMemoryError: Java heap space\n\tat a.B.c(B.java:1)\n");

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck1() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        final var assertionValue1Argument2 = s.run(List.of(8L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck2() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        final var assertionValue2Argument2 = line.get("verdict");
        assertEquals("ERROR", assertionValue2Argument2, "Line should match the expected value.");
        evaluate((Map<?, ?>) line.get("errorDetail"));
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck3() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        final var assertionValue3Argument2 = line.get("error");
        assertEquals("boom", assertionValue3Argument2, "기존 필드 의미는 그대로");
        evaluate((Map<?, ?>) line.get("errorDetail"));
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck4() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final Map<?, ?> detail;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        detail = (Map<?, ?>) line.get("errorDetail");
        final var assertionValue4Argument2 = detail.get("location");
        assertEquals("child-2", assertionValue4Argument2, "Detail should match the expected value.");
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck5() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final Map<?, ?> detail;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        detail = (Map<?, ?>) line.get("errorDetail");
        final var assertionValue5Argument2 = detail.get("exceptionClass");
        assertEquals("java.lang.OutOfMemoryError", assertionValue5Argument2, "Detail should match the expected value.");
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck6() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final Map<?, ?> detail;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        detail = (Map<?, ?>) line.get("errorDetail");
        final var assertionValue6Argument2 = detail.get("message");
        assertEquals("Java heap space", assertionValue6Argument2, "Detail should match the expected value.");
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck7() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final Map<?, ?> detail;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        detail = (Map<?, ?>) line.get("errorDetail");
        final var assertionValue7Argument1 = ((String) detail.get("stackTrace")).contains("at a.B.c");
        assertTrue(assertionValue7Argument1, "Detail should contain the expected value.");
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck8() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        final var assertionValue8Argument2 = line.get("walMaxBytes");
        assertEquals(852_000_000.0, assertionValue8Argument2, "Line should match the expected value.");
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck9() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String txt;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        txt = Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n");
        final var assertionValue9Argument1 = txt.startsWith("seed=8\nverdict=ERROR\nerror=boom\n");
        assertTrue(assertionValue9Argument1, "앞쪽 줄은 그대로");
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck10() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String txt;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        txt = Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n");
        final var assertionValue10Argument1 = txt.contains("errorLocation=child-2");
        assertTrue(assertionValue10Argument1, "Txt should contain the expected value.");
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck11() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String txt;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        txt = Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n");
        final var assertionValue11Argument1 = txt.contains("\tat a.B.c(B.java:1)");
        assertTrue(assertionValue11Argument1, "스택 트레이스 전체");
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck12() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String txt;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        txt = Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n");
        final var assertionValue12Argument1 = txt.contains("walMaxBytes=852000000");
        assertTrue(assertionValue12Argument1, "Txt should contain the expected value.");
        evaluate(output());
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck13() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String out;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        out = output();
        final var assertionValue13Argument1 = out.contains("child-2");
        assertTrue(assertionValue13Argument1, "Out should contain the expected value.");
    }

    @Test
    void anErrorKeepsItsLocationStackTraceAndWalSizeInTheJsonLineAndTheViolationsFileCheck14() throws IOException {
        final Session s;
        final Map<?, ?> line;
        final String out;
        s = session(fake(p -> RunResult.error(p, CHILD_OOM, "boom", 852_000_000L)));
        evaluate(s.run(List.of(8L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        evaluate((Map<?, ?>) line.get("errorDetail"));
        evaluate(Files.readString(s.failureDir(8).resolve("violations.txt")).replace("\r\n", "\n"));
        out = output();
        final var assertionValue14Argument1 = out.contains("at a.B.c");
        assertTrue(assertionValue14Argument1, "Out should contain the expected value.");
    }

    @Test
    void passingRunsRecordTheWalSizeAndNoErrorDetailCheck1() throws IOException {
        final Session s;
        s = session(fake(p -> new RunResult(
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
                null)));
        final var assertionValue1Argument2 = s.run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_OK, assertionValue1Argument2, "Run should match the expected value.");
        evaluate((Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst()));
    }

    @Test
    void passingRunsRecordTheWalSizeAndNoErrorDetailCheck2() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> new RunResult(
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
                null)));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        final var assertionValue2Argument2 = line.get("walMaxBytes");
        assertEquals(4096.0, assertionValue2Argument2, "Line should match the expected value.");
    }

    @Test
    void passingRunsRecordTheWalSizeAndNoErrorDetailCheck3() throws IOException {
        final Session s;
        final Map<?, ?> line;
        s = session(fake(p -> new RunResult(
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
                null)));
        evaluate(s.run(List.of(1L).iterator(), Long.MAX_VALUE));
        line = (Map<?, ?>) io.github.ppzxc.smq.benchmark.Json.parse(
                Files.readAllLines(dir.resolve("runs.jsonl")).getFirst());
        final var assertionValue3Argument1 = line.get("errorDetail");
        assertNull(assertionValue3Argument1, "Line should be null.");
    }

    private Session soakSession(final Session.Executor executor) {
        return new Session(
                dir,
                dir.resolve("runs.jsonl"),
                Map.of("workDirFs", "ext4", "caveat", "테스트"),
                LIMITS.withRate(500),
                executor,
                new PrintStream(bytes, true, StandardCharsets.UTF_8));
    }

    private static SoakJudge.Report report(final boolean growing) {
        return new SoakJudge.Report(500, 0.93, 460, 12.5, 800, growing);
    }

    private static RunResult soakResult(final RunParams p, final Verdict v, final SoakJudge.Report soak) {
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
                new JvmMetricsSampler.Summary(1, 2, 3, 4, 5, 6, 7, 8, 0.7, 0.35, 9),
                soak);
    }

    private record PassingSoak(int exitCode, String json, String output) {}

    private PassingSoak runPassingSoak() throws IOException {
        final Session session;
        final int exitCode;
        session = soakSession(fake(p -> soakResult(p, Verdict.PASS, report(false))));
        exitCode = session.run(List.of(1L).iterator(), Long.MAX_VALUE);
        return new PassingSoak(exitCode, Files.readString(dir.resolve("runs.jsonl")), output());
    }

    @Test
    void passingSoakExitsSuccessfully() throws IOException {
        assertEquals(Session.EXIT_OK, runPassingSoak().exitCode(), "A passing soak should exit successfully.");
    }

    @Test
    void passingSoakJsonIncludesTargetRate() throws IOException {
        assertTrue(
                runPassingSoak().json().contains("\"soak\":{\"targetRate\":500"),
                "The JSON should include the target rate.");
    }

    @Test
    void passingSoakJsonIncludesAchievement() throws IOException {
        assertTrue(
                runPassingSoak().json().contains("\"achievedRatio\":0.93"),
                "The JSON should include the achievement ratio.");
    }

    @Test
    void passingSoakJsonIncludesCpuLoad() throws IOException {
        assertTrue(runPassingSoak().json().contains("\"meanCpuLoad\":0.35"), "The JSON should include mean CPU load.");
    }

    @Test
    void passingSoakOutputIncludesAchievement() throws IOException {
        assertTrue(runPassingSoak().output().contains("달성률 93%"), "The output should include the achievement ratio.");
    }

    @Test
    @DisplayName("고정 속도가 아니면 soak 필드가 null이다")
    void ifItIsNotAFixedSpeedTheSoakFieldIsNull() throws IOException {
        final Session s;
        final String line;
        s = session(fake(p -> result(p, Verdict.PASS, 1000, 5.0, List.of(), RunResult.Records.EMPTY)));
        s.run(List.of(1L).iterator(), Long.MAX_VALUE);
        line = Files.readString(dir.resolve("runs.jsonl"));
        assertTrue(line.contains("\"soak\":null"), line);
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Removes Database")
    void passedSoakRemovesDatabase() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertFalse(Files.exists(dir.resolve("work/1/smq.db")), "Resolve should not exist.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Removes Wal")
    void passedSoakRemovesWal() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertFalse(Files.exists(dir.resolve("work/1/smq.db-wal")), "Resolve should not exist.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Retains Jfr")
    void passedSoakRetainsJfr() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertEquals(
                "jfr",
                Files.readString(dir.resolve("work/1/parent.jfr")),
                "Read string should match the expected value.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Retains Metrics")
    void passedSoakRetainsMetrics() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertTrue(Files.exists(dir.resolve("work/1/jvm-metrics.jsonl")), "The path should exist.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Removes Published Records")
    void passedSoakRemovesPublishedRecords() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertFalse(Files.exists(dir.resolve("work/1/published.jsonl")), "큰 기록 파일도 지운다");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidThrowingRawExceptionTypes",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    @DisplayName("고정 속도 실행은 통과해도 JFR과 메트릭 파일을 남기고 DB와 큰 기록만 지운다 — passed Soak Removes Delivery Records")
    void passedSoakRemovesDeliveryRecords() throws IOException {
        final Session.Executor exec;
        exec = (params, runDir) -> {
            try {
                Files.writeString(runDir.resolve("smq.db"), "db");
                Files.writeString(runDir.resolve("smq.db-wal"), "wal");
                Files.writeString(runDir.resolve("parent.jfr"), "jfr");
                Files.writeString(runDir.resolve("jvm-metrics.jsonl"), "{}");
                Files.writeString(runDir.resolve("published.jsonl"), "{}");
                Files.writeString(runDir.resolve("deliveries.jsonl"), "{}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return soakResult(params, Verdict.PASS, report(false));
        };
        soakSession(exec).run(List.of(1L).iterator(), Long.MAX_VALUE);
        assertFalse(Files.exists(dir.resolve("work/1/deliveries.jsonl")), "Resolve should not exist.");
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 1")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck1()
            throws IOException {
        final Session s;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        final var assertionValue1Argument2 = s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
        evaluate(output());
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 2")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck2()
            throws IOException {
        final Session s;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = List.of(1L, 2L, 3L);
        assertEquals(assertionValue2Argument1, executed, "용량 문제는 다음 시드를 막지 않는다");
        evaluate(output());
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 3")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck3()
            throws IOException {
        final Session s;
        final String out;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        out = output();
        final var assertionValue3Argument1 = out.contains("BACKLOG_GROWING");
        assertTrue(assertionValue3Argument1, out);
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 4")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck4()
            throws IOException {
        final Session s;
        final String out;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        out = output();
        final var assertionValue4Argument1 = out.contains("백로그");
        assertTrue(assertionValue4Argument1, out);
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 5")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck5()
            throws IOException {
        final Session s;
        final String out;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        out = output();
        final var assertionValue5Argument1 = out.contains("합계: 3회 실행, 실패");
        assertTrue(assertionValue5Argument1, out);
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 6")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck6()
            throws IOException {
        final Session s;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        evaluate(output());
        final var assertionValue6Argument1 = Files.exists(s.failureDir(2).resolve("smq.db"));
        assertTrue(assertionValue6Argument1, "실패한 실행은 DB까지 보존한다");
        evaluate(Files.readAllLines(dir.resolve("runs.jsonl")).size());
    }

    @Test
    @DisplayName("백로그 증가는 기록하고 계속 돌며 끝에서 실패로 종료한다 — check 7")
    void anIncreaseInTheBacklogIsRecordedAndTheProcessContinuesToRunUltimatelyEndingInFailureCheck7()
            throws IOException {
        final Session s;
        s = soakSession(fake(
                p -> soakResult(p, p.seed() == 2 ? Verdict.BACKLOG_GROWING : Verdict.PASS, report(p.seed() == 2))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        evaluate(output());
        evaluate(Files.exists(s.failureDir(2).resolve("smq.db")));
        final var assertionValue7Argument2 =
                Files.readAllLines(dir.resolve("runs.jsonl")).size();
        assertEquals(3, assertionValue7Argument2, "Size should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서도 불변식 위반은 바로 멈춘다 — check 1")
    void evenAtAFixedSpeedAnInvariantViolationCausesAnImmediateStopCheck1() {
        final Session s;
        s = soakSession(fake(p -> soakResult(p, p.seed() == 2 ? Verdict.VIOLATION : Verdict.PASS, report(false))));
        final var assertionValue1Argument2 = s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서도 불변식 위반은 바로 멈춘다 — check 2")
    void evenAtAFixedSpeedAnInvariantViolationCausesAnImmediateStopCheck2() {
        final Session s;
        s = soakSession(fake(p -> soakResult(p, p.seed() == 2 ? Verdict.VIOLATION : Verdict.PASS, report(false))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = List.of(1L, 2L);
        assertEquals(assertionValue2Argument1, executed, "Executed should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서는 예외도 기록하고 계속 돌되 연속 3번이면 멈춘다 — check 1")
    void
            atAFixedSpeedExceptionsAreAlsoRecordedAndTheProcessContinuesToRunButItStopsAfter3ConsecutiveOccurrencesCheck1() {
        final Session s;
        s = soakSession(fake(p -> p.seed() == 2 || p.seed() >= 4
                ? soakResult(p, Verdict.ERROR, null)
                : soakResult(p, Verdict.PASS, report(false))));
        final var assertionValue1Argument2 =
                s.run(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서는 예외도 기록하고 계속 돌되 연속 3번이면 멈춘다 — check 2")
    void
            atAFixedSpeedExceptionsAreAlsoRecordedAndTheProcessContinuesToRunButItStopsAfter3ConsecutiveOccurrencesCheck2() {
        final Session s;
        s = soakSession(fake(p -> p.seed() == 2 || p.seed() >= 4
                ? soakResult(p, Verdict.ERROR, null)
                : soakResult(p, Verdict.PASS, report(false))));
        evaluate(s.run(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = List.of(1L, 2L, 3L, 4L, 5L, 6L);
        assertEquals(assertionValue2Argument1, executed, "Executed should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서도 소진 실패는 바로 멈춘다 — check 1")
    void evenAtAFixedSpeedAnExhaustionFailureStopsImmediatelyCheck1() {
        final Session s;
        s = soakSession(fake(p -> soakResult(p, p.seed() == 2 ? Verdict.DRAIN_TIMEOUT : Verdict.PASS, report(false))));
        final var assertionValue1Argument2 = s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE);
        assertEquals(Session.EXIT_FAILED, assertionValue1Argument2, "Run should match the expected value.");
    }

    @Test
    @DisplayName("고정 속도에서도 소진 실패는 바로 멈춘다 — check 2")
    void evenAtAFixedSpeedAnExhaustionFailureStopsImmediatelyCheck2() {
        final Session s;
        s = soakSession(fake(p -> soakResult(p, p.seed() == 2 ? Verdict.DRAIN_TIMEOUT : Verdict.PASS, report(false))));
        evaluate(s.run(List.of(1L, 2L, 3L).iterator(), Long.MAX_VALUE));
        final var assertionValue2Argument1 = List.of(1L, 2L);
        assertEquals(assertionValue2Argument1, executed, "Executed should match the expected value.");
    }

    private static Object evaluate(final Object value) {
        return value;
    }
}
