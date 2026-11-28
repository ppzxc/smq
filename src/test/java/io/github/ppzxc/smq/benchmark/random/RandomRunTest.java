package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RandomRunTest {

    @TempDir
    Path dir;

    @Test
    void shortRunPassesAndRecordsPublishedAndDeliveredCheck1() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        final var assertionValue1Argument2 = r.verdict();
        final var assertionValue1Argument3 = r.violations() + " " + r.error();
        assertEquals(Verdict.PASS, assertionValue1Argument2, assertionValue1Argument3);
        evaluate(r.records().published().isEmpty());
        evaluate(r.records().deliveries().isEmpty());
        evaluate(Files.exists(dir.resolve("smq.db")));
        evaluate(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void shortRunPassesAndRecordsPublishedAndDeliveredCheck2() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        final var assertionValue2Argument1 = r.records().published().isEmpty();
        assertFalse(assertionValue2Argument1, "Published should not be empty.");
        evaluate(r.records().deliveries().isEmpty());
        evaluate(Files.exists(dir.resolve("smq.db")));
        evaluate(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void shortRunPassesAndRecordsPublishedAndDeliveredCheck3() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        evaluate(r.records().published().isEmpty());
        final var assertionValue3Argument1 = r.records().deliveries().isEmpty();
        assertFalse(assertionValue3Argument1, "Deliveries should not be empty.");
        evaluate(Files.exists(dir.resolve("smq.db")));
        evaluate(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void shortRunPassesAndRecordsPublishedAndDeliveredCheck4() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        evaluate(r.records().published().isEmpty());
        evaluate(r.records().deliveries().isEmpty());
        final var assertionValue4Argument1 = Files.exists(dir.resolve("smq.db"));
        assertTrue(assertionValue4Argument1, "The path should exist.");
        evaluate(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void shortRunPassesAndRecordsPublishedAndDeliveredCheck5() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        evaluate(r.records().published().isEmpty());
        evaluate(r.records().deliveries().isEmpty());
        evaluate(Files.exists(dir.resolve("smq.db")));
        final var assertionValue5Argument1 = r.records().samples().stream().anyMatch(CountsSample::afterDrain);
        assertTrue(assertionValue5Argument1, "A sample should be recorded after draining.");
    }

    @Test
    void checkerViolationSetsVerdict() {
        final RunParams p;
        final Violation v;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> List.of(v));
        assertEquals(Verdict.VIOLATION, r.verdict(), "Verdict should match the expected value.");
    }

    @Test
    void checkerViolationIsPreserved() {
        final RunParams p;
        final Violation v;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> List.of(v));
        assertEquals(List.of(v), r.violations(), "Violations should match the expected value.");
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarnessCheck1() {
        final RunParams p;
        final RunResult r;
        final ErrorDetail d;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        final var assertionValue1Argument2 = r.verdict();
        assertEquals(Verdict.ERROR, assertionValue1Argument2, "Verdict should match the expected value.");
        d = r.errorDetail();
        evaluate(d.location());
        evaluate(d.exceptionClass());
        evaluate(d.message());
        evaluate(d.stackTrace().contains("\tat "));
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarnessCheck2() {
        final RunParams p;
        final RunResult r;
        final ErrorDetail d;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        d = r.errorDetail();
        final var assertionValue2Argument2 = d.location();
        assertEquals("harness", assertionValue2Argument2, "Location should match the expected value.");
        evaluate(d.exceptionClass());
        evaluate(d.message());
        evaluate(d.stackTrace().contains("\tat "));
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarnessCheck3() {
        final RunParams p;
        final RunResult r;
        final ErrorDetail d;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        d = r.errorDetail();
        evaluate(d.location());
        final var assertionValue3Argument2 = d.exceptionClass();
        assertEquals(
                "java.lang.IllegalArgumentException",
                assertionValue3Argument2,
                "Exception class should match the expected value.");
        evaluate(d.message());
        evaluate(d.stackTrace().contains("\tat "));
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarnessCheck4() {
        final RunParams p;
        final RunResult r;
        final ErrorDetail d;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        d = r.errorDetail();
        evaluate(d.location());
        evaluate(d.exceptionClass());
        final var assertionValue4Argument2 = d.message();
        assertEquals("검사기가 터졌다", assertionValue4Argument2, "Message should match the expected value.");
        evaluate(d.stackTrace().contains("\tat "));
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarnessCheck5() {
        final RunParams p;
        final RunResult r;
        final ErrorDetail d;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        d = r.errorDetail();
        evaluate(d.location());
        evaluate(d.exceptionClass());
        evaluate(d.message());
        final var assertionValue5Argument1 = d.stackTrace().contains("\tat ");
        assertTrue(assertionValue5Argument1, "Stack trace should contain the expected value.");
    }

    @Test
    void everyRunRecordsTheLargestWalSizeSeenCheck1() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        final var assertionValue1Argument2 = r.verdict();
        final var assertionValue1Argument3 = r.violations() + " " + r.error();
        assertEquals(Verdict.PASS, assertionValue1Argument2, assertionValue1Argument3);
        evaluate(r.walMaxBytes());
        evaluate(r.walMaxBytes() > 0);
        evaluate("실행 중 WAL이 있었다: " + r.walMaxBytes());
    }

    @Test
    void everyRunRecordsTheLargestWalSizeSeenCheck2() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        final var assertionValue2Argument1 = r.walMaxBytes();
        assertNotNull(assertionValue2Argument1, "Wal max bytes should be present.");
        evaluate(r.walMaxBytes() > 0);
        evaluate("실행 중 WAL이 있었다: " + r.walMaxBytes());
    }

    @Test
    void everyRunRecordsTheLargestWalSizeSeenCheck3() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(400));
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error());
        evaluate(r.walMaxBytes());
        final var assertionValue3Argument1 = r.walMaxBytes() > 0;
        final var assertionValue3Argument2 = "실행 중 WAL이 있었다: " + r.walMaxBytes();
        assertTrue(assertionValue3Argument1, assertionValue3Argument2);
    }

    @Test
    void anErrorResultAlsoCarriesTheWalSizeCheck1() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalStateException("검사기");
        });
        final var assertionValue1Argument2 = r.verdict();
        assertEquals(Verdict.ERROR, assertionValue1Argument2, "Verdict should match the expected value.");
        evaluate(r.walMaxBytes());
    }

    @Test
    void anErrorResultAlsoCarriesTheWalSizeCheck2() {
        final RunParams p;
        final RunResult r;
        p = RunParams.fromSeed(5, Limits.smoke(300));
        r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalStateException("검사기");
        });
        final var assertionValue2Argument1 = r.walMaxBytes();
        assertNotNull(assertionValue2Argument1, "Wal max bytes should be present.");
    }

    @Test
    @DisplayName("고정 속도로 돌면 달성률과 백로그 보고가 붙는다 — check 1")
    void whenRunningAtAFixedSpeedTheAchievementRateAndBacklogReportAreAttachedCheck1() {
        final Limits soak;
        final RunParams p;
        final RunResult r;
        final RunResult plain;
        soak = Limits.smoke(2_000).withRate(200);
        p = RunParams.fromSeed(1, soak);
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        final var assertionValue1Argument2 = r.verdict();
        final var assertionValue1Argument3 = r.violations() + " " + r.error() + " " + r.soak();
        assertEquals(Verdict.PASS, assertionValue1Argument2, assertionValue1Argument3);
        evaluate(r.soak());
        evaluate(r.soak().targetRate());
        evaluate(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5);
        evaluate("달성률: " + r.soak());
        plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::findViolations);
        evaluate(plain.soak());
    }

    @Test
    @DisplayName("고정 속도로 돌면 달성률과 백로그 보고가 붙는다 — check 2")
    void whenRunningAtAFixedSpeedTheAchievementRateAndBacklogReportAreAttachedCheck2() {
        final Limits soak;
        final RunParams p;
        final RunResult r;
        final RunResult plain;
        soak = Limits.smoke(2_000).withRate(200);
        p = RunParams.fromSeed(1, soak);
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error() + " " + r.soak());
        final var assertionValue2Argument1 = r.soak();
        assertNotNull(assertionValue2Argument1, "Soak should be present.");
        evaluate(r.soak().targetRate());
        evaluate(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5);
        evaluate("달성률: " + r.soak());
        plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::findViolations);
        evaluate(plain.soak());
    }

    @Test
    @DisplayName("고정 속도로 돌면 달성률과 백로그 보고가 붙는다 — check 3")
    void whenRunningAtAFixedSpeedTheAchievementRateAndBacklogReportAreAttachedCheck3() {
        final Limits soak;
        final RunParams p;
        final RunResult r;
        final RunResult plain;
        soak = Limits.smoke(2_000).withRate(200);
        p = RunParams.fromSeed(1, soak);
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error() + " " + r.soak());
        evaluate(r.soak());
        final var assertionValue3Argument2 = r.soak().targetRate();
        assertEquals(200, assertionValue3Argument2, "Target rate should match the expected value.");
        evaluate(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5);
        evaluate("달성률: " + r.soak());
        plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::findViolations);
        evaluate(plain.soak());
    }

    @Test
    @DisplayName("고정 속도로 돌면 달성률과 백로그 보고가 붙는다 — check 4")
    void whenRunningAtAFixedSpeedTheAchievementRateAndBacklogReportAreAttachedCheck4() {
        final Limits soak;
        final RunParams p;
        final RunResult r;
        final RunResult plain;
        soak = Limits.smoke(2_000).withRate(200);
        p = RunParams.fromSeed(1, soak);
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error() + " " + r.soak());
        evaluate(r.soak());
        evaluate(r.soak().targetRate());
        final var assertionValue4Argument1 =
                r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5;
        final var assertionValue4Argument2 = "달성률: " + r.soak();
        assertTrue(assertionValue4Argument1, assertionValue4Argument2);
        plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::findViolations);
        evaluate(plain.soak());
    }

    @Test
    @DisplayName("고정 속도로 돌면 달성률과 백로그 보고가 붙는다 — check 5")
    void whenRunningAtAFixedSpeedTheAchievementRateAndBacklogReportAreAttachedCheck5() {
        final Limits soak;
        final RunParams p;
        final RunResult r;
        final RunResult plain;
        soak = Limits.smoke(2_000).withRate(200);
        p = RunParams.fromSeed(1, soak);
        r = RandomRun.execute(p, dir, InvariantChecker::findViolations);
        evaluate(r.violations() + " " + r.error() + " " + r.soak());
        evaluate(r.soak());
        evaluate(r.soak().targetRate());
        evaluate(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5);
        evaluate("달성률: " + r.soak());
        plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::findViolations);
        final var assertionValue5Argument1 = plain.soak();
        assertNull(assertionValue5Argument1, "Soak should be null.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 1")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck1() {
        final SoakJudge.Report growing;
        growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        evaluate(new SoakJudge.Report(500, 0.3, 100, 0, 10, false));
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue1Argument2 = RandomRun.verdict(true, List.of(), growing, 0);
        assertEquals(Verdict.BACKLOG_GROWING, assertionValue1Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 2")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck2() {
        final SoakJudge.Report fine;
        evaluate(new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true));
        fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue2Argument2 = RandomRun.verdict(true, List.of(), fine, 0);
        assertEquals(Verdict.PASS, assertionValue2Argument2, "달성률이 낮아도 보고만 한다");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 3")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck3() {
        evaluate(new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true));
        evaluate(new SoakJudge.Report(500, 0.3, 100, 0, 10, false));
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue3Argument2 = RandomRun.verdict(true, List.of(), null, 0.95);
        assertEquals(Verdict.PASS, assertionValue3Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 4")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck4() {
        final SoakJudge.Report growing;
        final Violation v;
        growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        evaluate(new SoakJudge.Report(500, 0.3, 100, 0, 10, false));
        v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        final var assertionValue4Argument2 = RandomRun.verdict(true, List.of(v), growing, 0);
        assertEquals(Verdict.VIOLATION, assertionValue4Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 5")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck5() {
        final SoakJudge.Report growing;
        final Violation v;
        growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        evaluate(new SoakJudge.Report(500, 0.3, 100, 0, 10, false));
        v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        final var assertionValue5Argument2 = RandomRun.verdict(false, List.of(v), growing, 0);
        assertEquals(Verdict.DRAIN_TIMEOUT, assertionValue5Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 6")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck6() {
        final SoakJudge.Report fine;
        evaluate(new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true));
        fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue6Argument2 = RandomRun.verdict(true, List.of(), fine, 0.95);
        assertEquals(Verdict.UNDER_RATE, assertionValue6Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 7")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck7() {
        final SoakJudge.Report fine;
        evaluate(new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true));
        fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue7Argument2 = RandomRun.verdict(true, List.of(), fine, 0.3);
        assertEquals(Verdict.PASS, assertionValue7Argument2, "기준과 같으면 통과다");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 8")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck8() {
        final SoakJudge.Report growing;
        growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        evaluate(new SoakJudge.Report(500, 0.3, 100, 0, 10, false));
        evaluate(new Violation(Invariant.GHOST, List.of(), List.of(), "합성"));
        final var assertionValue8Argument2 = RandomRun.verdict(true, List.of(), growing, 1.0);
        assertEquals(Verdict.BACKLOG_GROWING, assertionValue8Argument2, "Verdict should match the expected value.");
    }

    @Test
    @DisplayName("백로그 증가는 불변식이 멀쩡해도 실패이고 위반과 소진 실패가 우선한다 — check 9")
    void
            anIncreaseInTheBacklogIsAFailureEvenIfTheInvariantIsIntactAndViolationsAndExhaustionFailuresTakePrecedenceCheck9() {
        final SoakJudge.Report fine;
        final Violation v;
        evaluate(new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true));
        fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        final var assertionValue9Argument2 = RandomRun.verdict(true, List.of(v), fine, 0.95);
        assertEquals(Verdict.VIOLATION, assertionValue9Argument2, "Verdict should match the expected value.");
    }

    private Path subdir(final String name) {
        try {
            return Files.createDirectories(dir.resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long lateLoadMedianDepth(final RunResult r) {
        final List<Long> depths;
        final List<Long> late;
        final List<Long> sorted;
        depths = new ArrayList<>();
        for (final CountsSample c : r.records().samples()) {
            if (c.queue() == QueueKind.TARGET && !c.afterDrain()) {
                depths.add(c.ready() + c.leased() + c.delayed());
            }
        }
        late = depths.subList(depths.size() / 2, depths.size());
        sorted = late.stream().sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    @Test
    @DisplayName("깊은 큐 모드는 깊이가 목표 이하면 컨슈머가 쉬어서 미리 채운 깊이가 유지된다 — check 1")
    void inDeepQueueModeIfTheDepthIsBelowTheTargetTheConsumerPausesAndThePreFilledDepthIsMaintainedCheck1() {
        final Limits standingLimits;
        final RunParams standing;
        final RunResult kept;
        final Limits drainingLimits;
        final RunParams draining;
        final RunResult drained;
        standingLimits = Limits.smoke(2_500).withRate(200).withDepth(20_000).withStandingQueue(true);
        standing = RunParams.fromSeed(1, standingLimits);
        kept = RandomRun.execute(standing, subdir("standing"), InvariantChecker::findViolations);
        final var assertionValue1Argument2 = kept.verdict();
        final var assertionValue1Argument3 = kept.violations() + " " + kept.error();
        assertEquals(Verdict.PASS, assertionValue1Argument2, assertionValue1Argument3);
        evaluate(lateLoadMedianDepth(kept) > standing.prefillDepth() * 0.5);
        evaluate("깊이가 유지돼야 한다: " + lateLoadMedianDepth(kept) + " / " + standing.prefillDepth());
        drainingLimits = Limits.smoke(2_500).withRate(200);
        draining = RunParams.fromSeed(1, drainingLimits);
        drained = RandomRun.execute(draining, subdir("draining"), InvariantChecker::findViolations);
        evaluate(lateLoadMedianDepth(drained) < draining.prefillDepth() * 0.5);
        evaluate("조절하지 않으면 컨슈머가 깊이를 비운다(대조군): " + lateLoadMedianDepth(drained));
    }

    @Test
    @DisplayName("깊은 큐 모드는 깊이가 목표 이하면 컨슈머가 쉬어서 미리 채운 깊이가 유지된다 — check 2")
    void inDeepQueueModeIfTheDepthIsBelowTheTargetTheConsumerPausesAndThePreFilledDepthIsMaintainedCheck2() {
        final Limits standingLimits;
        final RunParams standing;
        final RunResult kept;
        final Limits drainingLimits;
        final RunParams draining;
        final RunResult drained;
        standingLimits = Limits.smoke(2_500).withRate(200).withDepth(20_000).withStandingQueue(true);
        standing = RunParams.fromSeed(1, standingLimits);
        kept = RandomRun.execute(standing, subdir("standing"), InvariantChecker::findViolations);
        evaluate(kept.violations() + " " + kept.error());
        final var assertionValue2Argument1 = lateLoadMedianDepth(kept) > standing.prefillDepth() * 0.5;
        final var assertionValue2Argument2 =
                "깊이가 유지돼야 한다: " + lateLoadMedianDepth(kept) + " / " + standing.prefillDepth();
        assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        drainingLimits = Limits.smoke(2_500).withRate(200);
        draining = RunParams.fromSeed(1, drainingLimits);
        drained = RandomRun.execute(draining, subdir("draining"), InvariantChecker::findViolations);
        evaluate(lateLoadMedianDepth(drained) < draining.prefillDepth() * 0.5);
        evaluate("조절하지 않으면 컨슈머가 깊이를 비운다(대조군): " + lateLoadMedianDepth(drained));
    }

    @Test
    @DisplayName("깊은 큐 모드는 깊이가 목표 이하면 컨슈머가 쉬어서 미리 채운 깊이가 유지된다 — check 3")
    void inDeepQueueModeIfTheDepthIsBelowTheTargetTheConsumerPausesAndThePreFilledDepthIsMaintainedCheck3() {
        final Limits standingLimits;
        final RunParams standing;
        final RunResult kept;
        final Limits drainingLimits;
        final RunParams draining;
        final RunResult drained;
        standingLimits = Limits.smoke(2_500).withRate(200).withDepth(20_000).withStandingQueue(true);
        standing = RunParams.fromSeed(1, standingLimits);
        kept = RandomRun.execute(standing, subdir("standing"), InvariantChecker::findViolations);
        evaluate(kept.violations() + " " + kept.error());
        evaluate(lateLoadMedianDepth(kept) > standing.prefillDepth() * 0.5);
        evaluate("깊이가 유지돼야 한다: " + lateLoadMedianDepth(kept) + " / " + standing.prefillDepth());
        drainingLimits = Limits.smoke(2_500).withRate(200);
        draining = RunParams.fromSeed(1, drainingLimits);
        drained = RandomRun.execute(draining, subdir("draining"), InvariantChecker::findViolations);
        final var assertionValue3Argument1 = lateLoadMedianDepth(drained) < draining.prefillDepth() * 0.5;
        final var assertionValue3Argument2 = "조절하지 않으면 컨슈머가 깊이를 비운다(대조군): " + lateLoadMedianDepth(drained);
        assertTrue(assertionValue3Argument1, assertionValue3Argument2);
    }

    @Test
    @DisplayName("깊은 큐 모드의 소진 한도는 부하 끝에 남아 있을 깊이만큼 늘어난다 — check 1")
    void theExhaustionLimitInDeepQueueModeIncreasesByTheDepthRemainingAtTheEndOfTheLoadCheck1() {
        final Limits base;
        final RunParams plain;
        final RunParams standing;
        base = Limits.full(10_000).withRate(500);
        plain = RunParams.fromSeed(7, base);
        standing = RunParams.fromSeed(7, base.withStandingQueue(true));
        final var assertionValue1Argument1 = plain.leaseMillis();
        final var assertionValue1Argument2 = standing.leaseMillis();
        assertEquals(assertionValue1Argument1, assertionValue1Argument2, "같은 시드라 리스 기한은 같다");
        evaluate(standing.prefillDepth() / 2);
        evaluate(RandomRun.drainLimit(standing) - RandomRun.drainLimit(plain));
        evaluate(RandomRun.drainLimit(standing) > RandomRun.drainLimit(plain));
    }

    @Test
    @DisplayName("깊은 큐 모드의 소진 한도는 부하 끝에 남아 있을 깊이만큼 늘어난다 — check 2")
    void theExhaustionLimitInDeepQueueModeIncreasesByTheDepthRemainingAtTheEndOfTheLoadCheck2() {
        final Limits base;
        final RunParams plain;
        final RunParams standing;
        base = Limits.full(10_000).withRate(500);
        plain = RunParams.fromSeed(7, base);
        standing = RunParams.fromSeed(7, base.withStandingQueue(true));
        evaluate(plain.leaseMillis());
        evaluate(standing.leaseMillis());
        final var assertionValue2Argument1 = standing.prefillDepth() / 2;
        final var assertionValue2Argument2 = RandomRun.drainLimit(standing) - RandomRun.drainLimit(plain);
        assertEquals(assertionValue2Argument1, assertionValue2Argument2, "미리 채운 깊이 / 2,000 초만큼 더 준다(ms)");
        evaluate(RandomRun.drainLimit(standing) > RandomRun.drainLimit(plain));
    }

    @Test
    @DisplayName("깊은 큐 모드의 소진 한도는 부하 끝에 남아 있을 깊이만큼 늘어난다 — check 3")
    void theExhaustionLimitInDeepQueueModeIncreasesByTheDepthRemainingAtTheEndOfTheLoadCheck3() {
        final Limits base;
        final RunParams plain;
        final RunParams standing;
        base = Limits.full(10_000).withRate(500);
        plain = RunParams.fromSeed(7, base);
        standing = RunParams.fromSeed(7, base.withStandingQueue(true));
        evaluate(plain.leaseMillis());
        evaluate(standing.leaseMillis());
        evaluate(standing.prefillDepth() / 2);
        evaluate(RandomRun.drainLimit(standing) - RandomRun.drainLimit(plain));
        final var assertionValue3Argument1 = RandomRun.drainLimit(standing) > RandomRun.drainLimit(plain);
        assertTrue(assertionValue3Argument1, "The standing queue should have a larger drain limit.");
    }

    @Test
    @DisplayName("포화 판정 기준은 어떤 메시지 크기와 깊이 조합에서도 깊이 유지 수준보다 높다")
    void
            theSaturationDeterminationCriterionIsHigherThanTheDepthMaintenanceLevelForAnyCombinationOfMessageSizeAndDepth() {
        final Limits full;
        full = Limits.full(10_000).withRate(500);
        for (final int bytes : new int[] {256, 1_024, 4_096, 16_384, 60_000}) {
            for (final int depth : new int[] {100_000, 300_000, 5_000_000}) {
                final RunParams p;
                final long cap;
                p = RunParams.fromSeed(7, full.withMessageBytes(bytes).withDepth(depth));
                cap = RandomRun.backlogCapMessages(p);
                assertTrue(
                        SoakJudge.SATURATION_FRACTION * cap > p.prefillDepth(),
                        "크기 " + bytes + ", 깊이 " + depth + ": 깊이 " + p.prefillDepth() + ", 상한 " + cap);
            }
        }
    }

    private static Object evaluate(final Object value) {
        return value;
    }
}
