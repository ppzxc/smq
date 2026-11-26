package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RandomRunTest {

    @TempDir
    Path dir;

    @Test
    void shortRunPassesAndRecordsPublishedAndDelivered() {
        RunParams p = RunParams.fromSeed(5, Limits.smoke(400));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check);
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        assertFalse(r.records().published().isEmpty());
        assertFalse(r.records().deliveries().isEmpty());
        assertTrue(Files.exists(dir.resolve("smq.db")));
        assertTrue(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void checkerViolationsMakeTheVerdictAViolation() {
        RunParams p = RunParams.fromSeed(5, Limits.smoke(300));
        Violation v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        RunResult r = RandomRun.execute(p, dir, (c, pub, del, smp) -> List.of(v));
        assertEquals(Verdict.VIOLATION, r.verdict());
        assertEquals(List.of(v), r.violations());
    }

    @Test
    void anExceptionInTheHarnessJvmIsReportedAsHarness() {
        RunParams p = RunParams.fromSeed(5, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalArgumentException("검사기가 터졌다");
        });
        assertEquals(Verdict.ERROR, r.verdict());
        ErrorDetail d = r.errorDetail();
        assertEquals("harness", d.location());
        assertEquals("java.lang.IllegalArgumentException", d.exceptionClass());
        assertEquals("검사기가 터졌다", d.message());
        assertTrue(d.stackTrace().contains("\tat "));
    }

    @Test
    void everyRunRecordsTheLargestWalSizeSeen() {
        RunParams p = RunParams.fromSeed(5, Limits.smoke(400));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check);
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        assertNotNull(r.walMaxBytes());
        assertTrue(r.walMaxBytes() > 0, "실행 중 WAL이 있었다: " + r.walMaxBytes());
    }

    @Test
    void anErrorResultAlsoCarriesTheWalSize() {
        RunParams p = RunParams.fromSeed(5, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, (c, pub, del, smp) -> {
            throw new IllegalStateException("검사기");
        });
        assertEquals(Verdict.ERROR, r.verdict());
        assertNotNull(r.walMaxBytes());
    }

    @Test
    void 고정_속도로_돌면_달성률과_백로그_보고가_붙는다() {
        Limits soak = Limits.smoke(2_000).withRate(200);
        RunParams p = RunParams.fromSeed(1, soak);
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check);
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error() + " " + r.soak());
        assertNotNull(r.soak());
        assertEquals(200, r.soak().targetRate());

        assertTrue(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5, "달성률: " + r.soak());

        RunResult plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::check);
        assertNull(plain.soak());
    }

    @Test
    void 백로그_증가는_불변식이_멀쩡해도_실패이고_위반과_소진_실패가_우선한다() {
        SoakJudge.Report growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        SoakJudge.Report fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        Violation v = new Violation(Invariant.GHOST, List.of(), List.of(), "합성");
        assertEquals(Verdict.BACKLOG_GROWING, RandomRun.verdict(true, List.of(), growing, 0));
        assertEquals(Verdict.PASS, RandomRun.verdict(true, List.of(), fine, 0), "달성률이 낮아도 보고만 한다");
        assertEquals(Verdict.PASS, RandomRun.verdict(true, List.of(), null, 0.95));
        assertEquals(Verdict.VIOLATION, RandomRun.verdict(true, List.of(v), growing, 0));
        assertEquals(Verdict.DRAIN_TIMEOUT, RandomRun.verdict(false, List.of(v), growing, 0));

        assertEquals(Verdict.UNDER_RATE, RandomRun.verdict(true, List.of(), fine, 0.95));
        assertEquals(Verdict.PASS, RandomRun.verdict(true, List.of(), fine, 0.3), "기준과 같으면 통과다");
        assertEquals(Verdict.BACKLOG_GROWING, RandomRun.verdict(true, List.of(), growing, 1.0));
        assertEquals(Verdict.VIOLATION, RandomRun.verdict(true, List.of(v), fine, 0.95));
    }

    private Path subdir(String name) {
        try {
            return Files.createDirectories(dir.resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long lateLoadMedianDepth(RunResult r) {
        List<Long> depths = new ArrayList<>();
        for (CountsSample c : r.records().samples()) {
            if (c.queue() == QueueKind.TARGET && !c.afterDrain()) {
                depths.add(c.ready() + c.leased() + c.delayed());
            }
        }
        List<Long> late = depths.subList(depths.size() / 2, depths.size());
        List<Long> sorted = late.stream().sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    @Test
    void 깊은_큐_모드는_깊이가_목표_이하면_컨슈머가_쉬어서_미리_채운_깊이가_유지된다() {

        Limits standingLimits =
                Limits.smoke(2_500).withRate(200).withDepth(20_000).withStandingQueue(true);
        RunParams standing = RunParams.fromSeed(1, standingLimits);
        RunResult kept = RandomRun.execute(standing, subdir("standing"), InvariantChecker::check);
        assertEquals(Verdict.PASS, kept.verdict(), kept.violations() + " " + kept.error());
        assertTrue(
                lateLoadMedianDepth(kept) > standing.prefillDepth() * 0.5,
                "깊이가 유지돼야 한다: " + lateLoadMedianDepth(kept) + " / " + standing.prefillDepth());

        Limits drainingLimits = Limits.smoke(2_500).withRate(200);
        RunParams draining = RunParams.fromSeed(1, drainingLimits);
        RunResult drained = RandomRun.execute(draining, subdir("draining"), InvariantChecker::check);
        assertTrue(
                lateLoadMedianDepth(drained) < draining.prefillDepth() * 0.5,
                "조절하지 않으면 컨슈머가 깊이를 비운다(대조군): " + lateLoadMedianDepth(drained));
    }

    @Test
    void 깊은_큐_모드의_소진_한도는_부하_끝에_남아_있을_깊이만큼_늘어난다() {
        Limits base = Limits.full(10_000).withRate(500);
        RunParams plain = RunParams.fromSeed(7, base);
        RunParams standing = RunParams.fromSeed(7, base.withStandingQueue(true));
        assertEquals(plain.leaseMillis(), standing.leaseMillis(), "같은 시드라 리스 기한은 같다");

        assertEquals(
                standing.prefillDepth() / 2,
                RandomRun.drainLimit(standing) - RandomRun.drainLimit(plain),
                "미리 채운 깊이 / 2,000 초만큼 더 준다(ms)");
        assertTrue(RandomRun.drainLimit(standing) > RandomRun.drainLimit(plain));
    }

    @Test
    void 포화_판정_기준은_어떤_메시지_크기와_깊이_조합에서도_깊이_유지_수준보다_높다() {

        Limits full = Limits.full(10_000).withRate(500);
        for (int bytes : new int[] {256, 1_024, 4_096, 16_384, 60_000}) {
            for (int depth : new int[] {100_000, 300_000, 5_000_000}) {
                RunParams p = RunParams.fromSeed(7, full.withMessageBytes(bytes).withDepth(depth));
                long cap = RandomRun.backlogCapMessages(p);
                assertTrue(
                        SoakJudge.SATURATION_FRACTION * cap > p.prefillDepth(),
                        "크기 " + bytes + ", 깊이 " + depth + ": 깊이 " + p.prefillDepth() + ", 상한 " + cap);
            }
        }
    }
}
