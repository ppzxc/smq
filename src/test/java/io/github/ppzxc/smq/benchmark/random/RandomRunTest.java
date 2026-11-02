package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 하네스 전체를 짧게 한 번 돌려 본다(SQLite). 실제 시계를 쓰므로 수치는 단언하지 않고 판정만 본다. */
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
        Violation v = new Violation(Invariant.GHOST, java.util.List.of(), java.util.List.of(), "합성");
        RunResult r = RandomRun.execute(p, dir, (c, pub, del, smp) -> java.util.List.of(v));
        assertEquals(Verdict.VIOLATION, r.verdict());
        assertEquals(java.util.List.of(v), r.violations());
    }

    /** 같은 JVM의 스레드로 자식을 흉내 낸다. 자식 JVM 띄우기는 {@code randomBenchSmoke}가 모듈 경로에서 확인한다. */
    private static ChildSpawner inProcess() {
        return spec -> {
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    RandomRun.runChild(spec.params(), spec.process(), spec.db(), spec.outDir(), spec.startAt());
                } catch (Throwable e) {
                    error.set(e);
                }
            });
            t.start();
            return new ChildSpawner.Running() {
                public void await(long timeoutMs) throws Exception {
                    t.join(timeoutMs);
                    if (error.get() != null) {
                        throw new IllegalStateException(error.get());
                    }
                }

                public void destroy() {}
            };
        };
    }

    /**
     * 자식이 남기는 기록을 고정 입력으로 쓰는 스포너. 락 경합과 무관하게 부모가 프로세스별 기록을 합치는 일만 본다.
     * {@code silent}에 든 프로세스는 부하 구간 내내 쓰기 락을 못 잡은 자식처럼 아무 기록도 남기지 않는다.
     */
    private static ChildSpawner fixedOutputs(java.util.Set<Integer> silent) {
        return spec -> {
            int n = spec.process();
            java.util.List<Published> pub = new java.util.ArrayList<>();
            java.util.List<Delivery> del = new java.util.ArrayList<>();
            if (!silent.contains(n)) {
                Tag tag = new Tag(spec.params().seed(), n, 1);
                Content content = new Content(77, java.util.Map.of("bench-tag", tag.toString()));
                pub.add(new Published(tag, content, 1234, 0));
                del.add(new Delivery(
                        QueueKind.TARGET,
                        tag,
                        100 + n,
                        1,
                        content,
                        new Delivery.Pull(((long) n << 40) | 1, 0, 2000, 3000),
                        Delivery.Settle.NONE));
            }
            ChildOutput.write(spec.outDir(), n, new ChildOutput(del.size(), 0, pub, del));
            return new ChildSpawner.Running() {
                public void await(long timeoutMs) {}

                public void destroy() {}
            };
        };
    }

    @Test
    void aChildThatNeverGotTheWriteLockLeavesTheOthersRecordsMergedAndTheRunPassing() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(
                p, dir, (c, pub, del, smp) -> java.util.List.of(), fixedOutputs(java.util.Set.of(1, 2, 3)));
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        assertEquals(
                java.util.Set.of(0),
                r.records().published().stream()
                        .map(x -> x.tag().process())
                        .collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void childRecordsRoundTripThroughTheirFiles() throws Exception {
        Published pub =
                new Published(new Tag(9, 2, 5), new Content(77, java.util.Map.of("bench-tag", "9/2/5")), 1234, 50);
        Delivery del = new Delivery(
                QueueKind.DEAD_LETTER,
                new Tag(9, 2, 5),
                31,
                3,
                new Content(77, java.util.Map.of("bench-tag", "9/2/5", "smq-reason", "rejected")),
                new Delivery.Pull((2L << 40) | 7, 1, 2000, 3000),
                Delivery.Settle.nack(2500, 100));
        ChildOutput.write(dir, 2, new ChildOutput(11, 2222, java.util.List.of(pub), java.util.List.of(del)));
        ChildOutput back = ChildOutput.read(dir, 2);
        assertEquals(java.util.List.of(pub), back.published());
        assertEquals(java.util.List.of(del), back.deliveries());
        assertEquals(11, back.loadAckedMessages());
        assertEquals(2222, back.loadAckedBytes());
    }

    // ---- ERROR 진단: 발생 위치, 스택 트레이스, 자식 종료 코드와 표준 오류

    /** 자식 {@code failing}만 {@code body}에서 실패하고, 나머지는 바로 끝나는 스포너. 자식 JVM의 보고 경로({@link ChildReport})를 그대로 탄다. */
    private static ChildSpawner failingChild(int failing, ChildReport.Body body) {
        return spec -> new ChildSpawner.Running() {
            public void await(long timeoutMs) throws Exception {
                if (spec.process() != failing) {
                    return;
                }
                try {
                    ChildReport.guard(spec.outDir(), spec.process(), body);
                } catch (Throwable t) {
                    // 실제 자식 JVM은 종료 코드 1로 끝난다. 부모는 그것만 본다.
                    throw new ChildFailure(spec.process(), 1, "", t instanceof Exception e ? e : new Exception(t));
                }
            }

            public void destroy() {}
        };
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
        assertNull(d.exitCode());
        assertNull(d.stderrTail());
    }

    @Test
    void stderrTailKeepsOnlyTheLastBytes() throws Exception {
        Path err = dir.resolve("p0.err");
        Files.writeString(err, "a".repeat(10_000) + "끝");
        String tail = ChildFailure.readTail(err, 100);
        assertTrue(tail.endsWith("끝"));
        assertTrue(tail.length() <= 100, "상한: " + tail.length());
        assertEquals("", ChildFailure.readTail(dir.resolve("없음.err"), 100));
    }

    // ---- WAL 최대 크기

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

    // ---- 고정 속도 모드

    /** 프로세스 수가 {@code processes}인 첫 시드. 시드마다 프로세스 수가 달라서 찾아 쓴다. */
    private static RunParams paramsWithProcesses(int processes, Limits limits) {
        for (long seed = 1; seed < 500; seed++) {
            RunParams p = RunParams.fromSeed(seed, limits);
            if (p.processes() == processes) {
                return p;
            }
        }
        throw new AssertionError("프로세스 수 " + processes + "인 시드가 없다");
    }

    @Test
    void 고정_속도로_돌면_달성률과_백로그_보고가_붙는다() {
        Limits soak = Limits.smoke(2_000).withRate(200);
        RunParams p = paramsWithProcesses(1, soak);
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check);
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error() + " " + r.soak());
        assertNotNull(r.soak());
        assertEquals(200, r.soak().targetRate());
        // 속도 조절이 없으면 2초에 수천 건이라 비율이 크게 넘는다. 실제 시계라 아래쪽은 느린 환경을 봐서 넉넉히, 위쪽만 조인다.
        assertTrue(r.soak().achievedRatio() > 0.2 && r.soak().achievedRatio() < 1.5, "달성률: " + r.soak());
        assertTrue(r.childMetrics().isEmpty(), "단일 프로세스에는 자식이 없다");

        RunResult plain = RandomRun.execute(
                RunParams.fromSeed(5, Limits.smoke(300)), dir.resolve("plain"), InvariantChecker::check);
        assertNull(plain.soak());
    }

    @Test
    void 백로그_증가는_불변식이_멀쩡해도_실패이고_위반과_소진_실패가_우선한다() {
        SoakJudge.Report growing = new SoakJudge.Report(500, 1.0, 100, 200, 5_000, true);
        SoakJudge.Report fine = new SoakJudge.Report(500, 0.3, 100, 0, 10, false);
        Violation v = new Violation(Invariant.GHOST, java.util.List.of(), java.util.List.of(), "합성");
        assertEquals(Verdict.BACKLOG_GROWING, RandomRun.verdict(true, java.util.List.of(), growing));
        assertEquals(Verdict.PASS, RandomRun.verdict(true, java.util.List.of(), fine), "달성률이 낮아도 보고만 한다");
        assertEquals(Verdict.PASS, RandomRun.verdict(true, java.util.List.of(), null));
        assertEquals(Verdict.VIOLATION, RandomRun.verdict(true, java.util.List.of(v), growing));
        assertEquals(Verdict.DRAIN_TIMEOUT, RandomRun.verdict(false, java.util.List.of(v), growing));
    }

    private Path subdir(String name) {
        try {
            return Files.createDirectories(dir.resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 부하 구간 후반 절반의 깊이 표본 중앙값. 마지막 한 표본은 청크 몇 개가 한꺼번에 빠진 순간일 수 있다. */
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
        // 청크 x 컨슈머 수(최대 2,000)보다 충분히 깊어야 한 번에 몇 청크가 빠져도 목표 근처에 머문다. 실제 설정은 10만 건이다.
        Limits standingLimits =
                Limits.smoke(2_500).withRate(200).withDepth(20_000).withStandingQueue(true);
        RunParams standing = paramsWithProcesses(1, standingLimits);
        RunResult kept = RandomRun.execute(standing, subdir("standing"), InvariantChecker::check);
        assertEquals(Verdict.PASS, kept.verdict(), kept.violations() + " " + kept.error());
        assertTrue(
                lateLoadMedianDepth(kept) > standing.prefillDepth() * 0.5,
                "깊이가 유지돼야 한다: " + lateLoadMedianDepth(kept) + " / " + standing.prefillDepth());

        // 대조군: 조절 없이 기본 깊이(2,000)면 컨슈머가 비운다. 깊이가 달라 직접 비교는 아니고, 조절이 없으면 비워진다는 것만 확인한다.
        // (깊이 20,000은 이 짧은 부하에서 비워지지 않아 대조군이 되지 못한다.)
        Limits drainingLimits = Limits.smoke(2_500).withRate(200);
        RunParams draining = paramsWithProcesses(1, drainingLimits);
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
        // 소진은 초당 2,000건 이상을 가정한다. 깊이가 유지되면 부하가 끝날 때 그만큼 남아 있다.
        assertEquals(
                standing.prefillDepth() / 2,
                RandomRun.drainLimit(standing) - RandomRun.drainLimit(plain),
                "미리 채운 깊이 / 2,000 초만큼 더 준다(ms)");
        assertTrue(RandomRun.drainLimit(standing) > RandomRun.drainLimit(plain));
    }

    @Test
    void 포화_판정_기준은_어떤_메시지_크기와_깊이_조합에서도_깊이_유지_수준보다_높다() {
        // 미리 채운 깊이는 데이터 256MB 추정으로도 제한되므로, 큰 메시지 + 깊은 큐 조합에서도 정상 깊이가 포화로 판정되지 않아야 한다.
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
