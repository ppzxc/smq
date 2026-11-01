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
    void theParentMergesTheRecordsOfEveryChildProcess() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r =
                RandomRun.execute(p, dir, (c, pub, del, smp) -> java.util.List.of(), fixedOutputs(java.util.Set.of()));
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        assertEquals(
                java.util.Set.of(0, 1, 2, 3),
                r.records().published().stream()
                        .map(x -> x.tag().process())
                        .collect(java.util.stream.Collectors.toSet()));
        assertEquals(
                java.util.Set.of(0L, 1L, 2L, 3L),
                r.records().deliveries().stream()
                        .map(d -> d.pull().chunk() >>> 40)
                        .collect(java.util.stream.Collectors.toSet()));
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
    void multiProcessRunMergesEveryProcessAndPasses() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(1_000));
        assertEquals(4, p.processes());
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, inProcess());
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        // 어느 프로세스가 실제로 발행하고 꺼내는지는 SQLite 쓰기 락 경합이 정한다. 한 프로세스가 부하 구간 내내 락을 못 잡을 수
        // 있으므로(ADR-0009의 락 대기 규약) 여기서 프로세스 분포는 단언하지 않는다. 합치기는 아래 고정 입력 테스트가 본다.
        assertTrue(r.records().published().stream()
                .allMatch(x -> x.tag().process() >= 0 && x.tag().process() <= p.processes()));
        for (int i = 0; i < p.processes(); i++) {
            assertTrue(Files.exists(dir.resolve("p" + i + "-deliveries.jsonl")), "자식 " + i + "의 전달 기록 파일");
        }
        assertTrue(r.records().samples().stream().anyMatch(CountsSample::afterDrain));
    }

    @Test
    void aFailingChildMakesTheRunAnError() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, spec -> {
            throw new java.io.IOException("자식을 띄우지 못했다");
        });
        assertEquals(Verdict.ERROR, r.verdict());
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

    @Test
    void aViolationInAMultiProcessRunKeepsEveryProcessDeliveriesInTheFailureDir() throws Exception {
        Violation v = new Violation(Invariant.GHOST, java.util.List.of(), java.util.List.of(), "합성");
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        Session session = new Session(
                dir,
                dir.resolve("runs.jsonl"),
                java.util.Map.of(),
                Limits.smoke(1_000),
                (params, runDir) -> RandomRun.execute(
                        params, runDir, (c, pub, del, smp) -> java.util.List.of(v), fixedOutputs(java.util.Set.of())),
                new java.io.PrintStream(bytes, true));
        assertEquals(Session.EXIT_FAILED, session.run(java.util.List.of(2L).iterator(), Long.MAX_VALUE));
        Path failure = session.failureDir(2);
        for (int i = 0; i < 4; i++) {
            assertTrue(Files.size(failure.resolve("p" + i + "-deliveries.jsonl")) >= 0, "프로세스 " + i);
        }
        assertTrue(Files.exists(failure.resolve("smq.db")));
        java.util.Set<String> processes = new java.util.HashSet<>();
        for (String line : Files.readAllLines(failure.resolve("deliveries.jsonl"))) {
            processes.add(RecordsJson.parseDelivery(line).tag().toString().split("/")[1]);
        }
        assertTrue(processes.size() > 1, "합친 전달 기록에 여러 프로세스의 태그가 있다: " + processes);
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
    void aChildOutOfMemoryErrorIsReportedWithItsLocationAndFullStackTrace() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, failingChild(1, () -> {
            throw new OutOfMemoryError("Java heap space");
        }));
        assertEquals(Verdict.ERROR, r.verdict());
        ErrorDetail d = r.errorDetail();
        assertNotNull(d);
        assertEquals("child-1", d.location());
        assertEquals("java.lang.OutOfMemoryError", d.exceptionClass());
        assertEquals("Java heap space", d.message());
        assertTrue(d.stackTrace().startsWith("java.lang.OutOfMemoryError: Java heap space"), d.stackTrace());
        assertTrue(d.stackTrace().contains("\tat "), "스택 프레임");
        assertEquals(1, d.exitCode());
    }

    @Test
    void aChildThatDiesWithoutReportingIsJudgedByItsExitCodeAndStderrTail() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, spec -> new ChildSpawner.Running() {
            public void await(long timeoutMs) throws Exception {
                if (spec.process() == 0) {
                    throw new ChildFailure(0, 137, "Killed", new IllegalStateException("자식 JVM 실패, 종료 코드 137"));
                }
            }

            public void destroy() {}
        });
        assertEquals(Verdict.ERROR, r.verdict());
        ErrorDetail d = r.errorDetail();
        assertEquals("child-0", d.location());
        assertEquals(137, d.exitCode());
        assertEquals("Killed", d.stderrTail());
        assertEquals("java.lang.IllegalStateException", d.exceptionClass());
    }

    @Test
    void anExceptionFromAnUnidentifiedAwaitIsStillAttributedToTheChildThatWasAwaited() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, spec -> new ChildSpawner.Running() {
            public void await(long timeoutMs) throws Exception {
                if (spec.process() == 2) {
                    throw new java.util.concurrent.TimeoutException("시간 초과");
                }
            }

            public void destroy() {}
        });
        assertEquals("child-2", r.errorDetail().location());
        assertEquals("java.util.concurrent.TimeoutException", r.errorDetail().exceptionClass());
        assertNull(r.errorDetail().exitCode());
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
    void aSpawnFailureIsAHarnessError() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(300));
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, spec -> {
            throw new java.io.IOException("자식을 띄우지 못했다");
        });
        assertEquals("harness", r.errorDetail().location());
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
    void 자식_프로세스마다_메트릭_요약이_결과에_붙는다() {
        Limits soak = Limits.smoke(1_000).withRate(200);
        RunParams p = paramsWithProcesses(2, soak);
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, inProcess());
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        assertEquals(2, r.childMetrics().size());
        assertTrue(
                r.childMetrics().stream().allMatch(m -> m.sampleCount() > 0),
                r.childMetrics().toString());
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

    @Test
    void 자식을_정리할_때는_모두에게_종료를_먼저_알리고_그다음_기다린다() {
        java.util.List<String> events = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        ChildSpawner recording = spec -> {
            int n = spec.process();
            ChildOutput.write(spec.outDir(), n, new ChildOutput(0, 0, java.util.List.of(), java.util.List.of()));
            return new ChildSpawner.Running() {
                @Override
                public void await(long timeoutMs) {}

                @Override
                public void requestStop() {
                    events.add("stop" + n);
                }

                @Override
                public void destroy() {
                    events.add("destroy" + n);
                }
            };
        };
        RunParams p = paramsWithProcesses(3, Limits.smoke(300));
        RandomRun.execute(p, dir, (c, pub, del, smp) -> java.util.List.of(), recording);

        assertEquals(
                java.util.List.of("stop0", "stop1", "stop2", "destroy0", "destroy1", "destroy2"),
                events,
                "순서대로 하나씩 신호를 보내고 기다리면 늦게 끝나는 자식 뒤의 자식은 그동안 부하를 돌린다");
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
