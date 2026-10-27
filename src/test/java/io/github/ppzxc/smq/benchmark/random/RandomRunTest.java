package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
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
            java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
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

                public void destroy() {
                }
            };
        };
    }

    @Test
    void multiProcessRunMergesEveryProcessAndPasses() {
        RunParams p = RunParams.fromSeed(2, Limits.smoke(1_000));
        assertEquals(4, p.processes());
        RunResult r = RandomRun.execute(p, dir, InvariantChecker::check, inProcess());
        assertEquals(Verdict.PASS, r.verdict(), r.violations() + " " + r.error());
        java.util.Set<Integer> producing = r.records().published().stream().map(x -> x.tag().process())
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(producing.size() > 1, "프로세스마다 다른 태그: " + producing);
        assertTrue(producing.stream().allMatch(n -> n >= 0 && n < p.processes()) || producing.contains(p.processes()));
        java.util.Set<Long> chunkOwners = r.records().deliveries().stream().map(d -> d.pull().chunk() >>> 40)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(chunkOwners.size() > 1, "청크 번호는 프로세스별로 갈린다: " + chunkOwners);
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
        Published pub = new Published(new Tag(9, 2, 5), new Content(77, java.util.Map.of("bench-tag", "9/2/5")), 1234, 50);
        Delivery del = new Delivery(QueueKind.DEAD_LETTER, new Tag(9, 2, 5), 31, 3,
                new Content(77, java.util.Map.of("bench-tag", "9/2/5", "smq-reason", "rejected")),
                new Delivery.Pull((2L << 40) | 7, 1, 2000, 3000), Delivery.Settle.nack(2500, 100));
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
        Session session = new Session(dir, dir.resolve("runs.jsonl"), java.util.Map.of(), Limits.smoke(1_000),
                (params, runDir) -> RandomRun.execute(params, runDir, (c, pub, del, smp) -> java.util.List.of(v),
                        inProcess()),
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
}
