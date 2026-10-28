package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.Environment;
import io.github.ppzxc.smq.benchmark.ModuleGuard;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** 무작위 하네스의 부모 JVM 진입점. 옵션은 {@link Options}를 본다. 모든 실행이 통과하면 0, 첫 위반에서 멈추면 1로 끝낸다. */
public final class RandomBenchMain {

    private RandomBenchMain() {}

    public static void main(String[] args) throws IOException {
        ModuleGuard.requireSmqModule("부모 JVM");
        Options o;
        try {
            o = Options.parse(args, System.currentTimeMillis());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        System.exit(run(o));
    }

    static int run(Options o) throws IOException {
        Files.createDirectories(o.outDir());
        Path workRoot = o.outDir().resolve("work");
        Files.createDirectories(workRoot);
        Map<String, String> env = Environment.capture(o.smoke() ? "smoke" : "random", "sqlite", workRoot);
        Limits limits = o.smoke() ? Limits.smoke(o.load().toMillis()) : Limits.full(o.load().toMillis());
        Path jsonLines = o.outDir().resolve("runs-"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".jsonl");

        Iterator<Long> seeds;
        long budget;
        if (o.smoke()) {
            seeds = Options.SMOKE_SEEDS.iterator();
            budget = Long.MAX_VALUE;
        } else if (o.seed().isPresent()) {
            seeds = List.of(o.seed().getAsLong()).iterator();
            budget = Long.MAX_VALUE;
        } else {
            SplittableRandom master = new SplittableRandom(o.masterSeed());
            seeds = new Iterator<>() {
                public boolean hasNext() {
                    return true;
                }

                public Long next() {
                    return master.nextLong();
                }
            };
            budget = o.budget().toMillis();
            System.out.println("마스터 시드: " + o.masterSeed());
        }
        System.out.println("환경: " + env);
        System.out.println("기록: " + jsonLines);
        Session session = new Session(o.outDir(), jsonLines, env, limits,
                (params, dir) -> RandomRun.execute(params, dir, InvariantChecker::check, ChildSpawner.jvm(o.heapDump())), System.out);
        return session.run(seeds, budget);
    }
}
