package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** 부모 JVM 쪽 러너. 조건마다 새 SQLite 파일을 만들고 자식 JVM 하나를 띄워 잰다. */
final class Runner {

    /** 자식 JVM이 뜨고 SQLite를 여는 데 주는 여유. */
    private static final long STARTUP_MARGIN_MS = 4_000;

    private static final long KILL_GRACE_MS = 60_000;

    private final Path workDir;

    Runner(Path workDir) {
        this.workDir = workDir;
    }

    Map<String, Double> run(RunSpec spec) throws Exception {
        Path dir = workDir.resolve("run");
        deleteTree(dir);
        Files.createDirectories(dir);
        try {
            Path db = dir.resolve("smq.db");
            if (spec.storage() == Storage.SQLITE) {
                try (Smq smq = Workload.open(spec, db)) {
                    Workload.prepare(smq, spec);
                }
            }
            long margin = STARTUP_MARGIN_MS + (spec.storage() == Storage.MEMORY ? spec.depth() / 100 : 0);
            long startAt = System.currentTimeMillis() + margin + spec.warmupMs();
            ChildJvm child = launch(spec, role(spec), db, startAt, dir);
            long timeout =
                    startAt + spec.durationMs() + Workload.S3_DRAIN_MS + KILL_GRACE_MS - System.currentTimeMillis();
            child.await(timeout);
            return Aggregator.aggregate(spec, List.of(ChildResult.fromJson(child.lastJsonLine())));
        } finally {
            deleteTree(dir);
        }
    }

    static Role role(RunSpec spec) {
        return switch (spec.scenario()) {
            case S1, S2 -> Role.WORKER;
            case S3 -> Role.BOTH;
        };
    }

    private static ChildJvm launch(RunSpec spec, Role role, Path db, long startAt, Path dir) throws IOException {
        return ChildJvm.start(
                ChildMain.class.getName(),
                List.of(spec.toArg(), role.name(), db.toString(), String.valueOf(startAt)),
                dir.resolve("child.out"),
                role.name());
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
