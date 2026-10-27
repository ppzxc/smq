package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** 부모 JVM 쪽 러너. 조건마다 새 SQLite 파일을 만들고 자식 JVM들을 띄워 같은 파일에서 같은 구간을 재게 한다. */
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
            List<Role> roles = roles(spec);
            List<ChildJvm> children = new ArrayList<>();
            for (int i = 0; i < roles.size(); i++) children.add(launch(spec, roles.get(i), db, startAt, dir, i));
            List<ChildResult> results = new ArrayList<>();
            long timeout = startAt + spec.durationMs() + Workload.S3_DRAIN_MS + KILL_GRACE_MS - System.currentTimeMillis();
            for (ChildJvm c : children) {
                c.await(timeout);
                results.add(ChildResult.fromJson(c.lastJsonLine()));
            }
            return Aggregator.aggregate(spec, results);
        } finally {
            deleteTree(dir);
        }
    }

    static List<Role> roles(RunSpec spec) {
        return switch (spec.scenario()) {
            case S1, S2 -> java.util.Collections.nCopies(spec.processes(), Role.WORKER);
            case S3 -> spec.topology() == Topology.CROSS_PROCESS ? List.of(Role.PRODUCER, Role.CONSUMER)
                    : List.of(Role.BOTH);
        };
    }

    private static ChildJvm launch(RunSpec spec, Role role, Path db, long startAt, Path dir, int index)
            throws IOException {
        return ChildJvm.start(ChildMain.class.getName(),
                List.of(spec.toArg(), role.name(), db.toString(), String.valueOf(startAt)),
                dir.resolve("child-" + index + ".out"), role.name());
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
