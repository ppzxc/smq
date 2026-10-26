package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
            List<Child> children = new ArrayList<>();
            for (int i = 0; i < roles.size(); i++) children.add(launch(spec, roles.get(i), db, startAt, dir, i));
            List<ChildResult> results = new ArrayList<>();
            long timeout = startAt + spec.durationMs() + Workload.S3_DRAIN_MS + KILL_GRACE_MS - System.currentTimeMillis();
            for (Child c : children) results.add(c.await(timeout));
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

    private record Child(Process process, Path out, Role role) {

        ChildResult await(long timeoutMs) throws Exception {
            if (!process.waitFor(Math.max(timeoutMs, 1_000), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("자식 JVM이 제한 시간 안에 끝나지 않았다: " + role);
            }
            if (process.exitValue() != 0) throw new IllegalStateException("자식 JVM 실패(" + role + "), 종료 코드 " + process.exitValue());
            String line = Files.readAllLines(out).stream().filter(l -> l.startsWith("{")).reduce((a, b) -> b)
                    .orElseThrow(() -> new IllegalStateException("자식 JVM이 결과를 내지 않았다: " + role));
            return ChildResult.fromJson(line);
        }
    }

    private static Child launch(RunSpec spec, Role role, Path db, long startAt, Path dir, int index)
            throws IOException {
        Path out = dir.resolve("child-" + index + ".out");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process p = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-cp",
                System.getProperty("java.class.path"), ChildMain.class.getName(), spec.toArg(), role.name(),
                db.toString(), String.valueOf(startAt))
                .redirectOutput(out.toFile())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        return new Child(p, out, role);
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
