package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.Smq;
import java.nio.file.Path;

/**
 * 자식 JVM 진입점. 인자는 {@code <RunSpec JSON> <역할> <DB 경로> <측정 시작 epoch ms>}이고, 결과를 JSON 한 줄로 표준 출력에 낸다.
 */
public final class ChildMain {

    private ChildMain() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(final String[] args) throws Exception {
        final RunSpec spec;
        final Role role;
        final Path db;
        final long startAt;
        final ChildResult result;
        if (args.length != 4) {
            System.err.println("사용법: ChildMain <specArg> <role> <dbPath> <startAtEpochMs>");
            System.exit(2);
        }
        ModuleGuard.requireSmqModule("자식 JVM");
        spec = RunSpec.fromArg(args[0]);
        role = Role.valueOf(args[1]);
        db = Path.of(args[2]);
        startAt = Long.parseLong(args[3]);

        try (Smq smq = Workload.open(spec, db)) {
            // 인메모리는 JVM끼리 공유되지 않으므로 이 JVM이 직접 채운다. SQLite는 부모가 미리 채웠다.
            if (spec.storage() == Storage.MEMORY) Workload.prepare(smq, spec);
            result = Workload.run(smq, spec, role, startAt);
        }
        System.out.println(result.toJson());
    }
}
