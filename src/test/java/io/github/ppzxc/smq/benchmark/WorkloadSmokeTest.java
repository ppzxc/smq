package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Smq;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 하네스 워크로드가 인메모리와 SQLite에서 끝까지 도는지 확인하는 스모크. 수치는 단언하지 않는다. */
class WorkloadSmokeTest {

    @TempDir
    Path dir;

    private ChildResult run(RunSpec spec, Role role) throws Exception {
        try (Smq smq = Workload.open(spec, dir.resolve("smq.db"))) {
            Workload.prepare(smq, spec);
            return Workload.run(smq, spec, role, System.currentTimeMillis() + spec.warmupMs());
        }
    }

    @Test
    void s1CountsAckedMessagesWithPrefilledDepth() throws Exception {
        ChildResult r = run(RunSpec.s1(Storage.MEMORY, Profile.P1, 1, null, 500, 200, 50), Role.WORKER);
        assertTrue(r.messages() >= Profile.P1.chunk());
        assertTrue(r.elapsedNanos() > 0);
        assertEquals(0, r.messages() % Profile.P1.chunk());
    }

    @Test
    void s1RunsOnSqliteForBothDurabilities() throws Exception {
        for (Durability d : Durability.values()) {
            ChildResult r = run(RunSpec.s1(Storage.SQLITE, Profile.P1, 1, d, 0, 200, 50), Role.WORKER);
            assertTrue(r.messages() > 0, d.name());
        }
    }

    @Test
    void s2Publishes() throws Exception {
        ChildResult r = run(RunSpec.s2(Storage.SQLITE, Profile.P2, 200, 50), Role.WORKER);
        assertTrue(r.messages() > 0);
    }

    @Test
    void s3SameProcessCollectsLatencySamples() throws Exception {
        ChildResult r = run(RunSpec.s3(Storage.MEMORY, Profile.P1, Topology.SAME_PROCESS, 600, 0), Role.BOTH);
        assertTrue(r.latenciesMicros().length > 0);
        for (long l : r.latenciesMicros()) assertTrue(l >= 0, "지연은 음수일 수 없다");
    }
}
