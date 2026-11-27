package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Smq;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WorkloadSmokeTest {

    @TempDir
    Path dir;

    private ChildResult run(final RunSpec spec, final Role role) throws Exception {
        try (Smq smq = Workload.open(spec, dir.resolve("smq.db"))) {
            Workload.prepare(smq, spec);
            return Workload.run(smq, spec, role, System.currentTimeMillis() + spec.warmupMs());
        }
    }

    @Test
    void s1ProcessesAtLeastOneChunk() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s1(Storage.MEMORY, Profile.P1, null, 500, 200, 50), Role.WORKER);
        assertTrue(r.messages() >= Profile.P1.chunk(), "The workload should process at least one chunk.");
    }

    @Test
    void s1RecordsElapsedTime() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s1(Storage.MEMORY, Profile.P1, null, 500, 200, 50), Role.WORKER);
        assertTrue(r.elapsedNanos() > 0, "Elapsed nanos should be greater than 0.");
    }

    @Test
    void s1ProcessesCompleteChunks() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s1(Storage.MEMORY, Profile.P1, null, 500, 200, 50), Role.WORKER);
        assertEquals(0, r.messages() % Profile.P1.chunk(), "Messages should match the expected value.");
    }

    @ParameterizedTest
    @EnumSource(Durability.class)
    void s1RunsOnSqliteForBothDurabilities(final Durability durability) throws Exception {
        final ChildResult r;
        r = run(RunSpec.s1(Storage.SQLITE, Profile.P1, durability, 0, 200, 50), Role.WORKER);
        assertTrue(r.messages() > 0, "The workload should process messages for " + durability + ".");
    }

    @Test
    void leasedHeadRemainsLeased() throws Exception {
        final RunSpec spec;
        spec = RunSpec.s1(RunSpec.QueueKind.LEASED_HEAD, 123, 200, 0);
        try (Smq smq = Workload.open(spec, dir.resolve("leased.db"))) {
            Workload.prepare(smq, spec);
            final var q = smq.queue(Workload.QUEUE);
            assertEquals(123, q.counts().leased(), "Leased should match the expected value.");
        }
    }

    @Test
    void leasedHeadHasNoReadyMessages() throws Exception {
        final RunSpec spec;
        spec = RunSpec.s1(RunSpec.QueueKind.LEASED_HEAD, 123, 200, 0);
        try (Smq smq = Workload.open(spec, dir.resolve("leased.db"))) {
            Workload.prepare(smq, spec);
            final var q = smq.queue(Workload.QUEUE);
            assertEquals(0, q.counts().ready(), "Ready should match the expected value.");
        }
    }

    @Test
    void leasedHeadAllowsNewMessagesToBePulled() throws Exception {
        final RunSpec spec;
        spec = RunSpec.s1(RunSpec.QueueKind.LEASED_HEAD, 123, 200, 0);
        try (Smq smq = Workload.open(spec, dir.resolve("leased.db"))) {
            Workload.prepare(smq, spec);
            assertTrue(
                    Workload.run(smq, spec, Role.WORKER, System.currentTimeMillis())
                                    .messages()
                            > 0,
                    "Messages should be greater than 0.");
        }
    }

    @Test
    void s2Publishes() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s2(Storage.SQLITE, Profile.P3, 200, 50), Role.WORKER);
        assertTrue(r.messages() > 0, "Messages should be greater than 0.");
    }

    @Test
    void s3CollectsLatencySamples() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s3(Storage.MEMORY, Profile.P1, 600, 0), Role.BOTH);
        assertTrue(r.latenciesMicros().length > 0, "Length should be greater than 0.");
    }

    @Test
    void s3LatencySamplesAreNonNegative() throws Exception {
        final ChildResult r;
        r = run(RunSpec.s3(Storage.MEMORY, Profile.P1, 600, 0), Role.BOTH);
        assertTrue(
                Arrays.stream(r.latenciesMicros()).allMatch(latency -> latency >= 0),
                "Latency samples should be nonnegative.");
    }
}
