package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class PlanTest {

    private static Set<String> keys(List<RunSpec> plan) {
        return plan.stream().map(RunSpec::key).collect(Collectors.toSet());
    }

    @Test
    void fullSqlitePlanFollowsTheSpecTable() {
        List<RunSpec> plan = Plan.full(Storage.SQLITE);
        Set<String> k = keys(plan);
        assertEquals(plan.size(), k.size(), "키가 겹치면 안 된다");
        assertEquals(23, plan.size());
        // S1: P1~P3 x {1,4} x {POWER_LOSS, PROCESS_CRASH}
        for (String p : List.of("P1", "P2", "P3"))
            for (String n : List.of("proc1", "proc4"))
                for (String d : List.of("POWER_LOSS", "PROCESS_CRASH"))
                    assertTrue(k.contains("sqlite/S1/" + p + "/" + n + "/" + d + "/empty"), p + n + d);
        // P0은 1프로세스, 빈 큐만
        assertTrue(k.contains("sqlite/S1/P0/proc1/POWER_LOSS/empty"));
        assertFalse(k.contains("sqlite/S1/P0/proc4/POWER_LOSS/empty"));
        // 100만 건 깊이는 P1에서만
        assertTrue(k.contains("sqlite/S1/P1/proc1/POWER_LOSS/1m"));
        assertTrue(k.contains("sqlite/S1/P1/proc4/PROCESS_CRASH/1m"));
        assertFalse(k.contains("sqlite/S1/P2/proc1/POWER_LOSS/1m"));
        // S2: P1~P3, 1프로세스, 빈 큐
        assertTrue(k.contains("sqlite/S2/P3/proc1/POWER_LOSS/empty"));
        assertFalse(k.contains("sqlite/S2/P0/proc1/POWER_LOSS/empty"));
        assertFalse(k.contains("sqlite/S2/P1/proc4/POWER_LOSS/empty"));
        // S3: P1만, 같은 프로세스와 다른 프로세스
        assertTrue(k.contains("sqlite/S3/P1/same-process/POWER_LOSS/empty"));
        assertTrue(k.contains("sqlite/S3/P1/cross-process/POWER_LOSS/empty"));
        assertEquals(2, k.stream().filter(s -> s.contains("/S3/")).count());
        assertTrue(plan.stream().filter(s -> s.depth() > 0).allMatch(s -> s.depth() == 1_000_000));
    }

    @Test
    void fullMemoryPlanIsSingleJvmDiagnostics() {
        List<RunSpec> plan = Plan.full(Storage.MEMORY);
        assertTrue(plan.stream().allMatch(s -> s.processes() == 1 && s.storage() == Storage.MEMORY));
        assertTrue(plan.stream().noneMatch(s -> s.topology() == Topology.CROSS_PROCESS));
        assertTrue(keys(plan).contains("memory/S1/P1/proc1/none/empty"));
        assertTrue(keys(plan).contains("memory/S3/P1/same-process/none/empty"));
    }

    @Test
    void smokeIsShortAndSmall() {
        List<RunSpec> plan = Plan.smoke(Storage.SQLITE);
        assertTrue(plan.size() >= 4 && plan.size() <= 8);
        assertTrue(plan.stream().allMatch(s -> s.durationMs() <= 3000 && s.depth() <= 10_000));
        assertTrue(plan.stream().anyMatch(s -> s.processes() > 1), "다중 JVM 경로를 지나야 한다");
        assertTrue(plan.stream().anyMatch(s -> s.topology() == Topology.CROSS_PROCESS));
    }

    @Test
    void profilesMatchTheSpec() {
        assertEquals(256, Profile.P0.payloadBytes());
        assertEquals(1, Profile.P0.chunk());
        assertEquals(100, Profile.P1.chunk());
        assertEquals(2048, Profile.P2.payloadBytes());
        assertEquals(300, Profile.P2.chunk());
        assertEquals(6144, Profile.P3.payloadBytes());
        assertEquals(500, Profile.P3.chunk());
    }

    @Test
    void specRoundTripsThroughJson() {
        RunSpec s = RunSpec.s1(Storage.SQLITE, Profile.P3, 4, Durability.PROCESS_CRASH, 1_000_000, 5000, 700);
        RunSpec back = RunSpec.fromJson(s.toJson());
        assertEquals(s, back);
        RunSpec m = RunSpec.s3(Storage.MEMORY, Profile.P1, Topology.SAME_PROCESS, 10, 1);
        assertEquals(m, RunSpec.fromJson(m.toJson()));
    }
}
