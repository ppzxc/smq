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
        assertEquals(38, plan.size());
        // S1: P1~P4 x {1,4} x {POWER_LOSS, PROCESS_CRASH}
        for (String p : List.of("P1", "P2", "P3", "P4"))
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
        // S2: P1~P4, 1프로세스, 빈 큐
        assertTrue(k.contains("sqlite/S2/P3/proc1/POWER_LOSS/empty"));
        assertTrue(k.contains("sqlite/S2/P4/proc1/POWER_LOSS/empty"));
        // 단건(청크 1) 프로파일: 크기마다 S1 1프로세스 빈 큐(두 내구성)와 S2
        for (String p : List.of("P0", "P2S", "P3S", "P4S")) {
            assertTrue(k.contains("sqlite/S1/" + p + "/proc1/POWER_LOSS/empty"), p);
            assertTrue(k.contains("sqlite/S1/" + p + "/proc1/PROCESS_CRASH/empty"), p);
            assertTrue(k.contains("sqlite/S2/" + p + "/proc1/POWER_LOSS/empty"), p);
            assertFalse(k.contains("sqlite/S1/" + p + "/proc4/POWER_LOSS/empty"), p);
        }
        // P4(4KB)는 100만 건 깊이를 재지 않는다
        assertFalse(k.contains("sqlite/S1/P4/proc1/POWER_LOSS/1m"));
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
        assertEquals(4096, Profile.P4.payloadBytes());
        assertEquals(400, Profile.P4.chunk());
        // 단건 프로파일은 같은 데이터 크기에 청크만 1이다
        assertEquals(Profile.P2.payloadBytes(), Profile.P2S.payloadBytes());
        assertEquals(Profile.P3.payloadBytes(), Profile.P3S.payloadBytes());
        assertEquals(Profile.P4.payloadBytes(), Profile.P4S.payloadBytes());
        assertEquals(Profile.P1.payloadBytes(), Profile.P0.payloadBytes());
        assertEquals(1, Profile.P2S.chunk());
        assertEquals(1, Profile.P3S.chunk());
        assertEquals(1, Profile.P4S.chunk());
    }

    @Test
    void specTravelsToChildAsOneQuoteFreeArgument() {
        RunSpec s = RunSpec.s1(Storage.SQLITE, Profile.P4, 4, Durability.PROCESS_CRASH, 0, 5000, 700);
        String arg = s.toArg();
        // Windows의 ProcessBuilder는 인자 안의 큰따옴표를 지우므로, 따옴표와 공백이 없어야 한다.
        assertFalse(arg.contains("\"") || arg.contains(" "), arg);
        assertEquals(s, RunSpec.fromArg(arg));
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
