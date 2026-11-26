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
        assertEquals(33, plan.size());
        for (String kind : List.of("MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD"))
            for (String depth : List.of("empty", "1m"))
                assertTrue(k.contains("sqlite/S1/P1/PROCESS_CRASH/" + depth + "/" + kind));

        for (String p : List.of("P1", "P3", "P5", "P7"))
            for (String d : List.of("POWER_LOSS", "PROCESS_CRASH"))
                assertTrue(k.contains("sqlite/S1/" + p + "/" + d + "/empty"), p + d);

        assertTrue(k.contains("sqlite/S1/P0/POWER_LOSS/empty"));

        assertTrue(k.contains("sqlite/S1/P1/POWER_LOSS/1m"));
        assertTrue(k.contains("sqlite/S1/P1/PROCESS_CRASH/1m"));
        assertFalse(k.contains("sqlite/S1/P3/POWER_LOSS/1m"));

        assertTrue(k.contains("sqlite/S2/P7/POWER_LOSS/empty"));
        assertTrue(k.contains("sqlite/S2/P5/POWER_LOSS/empty"));

        for (String p : List.of("P0", "P2", "P4", "P6")) {
            assertTrue(k.contains("sqlite/S1/" + p + "/POWER_LOSS/empty"), p);
            assertTrue(k.contains("sqlite/S1/" + p + "/PROCESS_CRASH/empty"), p);
            assertTrue(k.contains("sqlite/S2/" + p + "/POWER_LOSS/empty"), p);
        }

        assertFalse(k.contains("sqlite/S1/P5/POWER_LOSS/1m"));

        assertTrue(k.contains("sqlite/S3/P1/POWER_LOSS/empty"));
        assertEquals(1, k.stream().filter(s -> s.contains("/S3/")).count());
        assertTrue(plan.stream().filter(s -> s.depth() > 0).allMatch(s -> s.depth() == 1_000_000));
    }

    @Test
    void repeatedRoundsRunTheWholePlanThenOnlyTheRatioConditions() {
        List<RunSpec> plan = Plan.full(Storage.SQLITE);
        List<List<RunSpec>> rounds = Plan.rounds(plan, 3);
        assertEquals(3, rounds.size());
        assertEquals(plan, rounds.get(0));
        List<RunSpec> ratio = rounds.get(1);
        assertEquals(ratio, rounds.get(2));
        assertEquals(10, ratio.size(), "depth의 분자와 분모가 되는 S1 조건은 P1 빈 큐와 100만 건 x 두 내구성");
        assertTrue(ratio.stream().allMatch(s -> s.scenario() == Scenario.S1));
        Set<String> k = keys(ratio);
        assertTrue(k.contains("sqlite/S1/P1/POWER_LOSS/1m"));
        assertTrue(k.contains("sqlite/S1/P1/POWER_LOSS/empty"));
        assertTrue(k.contains("sqlite/S1/P1/PROCESS_CRASH/1m"));
        assertTrue(k.contains("sqlite/S1/P1/PROCESS_CRASH/empty"));
        assertFalse(k.contains("sqlite/S1/P0/POWER_LOSS/empty"));
        assertFalse(k.contains("sqlite/S1/P3/POWER_LOSS/empty"));
        assertFalse(k.contains("sqlite/S1/P3/POWER_LOSS/1m"));
    }

    @Test
    void oneRepeatIsTheCurrentSingleRound() {
        List<RunSpec> plan = Plan.full(Storage.SQLITE);
        assertEquals(List.of(plan), Plan.rounds(plan, 1));
    }

    @Test
    void repeatCountMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> Plan.rounds(Plan.full(Storage.SQLITE), 0));
    }

    @Test
    void memoryPlanHasNoRatioConditionsSoItRunsOnce() {
        List<RunSpec> plan = Plan.full(Storage.MEMORY);
        assertEquals(List.of(plan), Plan.rounds(plan, 3));
    }

    @Test
    void fullMemoryPlanIsSingleJvmDiagnostics() {
        List<RunSpec> plan = Plan.full(Storage.MEMORY);
        assertTrue(plan.stream().allMatch(s -> s.storage() == Storage.MEMORY));
        assertTrue(keys(plan).contains("memory/S1/P1/none/empty"));
        assertTrue(keys(plan).contains("memory/S3/P1/none/empty"));
    }

    @Test
    void smokeIsShortAndSmall() {
        List<RunSpec> plan = Plan.smoke(Storage.SQLITE);
        assertTrue(plan.size() >= 4 && plan.size() <= 8);
        assertTrue(plan.stream().allMatch(s -> s.durationMs() <= 3000 && s.depth() <= 10_000));
    }

    @Test
    void newKindsRoundTripAndOldJsonDefaultsToStandardQueue() {
        for (RunSpec.QueueKind kind : List.of(
                RunSpec.QueueKind.MAX_DELIVERIES,
                RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER,
                RunSpec.QueueKind.LEASED_HEAD)) {
            RunSpec s = RunSpec.s1(kind, 5000, 10, 1);
            assertEquals(s, RunSpec.fromArg(s.toArg()));
            assertTrue(keys(Plan.smoke(Storage.SQLITE)).contains(s.key()));
        }
        RunSpec old = RunSpec.s1(Storage.SQLITE, Profile.P1, Durability.PROCESS_CRASH, 0, 10, 1);
        assertEquals(old, RunSpec.fromJson(old.toJson().replace(",\"queueKind\":\"DEFAULT\"", "")));
    }

    @Test
    void profilesMatchTheSpec() {
        assertEquals(256, Profile.P0.payloadBytes());
        assertEquals(1, Profile.P0.chunk());
        assertEquals(100, Profile.P1.chunk());
        assertEquals(2048, Profile.P3.payloadBytes());
        assertEquals(300, Profile.P3.chunk());
        assertEquals(6144, Profile.P7.payloadBytes());
        assertEquals(500, Profile.P7.chunk());
        assertEquals(4096, Profile.P5.payloadBytes());
        assertEquals(400, Profile.P5.chunk());

        assertEquals(Profile.P3.payloadBytes(), Profile.P2.payloadBytes());
        assertEquals(Profile.P7.payloadBytes(), Profile.P6.payloadBytes());
        assertEquals(Profile.P5.payloadBytes(), Profile.P4.payloadBytes());
        assertEquals(Profile.P1.payloadBytes(), Profile.P0.payloadBytes());
        assertEquals(1, Profile.P2.chunk());
        assertEquals(1, Profile.P6.chunk());
        assertEquals(1, Profile.P4.chunk());
    }

    @Test
    void specTravelsToChildAsOneQuoteFreeArgument() {
        RunSpec s = RunSpec.s1(Storage.SQLITE, Profile.P5, Durability.PROCESS_CRASH, 0, 5000, 700);
        String arg = s.toArg();

        assertFalse(arg.contains("\"") || arg.contains(" "), arg);
        assertEquals(s, RunSpec.fromArg(arg));
    }

    @Test
    void specRoundTripsThroughJson() {
        RunSpec s = RunSpec.s1(Storage.SQLITE, Profile.P7, Durability.PROCESS_CRASH, 1_000_000, 5000, 700);
        RunSpec back = RunSpec.fromJson(s.toJson());
        assertEquals(s, back);
        RunSpec m = RunSpec.s3(Storage.MEMORY, Profile.P1, 10, 1);
        assertEquals(m, RunSpec.fromJson(m.toJson()));
    }
}
