package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PlanTest {

    private static Set<String> keys(final List<RunSpec> plan) {
        return plan.stream().map(RunSpec::key).collect(Collectors.toSet());
    }

    private static Stream<String> requiredFullSqliteKeys() {
        final List<String> expected;
        expected = new ArrayList<>();
        for (final String kind : List.of("MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD")) {
            for (final String depth : List.of("empty", "1m")) {
                expected.add("sqlite/S1/P1/PROCESS_CRASH/" + depth + "/" + kind);
            }
        }
        for (final String profile : List.of("P1", "P3", "P5", "P7")) {
            for (final String durability : List.of("POWER_LOSS", "PROCESS_CRASH")) {
                expected.add("sqlite/S1/" + profile + "/" + durability + "/empty");
            }
        }
        for (final String profile : List.of("P0", "P2", "P4", "P6")) {
            expected.add("sqlite/S1/" + profile + "/POWER_LOSS/empty");
            expected.add("sqlite/S1/" + profile + "/PROCESS_CRASH/empty");
            expected.add("sqlite/S2/" + profile + "/POWER_LOSS/empty");
        }
        expected.addAll(List.of(
                "sqlite/S1/P1/POWER_LOSS/1m",
                "sqlite/S1/P1/PROCESS_CRASH/1m",
                "sqlite/S2/P7/POWER_LOSS/empty",
                "sqlite/S2/P5/POWER_LOSS/empty",
                "sqlite/S3/P1/POWER_LOSS/empty"));
        return expected.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("requiredFullSqliteKeys")
    void fullSqlitePlanContainsRequiredCase(final String key) {
        assertTrue(keys(Plan.full(Storage.SQLITE)).contains(key), "The full SQLite plan should include " + key + ".");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"sqlite/S1/P3/POWER_LOSS/1m", "sqlite/S1/P5/POWER_LOSS/1m"})
    void fullSqlitePlanExcludesUnsupportedDeepCase(final String key) {
        assertFalse(keys(Plan.full(Storage.SQLITE)).contains(key), "The full SQLite plan should exclude " + key + ".");
    }

    @Test
    void fullSqlitePlanHasUniqueKeys() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.SQLITE);
        assertEquals(plan.size(), keys(plan).size(), "Every plan case should have a unique key.");
    }

    @Test
    void fullSqlitePlanHasExpectedSize() {
        assertEquals(33, Plan.full(Storage.SQLITE).size(), "The full SQLite plan should contain 33 cases.");
    }

    @Test
    void fullSqlitePlanHasOneLatencyCase() {
        assertEquals(
                1,
                keys(Plan.full(Storage.SQLITE)).stream()
                        .filter(key -> key.contains("/S3/"))
                        .count(),
                "The full SQLite plan should contain one latency case.");
    }

    @Test
    void fullSqlitePlanUsesExpectedDeepQueueSize() {
        assertTrue(
                Plan.full(Storage.SQLITE).stream()
                        .filter(spec -> spec.depth() > 0)
                        .allMatch(spec -> spec.depth() == 1_000_000),
                "Every deep queue should contain one million messages.");
    }

    private static List<List<RunSpec>> repeatedSqliteRounds() {
        return Plan.rounds(Plan.full(Storage.SQLITE), 3);
    }

    @Test
    void repeatedSqlitePlanHasThreeRounds() {
        assertEquals(3, repeatedSqliteRounds().size(), "The plan should contain three rounds.");
    }

    @Test
    void firstRepeatedRoundUsesTheFullPlan() {
        assertEquals(
                Plan.full(Storage.SQLITE), repeatedSqliteRounds().get(0), "The first round should use the full plan.");
    }

    @Test
    void laterRepeatedRoundsUseTheSameCases() {
        assertEquals(
                repeatedSqliteRounds().get(1),
                repeatedSqliteRounds().get(2),
                "Later rounds should use the same cases.");
    }

    @Test
    void laterRepeatedRoundHasTenCases() {
        assertEquals(10, repeatedSqliteRounds().get(1).size(), "The ratio round should contain ten cases.");
    }

    @Test
    void laterRepeatedRoundUsesS1Cases() {
        assertTrue(
                repeatedSqliteRounds().get(1).stream().allMatch(spec -> spec.scenario() == Scenario.S1),
                "The ratio round should contain only S1 cases.");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {
                "sqlite/S1/P1/POWER_LOSS/1m", "sqlite/S1/P1/POWER_LOSS/empty",
                "sqlite/S1/P1/PROCESS_CRASH/1m", "sqlite/S1/P1/PROCESS_CRASH/empty"
            })
    void ratioRoundContainsRequiredCase(final String key) {
        assertTrue(keys(repeatedSqliteRounds().get(1)).contains(key), "The ratio round should include " + key + ".");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {"sqlite/S1/P0/POWER_LOSS/empty", "sqlite/S1/P3/POWER_LOSS/empty", "sqlite/S1/P3/POWER_LOSS/1m"})
    void ratioRoundExcludesUnrelatedCase(final String key) {
        assertFalse(keys(repeatedSqliteRounds().get(1)).contains(key), "The ratio round should exclude " + key + ".");
    }

    @Test
    void oneRepeatIsTheCurrentSingleRound() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.SQLITE);
        assertEquals(List.of(plan), Plan.rounds(plan, 1), "Rounds should match the expected value.");
    }

    @Test
    void repeatCountMustBePositive() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Plan.rounds(Plan.full(Storage.SQLITE), 0),
                "Rounds should throw IllegalArgumentException.");
    }

    @Test
    void memoryPlanHasNoRatioConditionsSoItRunsOnce() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.MEMORY);
        assertEquals(List.of(plan), Plan.rounds(plan, 3), "Rounds should match the expected value.");
    }

    @Test
    void memoryPlanUsesMemoryStorage() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.MEMORY);
        assertTrue(
                plan.stream().allMatch(s -> s.storage() == Storage.MEMORY),
                "Every plan entry should satisfy the selected profile.");
    }

    @Test
    void memoryPlanIncludesThroughputCase() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.MEMORY);
        assertTrue(keys(plan).contains("memory/S1/P1/none/empty"), "Keys should contain the expected value.");
    }

    @Test
    void memoryPlanIncludesLatencyCase() {
        final List<RunSpec> plan;
        plan = Plan.full(Storage.MEMORY);
        assertTrue(keys(plan).contains("memory/S3/P1/none/empty"), "Keys should contain the expected value.");
    }

    @Test
    void smokePlanHasExpectedCaseCount() {
        final List<RunSpec> plan;
        plan = Plan.smoke(Storage.SQLITE);
        assertTrue(plan.size() >= 4 && plan.size() <= 8, "The smoke plan should contain four to eight cases.");
    }

    @Test
    void smokePlanLimitsDurationAndDepth() {
        final List<RunSpec> plan;
        plan = Plan.smoke(Storage.SQLITE);
        assertTrue(
                plan.stream().allMatch(s -> s.durationMs() <= 3000 && s.depth() <= 10_000),
                "Every plan entry should satisfy the selected profile.");
    }

    @ParameterizedTest
    @EnumSource(
            value = RunSpec.QueueKind.class,
            names = {"MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD"})
    void queueKindRoundTripsThroughArgument(final RunSpec.QueueKind kind) {
        final RunSpec spec;
        spec = RunSpec.s1(kind, 5000, 10, 1);
        assertEquals(spec, RunSpec.fromArg(spec.toArg()), "The queue kind should survive argument encoding.");
    }

    @ParameterizedTest
    @EnumSource(
            value = RunSpec.QueueKind.class,
            names = {"MAX_DELIVERIES", "SINGLE_ACTIVE_CONSUMER", "LEASED_HEAD"})
    void queueKindAppearsInSmokePlan(final RunSpec.QueueKind kind) {
        final RunSpec spec;
        spec = RunSpec.s1(kind, 5000, 10, 1);
        assertTrue(
                keys(Plan.smoke(Storage.SQLITE)).contains(spec.key()), "The smoke plan should include the queue kind.");
    }

    @Test
    void oldJsonDefaultsToStandardQueue() {
        final RunSpec old;
        old = RunSpec.s1(Storage.SQLITE, Profile.P1, Durability.PROCESS_CRASH, 0, 10, 1);
        assertEquals(
                old,
                RunSpec.fromJson(old.toJson().replace(",\"queueKind\":\"DEFAULT\"", "")),
                "Old JSON should default to the standard queue.");
    }

    @ParameterizedTest
    @CsvSource({"P0, 256", "P1, 256", "P2, 2048", "P3, 2048", "P4, 4096", "P5, 4096", "P6, 6144", "P7, 6144"})
    void profilePayloadSizeMatchesSpec(final Profile profile, final int expectedBytes) {
        assertEquals(expectedBytes, profile.payloadBytes(), "The profile payload size should match the specification.");
    }

    @ParameterizedTest
    @CsvSource({"P0, 1", "P1, 100", "P2, 1", "P3, 300", "P4, 1", "P5, 400", "P6, 1", "P7, 500"})
    void profileChunkSizeMatchesSpec(final Profile profile, final int expectedChunkSize) {
        assertEquals(expectedChunkSize, profile.chunk(), "The profile chunk size should match the specification.");
    }

    @Test
    void specArgumentContainsNoQuotesOrSpaces() {
        final RunSpec s;
        final String arg;
        s = RunSpec.s1(Storage.SQLITE, Profile.P5, Durability.PROCESS_CRASH, 0, 5000, 700);
        arg = s.toArg();
        assertFalse(arg.contains("\"") || arg.contains(" "), arg);
    }

    @Test
    void specArgumentRoundTrips() {
        final RunSpec s;
        final String arg;
        s = RunSpec.s1(Storage.SQLITE, Profile.P5, Durability.PROCESS_CRASH, 0, 5000, 700);
        arg = s.toArg();
        assertEquals(s, RunSpec.fromArg(arg), "From arg should match the expected value.");
    }

    @Test
    void sqliteSpecRoundTripsThroughJson() {
        final RunSpec s;
        final RunSpec back;
        s = RunSpec.s1(Storage.SQLITE, Profile.P7, Durability.PROCESS_CRASH, 1_000_000, 5000, 700);
        back = RunSpec.fromJson(s.toJson());
        assertEquals(s, back, "Back should match the expected value.");
    }

    @Test
    void memorySpecRoundTripsThroughJson() {
        final RunSpec m;
        m = RunSpec.s3(Storage.MEMORY, Profile.P1, 10, 1);
        assertEquals(m, RunSpec.fromJson(m.toJson()), "From json should match the expected value.");
    }
}
