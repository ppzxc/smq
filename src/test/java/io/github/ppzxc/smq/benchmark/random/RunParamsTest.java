package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RunParamsTest {

    private static final Limits FULL = Limits.full(10_000);

    @Test
    void sameSeedProducesSameParameters() {
        assertEquals(
                RunParams.fromSeed(42, FULL),
                RunParams.fromSeed(42, FULL),
                "From seed should match the expected value.");
    }

    @Test
    void differentSeedsProduceDifferentParameters() {
        assertNotEquals(
                RunParams.fromSeed(42, FULL),
                RunParams.fromSeed(43, FULL),
                "From seed should differ from the unexpected value.");
    }

    private static Arguments parameterRange(final String name, final Predicate<RunParams> valid) {
        return Arguments.of(name, valid);
    }

    private static Stream<Arguments> parameterRanges() {
        return Stream.of(
                parameterRange("message bytes", p -> p.maxMessageBytes() >= 0 && p.maxMessageBytes() <= 65_536),
                parameterRange("publish batch", p -> p.publishBatch() >= 1 && p.publishBatch() <= 500),
                parameterRange("chunk size", p -> p.chunkSize() >= 1 && p.chunkSize() <= 500),
                parameterRange("producers", p -> p.producers() >= 1 && p.producers() <= 4),
                parameterRange("consumers", p -> p.consumers() >= 1 && p.consumers() <= 4),
                parameterRange(
                        "prefill depth",
                        p -> p.prefillDepth() == 0 || (p.prefillDepth() >= 1 && p.prefillDepth() <= 100_000)),
                parameterRange("ACK ratio", p -> p.ackRatio() > 0.5),
                parameterRange(
                        "settlement ratios",
                        p -> Math.abs(1.0 - (p.ackRatio() + p.nackRatio() + p.rejectRatio() + p.noneRatio())) <= 1e-9),
                parameterRange("lease duration", p -> p.leaseMillis() >= 200 && p.leaseMillis() <= 10_000),
                parameterRange("publish delay ratio", p -> p.publishDelayRatio() >= 0 && p.publishDelayRatio() <= 0.3),
                parameterRange("NACK delay ratio", p -> p.nackDelayRatio() >= 0 && p.nackDelayRatio() <= 0.3),
                parameterRange(
                        "delay limits", p -> p.publishDelayMaxMillis() <= 2_000 && p.nackDelayMaxMillis() <= 2_000),
                parameterRange("max deliveries", p -> p.maxDeliveries() >= 0 && p.maxDeliveries() <= 5),
                parameterRange("pull wait", p -> p.pullWaitMillis() >= 0 && p.pullWaitMillis() <= 500),
                parameterRange("slow chunk ratio", p -> p.slowChunkRatio() <= 0.3));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameterRanges")
    void everySeedStaysWithinParameterRange(final String name, final Predicate<RunParams> valid) {
        for (long seed = 0; seed < 3_000; seed++) {
            assertTrue(valid.test(RunParams.fromSeed(seed, FULL)), name + " should be valid for seed " + seed);
        }
    }

    private record SeedCoverage(Set<Durability> durabilities, int single, int withMax, int withPrefill) {}

    private static SeedCoverage seedCoverage() {
        int single;
        int withMax;
        int withPrefill;
        final Set<Durability> durabilities;
        single = 0;
        withMax = 0;
        withPrefill = 0;
        durabilities = EnumSet.noneOf(Durability.class);
        for (long seed = 0; seed < 3_000; seed++) {
            final RunParams params;
            params = RunParams.fromSeed(seed, FULL);
            durabilities.add(params.durability());
            single += params.singleActiveConsumer() ? 1 : 0;
            withMax += params.maxDeliveries() > 0 ? 1 : 0;
            withPrefill += params.prefillDepth() > 0 ? 1 : 0;
        }
        return new SeedCoverage(durabilities, single, withMax, withPrefill);
    }

    @Test
    void seedsCoverAllDurabilities() {
        assertEquals(
                EnumSet.allOf(Durability.class),
                seedCoverage().durabilities(),
                "Seeds should cover all durability levels.");
    }

    @Test
    void seedsSelectSingleActiveConsumerAtExpectedFrequency() {
        final int single;
        single = seedCoverage().single();
        assertTrue(single > 450 && single < 750, "Single-active selection should be close to 20%: " + single);
    }

    @Test
    void seedsSelectMaximumDeliveriesAtExpectedFrequency() {
        final int withMax;
        withMax = seedCoverage().withMax();
        assertTrue(
                withMax > 1_200 && withMax < 1_800,
                "Maximum deliveries should be selected about half the time: " + withMax);
    }

    @Test
    void seedsCoverPrefilledAndEmptyQueues() {
        final int withPrefill;
        withPrefill = seedCoverage().withPrefill();
        assertTrue(withPrefill > 0 && withPrefill < 3_000, "Seeds should include both prefilled and empty queues.");
    }

    private static long fixedRateThreadShare(final RunParams params) {
        return Math.max(1, (500 + params.producers() - 1) / params.producers());
    }

    private static long fixedRateMinimumBatch(final RunParams params) {
        return Math.min(RunParams.SOAK_MIN_PUBLISH_BATCH, fixedRateThreadShare(params));
    }

    private static Stream<Arguments> fixedRateParameterRanges() {
        return Stream.of(
                parameterRange("target rate", p -> p.targetRate() == 500),
                parameterRange("publish delay", p -> p.publishDelayRatio() == 0),
                parameterRange("slow chunks", p -> p.slowChunkRatio() == 0),
                parameterRange("NACK delay", p -> p.nackDelayRatio() == 0),
                parameterRange("single consumer", p -> !p.singleActiveConsumer()),
                parameterRange("chunk size", p -> p.chunkSize() >= RunParams.SOAK_MIN_CHUNK_SIZE),
                parameterRange("prefill depth", p -> p.prefillDepth() == FULL.maxDepth()),
                parameterRange("message bytes", p -> p.maxMessageBytes() <= 1024),
                parameterRange("minimum batch", p -> p.publishBatch() >= fixedRateMinimumBatch(p)),
                parameterRange(
                        "maximum batch",
                        p -> p.publishBatch()
                                <= Math.max(
                                        fixedRateMinimumBatch(p),
                                        fixedRateThreadShare(p) / RunParams.SOAK_BATCH_WINDOW_DIVISOR)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixedRateParameterRanges")
    @DisplayName("고정 속도 모드는 운영에 가까운 매개변수를 사용한다")
    void fixedRateParametersMatchSpec(final String name, final Predicate<RunParams> valid) {
        final Limits soak;
        soak = FULL.withRate(500);
        for (long seed = 0; seed < 300; seed++) {
            assertTrue(valid.test(RunParams.fromSeed(seed, soak)), name + " should be valid for seed " + seed);
        }
    }

    @Test
    @DisplayName("속도가 없으면 기존 매개변수와 같고 목표 속도는 0이다")
    void ifThereIsNoSpeedItIsTheSameAsTheExistingParametersAndTheTargetSpeedIs0() {
        for (long seed = 0; seed < 50; seed++) {
            assertEquals(
                    0, RunParams.fromSeed(seed, FULL).targetRate(), "Target rate should match the expected value.");
        }
    }

    @Test
    @DisplayName("같은 시드와 속도는 같은 매개변수다")
    void theSameSeedAndSpeedAreTheSameParameters() {
        assertEquals(
                RunParams.fromSeed(7, FULL.withRate(500)),
                RunParams.fromSeed(7, FULL.withRate(500)),
                "From seed should match the expected value.");
    }

    private static Stream<Arguments> smokeParameterRanges() {
        return Stream.of(
                parameterRange("prefill depth", p -> p.prefillDepth() <= 2_000),
                parameterRange("lease duration", p -> p.leaseMillis() <= 1_000),
                parameterRange("load duration", p -> p.loadMillis() == 2_000));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smokeParameterRanges")
    void smokeLimitsBoundParameters(final String name, final Predicate<RunParams> valid) {
        for (long seed = 0; seed < 500; seed++) {
            assertTrue(
                    valid.test(RunParams.fromSeed(seed, Limits.smoke(2_000))),
                    name + " should be valid for seed " + seed);
        }
    }

    @Test
    void prefillIsBoundedByEstimatedBytes() {
        for (long seed = 0; seed < 3_000; seed++) {
            final RunParams p;
            p = RunParams.fromSeed(seed, FULL);
            assertTrue(
                    (long) p.prefillDepth() * (p.maxMessageBytes() / 4 + 64) <= 256L * 1024 * 1024,
                    "Prefill depth should stay within the configured limit.");
        }
    }

    @Test
    void descriptionIncludesSeed() {
        final String text;
        text = RunParams.fromSeed(7, FULL).describe();
        assertTrue(text.contains("seed = 7"), "Text should contain the expected value.");
    }

    @Test
    void descriptionIncludesLeaseDuration() {
        final String text;
        text = RunParams.fromSeed(7, FULL).describe();
        assertTrue(text.contains("leaseMillis = "), "Text should contain the expected value.");
    }

    @Test
    void descriptionIncludesSingleConsumerSetting() {
        final String text;
        text = RunParams.fromSeed(7, FULL).describe();
        assertTrue(text.contains("singleActiveConsumer = "), "Text should contain the expected value.");
    }

    private static Stream<Arguments> smokeSeedAxes() {
        return Stream.of(
                parameterRange("single active consumer", RunParams::singleActiveConsumer),
                parameterRange("max deliveries", p -> p.maxDeliveries() > 0),
                parameterRange("reject", p -> p.rejectRatio() > 0),
                parameterRange("publish delay", p -> p.publishDelayRatio() > 0 && p.publishDelayMaxMillis() > 0),
                parameterRange("NACK delay", p -> p.nackDelayRatio() > 0 && p.nackDelayMaxMillis() > 0),
                parameterRange("prefill", p -> p.prefillDepth() > 0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("smokeSeedAxes")
    void smokeSeedsCoverAxis(final String name, final Predicate<RunParams> matches) {
        assertTrue(
                Options.SMOKE_SEEDS.stream()
                        .map(seed -> RunParams.fromSeed(seed, Limits.smoke(Options.SMOKE_LOAD.toMillis())))
                        .anyMatch(matches),
                "Smoke seeds should cover " + name + ".");
    }

    @Test
    void smokeSeedsCoverAllDurabilities() {
        final Set<Durability> durabilities;
        durabilities = EnumSet.noneOf(Durability.class);
        for (final long seed : Options.SMOKE_SEEDS) {
            durabilities.add(RunParams.fromSeed(seed, Limits.smoke(Options.SMOKE_LOAD.toMillis()))
                    .durability());
        }
        assertEquals(EnumSet.allOf(Durability.class), durabilities, "Smoke seeds should cover all durability levels.");
    }

    private static Arguments fixedRateCase(final String name, final Limits limits, final Predicate<RunParams> valid) {
        return Arguments.of(name, limits, valid);
    }

    private static Stream<Arguments> fixedRateOptionCases() {
        return Stream.of(
                fixedRateCase("default maximum bytes", FULL.withRate(500), p -> p.maxMessageBytes() == 1024),
                fixedRateCase("default minimum bytes", FULL.withRate(500), p -> p.minMessageBytes() == 512),
                fixedRateCase("default consumer pacing", FULL.withRate(500), p -> !p.consumerPaced()),
                fixedRateCase(
                        "configured maximum bytes",
                        FULL.withRate(500).withMessageBytes(4096),
                        p -> p.maxMessageBytes() == 4096),
                fixedRateCase(
                        "configured minimum bytes",
                        FULL.withRate(500).withMessageBytes(4096),
                        p -> p.minMessageBytes() == 2048),
                fixedRateCase(
                        "configured depth", FULL.withRate(500).withDepth(300_000), p -> p.prefillDepth() == 300_000),
                fixedRateCase(
                        "standing queue pacing", FULL.withRate(500).withStandingQueue(true), RunParams::consumerPaced),
                fixedRateCase(
                        "random mode pacing",
                        FULL.withStandingQueue(true).withMessageBytes(4096),
                        p -> !p.consumerPaced()),
                fixedRateCase(
                        "random mode minimum bytes",
                        FULL.withStandingQueue(true).withMessageBytes(4096),
                        p -> p.minMessageBytes() == 0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixedRateOptionCases")
    @DisplayName("메시지 크기와 깊이와 깊은 큐 옵션은 고정 속도 모드에서만 먹는다")
    void fixedRateOptionsMatchSpec(final String name, final Limits limits, final Predicate<RunParams> valid) {
        assertTrue(valid.test(RunParams.fromSeed(7, limits)), name + " should match the specification.");
    }

    private static RunParams largePrefillParameters() {
        return RunParams.fromSeed(7, FULL.withRate(500).withMessageBytes(60_000).withDepth(5_000_000));
    }

    @Test
    @DisplayName("큰 메시지로 깊이를 늘려도 미리 채우기는 데이터 상한을 넘지 않는다")
    void largePrefillStaysWithinByteLimit() {
        final RunParams params;
        final long average;
        params = largePrefillParameters();
        average = RunParams.avgMessageBytes(params.minMessageBytes(), params.maxMessageBytes());
        assertTrue(
                params.prefillDepth() * average <= 256L * 1024 * 1024,
                "Estimated prefill bytes should stay within the limit.");
    }

    @Test
    @DisplayName("큰 메시지의 미리 채우기 깊이는 양수다")
    void largePrefillRemainsPositive() {
        assertTrue(largePrefillParameters().prefillDepth() > 0, "Prefill depth should remain positive.");
    }

    private static RunParams oneByteParameters() {
        return RunParams.fromSeed(7, FULL.withRate(500).withMessageBytes(1));
    }

    @Test
    @DisplayName("메시지 크기 1은 상한이 1이다")
    void oneByteMessageHasMaximumSizeOne() {
        assertEquals(1, oneByteParameters().maxMessageBytes(), "The maximum message size should be one byte.");
    }

    @Test
    @DisplayName("메시지 크기 1은 하한이 1이다")
    void oneByteMessageHasMinimumSizeOne() {
        assertEquals(1, oneByteParameters().minMessageBytes(), "The minimum message size should be one byte.");
    }

    @Test
    @DisplayName("메시지 크기 1은 로그 균등 분포로 돌아가지 않는다")
    void oneByteMessagesStayOneByteAcrossSequences() {
        final Script script;
        script = new Script(oneByteParameters());
        for (long seq = 0; seq < 50; seq++) {
            assertEquals(
                    1,
                    script.message(new Tag(7, seq)).message().payload().length,
                    "The message should contain one byte.");
        }
    }

    @Test
    @DisplayName("지정한 내구성은 시드 선택을 덮어쓴다")
    void forcedDurabilityOverridesSeedSelection() {
        for (final Durability forced : Durability.values()) {
            for (long seed = 0; seed < 200; seed++) {
                assertEquals(
                        forced,
                        RunParams.fromSeed(seed, FULL.withDurability(forced)).durability(),
                        "Forced durability should override seed " + seed);
            }
        }
    }

    @Test
    @DisplayName("내구성을 지정해도 나머지 매개변수는 그대로다")
    void forcedDurabilityPreservesOtherSeedParameters() {
        for (final Durability forced : Durability.values()) {
            for (long seed = 0; seed < 200; seed++) {
                final Map<String, Object> expected;
                final Map<String, Object> actual;
                expected = new LinkedHashMap<>(RunParams.fromSeed(seed, FULL).toMap());
                actual = new LinkedHashMap<>(
                        RunParams.fromSeed(seed, FULL.withDurability(forced)).toMap());
                expected.remove("durability");
                actual.remove("durability");
                assertEquals(expected, actual, "Other parameters should remain unchanged for seed " + seed);
            }
        }
    }

    @Test
    @DisplayName("고정 속도 모드에서도 지정한 내구성을 사용한다")
    void forcedDurabilityAppliesInFixedRateMode() {
        assertEquals(
                Durability.POWER_LOSS,
                RunParams.fromSeed(7, FULL.withRate(500).withDurability(Durability.POWER_LOSS))
                        .durability(),
                "Fixed-rate mode should use the forced durability.");
    }

    @Test
    @DisplayName("내구성을 주지 않으면 두 수준이 모두 나온다")
    void ifDurabilityIsNotProvidedBothLevelsAppear() {
        final Set<Durability> seen;
        seen = EnumSet.noneOf(Durability.class);
        for (long seed = 0; seed < 50; seed++) {
            seen.add(RunParams.fromSeed(seed, FULL).durability());
        }
        assertEquals(EnumSet.allOf(Durability.class), seen, "Seen should match the expected value.");
    }
}
