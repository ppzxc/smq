package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OptionsTest {

    @Test
    void defaultOptionsHaveNoSeed() {
        final Options o;
        o = Options.parse(new String[0], 1234);
        assertTrue(o.seed().isEmpty(), "Seed should be empty.");
    }

    @Test
    void defaultBudgetIsThirtyMinutes() {
        final Options o;
        o = Options.parse(new String[0], 1234);
        assertEquals(Duration.ofMinutes(30), o.budget(), "Budget should match the expected value.");
    }

    @Test
    void defaultMasterSeedIsPreserved() {
        final Options o;
        o = Options.parse(new String[0], 1234);
        assertEquals(1234, o.masterSeed(), "Master seed should match the expected value.");
    }

    @Test
    void defaultLoadIsTenSeconds() {
        final Options o;
        o = Options.parse(new String[0], 1234);
        assertEquals(Duration.ofSeconds(10), o.load(), "Load should match the expected value.");
    }

    @Test
    void smokeModeIsDisabledByDefault() {
        final Options o;
        o = Options.parse(new String[0], 1234);
        assertFalse(o.smoke(), "Smoke mode should be disabled.");
    }

    @Test
    void seedOptionIsParsed() {
        final Options o;
        o = Options.parse(new String[] {"--seed", "-42", "--budget", "90s", "--load", "3s"}, 0);
        assertEquals(-42, o.seed().getAsLong(), "As long should match the expected value.");
    }

    @Test
    void budgetOptionIsParsed() {
        final Options o;
        o = Options.parse(new String[] {"--seed", "-42", "--budget", "90s", "--load", "3s"}, 0);
        assertEquals(Duration.ofSeconds(90), o.budget(), "Budget should match the expected value.");
    }

    @Test
    void loadOptionIsParsed() {
        final Options o;
        o = Options.parse(new String[] {"--seed", "-42", "--budget", "90s", "--load", "3s"}, 0);
        assertEquals(Duration.ofSeconds(3), o.load(), "Load should match the expected value.");
    }

    @Test
    void durationParsesMinutes() {
        assertEquals(
                Duration.ofMinutes(30),
                Options.parseDuration("30m"),
                "Parse duration should match the expected value.");
    }

    @Test
    void durationParsesHours() {
        assertEquals(
                Duration.ofHours(2), Options.parseDuration("2h"), "Parse duration should match the expected value.");
    }

    @Test
    void durationParsesSeconds() {
        assertEquals(
                Duration.ofSeconds(45), Options.parseDuration("45"), "Parse duration should match the expected value.");
    }

    @Test
    void durationParsesMilliseconds() {
        assertEquals(
                Duration.ofMillis(1500),
                Options.parseDuration("1500ms"),
                "Parse duration should match the expected value.");
    }

    @Test
    void durationParsesIsoFormat() {
        assertEquals(
                Duration.ofMinutes(5),
                Options.parseDuration("PT5M"),
                "Parse duration should match the expected value.");
    }

    @Test
    void durationRejectsInvalidText() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parseDuration("soon"),
                "Parse duration should throw IllegalArgumentException.");
    }

    @Test
    void smokeOptionEnablesSmokeMode() {
        assertTrue(Options.parse(new String[] {"--smoke"}, 0).smoke(), "Smoke mode should be enabled.");
    }

    @Test
    void smokeOptionUsesTwoSecondLoad() {
        assertEquals(
                Duration.ofSeconds(2),
                Options.parse(new String[] {"--smoke"}, 0).load(),
                "The smoke load should last two seconds.");
    }

    @Test
    void smokeOptionRejectsSeed() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--smoke", "--seed", "1"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    void optionsRejectMissingSeed() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--seed"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    void optionsRejectInvalidSeed() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--seed", "x"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    void optionsRejectUnknownOption() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--nope", "1"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 옵션은 기본이 0이고 양의 정수를 받는다 — rateDefaultsToZero")
    void rateDefaultsToZero() {
        assertEquals(0, Options.parse(new String[0], 0).rate(), "Rate should match the expected value.");
    }

    @Test
    @DisplayName("속도 옵션은 기본이 0이고 양의 정수를 받는다 — rateAcceptsPositiveInteger")
    void rateAcceptsPositiveInteger() {
        assertEquals(
                500, Options.parse(new String[] {"--rate", "500"}, 0).rate(), "Rate should match the expected value.");
    }

    @Test
    @DisplayName("속도 옵션은 기본이 0이고 양의 정수를 받는다 — rateRejectsZero")
    void rateRejectsZero() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "0"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 옵션은 기본이 0이고 양의 정수를 받는다 — rateRejectsNegativeValue")
    void rateRejectsNegativeValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "-5"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 옵션은 기본이 0이고 양의 정수를 받는다 — rateRejectsNonNumericValue")
    void rateRejectsNonNumericValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "fast"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 옵션과 스모크는 함께 쓸 수 없다")
    void theSpeedOptionAndSmokeCannotBeUsedTogether() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--smoke", "--rate", "100"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 옵션이 있으면 JFR이 기본으로 켜진다 — rate option")
    void rateOptionEnablesJfrByDefault() {
        assertTrue(Options.parse(new String[] {"--rate", "500"}, 0).jfr(), "JFR should be enabled.");
    }

    @Test
    @DisplayName("속도 옵션이 있으면 JFR이 기본으로 켜진다 — no rate option")
    void jfrIsDisabledByDefaultWithoutRate() {
        assertFalse(Options.parse(new String[0], 0).jfr(), "JFR should be disabled.");
    }

    private static Options fixedRateOverrides() {
        return Options.parse(
                new String[] {"--rate", "500", "--message-bytes", "2048", "--depth", "300000", "--standing-queue"}, 0);
    }

    private static Options fixedRateDefaults() {
        return Options.parse(new String[] {"--rate", "500"}, 0);
    }

    @Test
    void fixedRateParsesMessageBytes() {
        assertEquals(2048, fixedRateOverrides().messageBytes(), "The configured message size should be parsed.");
    }

    @Test
    void fixedRateParsesDepth() {
        assertEquals(300_000, fixedRateOverrides().depth(), "The configured depth should be parsed.");
    }

    @Test
    void fixedRateParsesStandingQueue() {
        assertTrue(fixedRateOverrides().standingQueue(), "The standing queue option should be enabled.");
    }

    @Test
    void fixedRateMessageBytesDefaultToZero() {
        assertEquals(0, fixedRateDefaults().messageBytes(), "Message size should default to zero.");
    }

    @Test
    void fixedRateDepthDefaultsToZero() {
        assertEquals(0, fixedRateDefaults().depth(), "Depth should default to zero.");
    }

    @Test
    void fixedRateStandingQueueDefaultsToDisabled() {
        assertFalse(fixedRateDefaults().standingQueue(), "Standing queue mode should default to disabled.");
    }

    @Test
    @DisplayName("속도 없이 쓰면 거절하고 값은 양수여야 한다 — messageBytesRequireRate")
    void messageBytesRequireRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--message-bytes", "1024"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 없이 쓰면 거절하고 값은 양수여야 한다 — depthRequiresRate")
    void depthRequiresRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--depth", "1000"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 없이 쓰면 거절하고 값은 양수여야 한다 — standingQueueRequiresRate")
    void standingQueueRequiresRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--standing-queue"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 없이 쓰면 거절하고 값은 양수여야 한다 — messageBytesRejectZero")
    void messageBytesRejectZero() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "500", "--message-bytes", "0"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("속도 없이 쓰면 거절하고 값은 양수여야 한다 — depthRejectsNegativeValue")
    void depthRejectsNegativeValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "500", "--depth", "-1"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("내구성 옵션은 기본이 없고 수준 이름을 받는다 — durabilityHasNoDefault")
    void durabilityHasNoDefault() {
        assertTrue(Options.parse(new String[0], 0).durability().isEmpty(), "Durability should be empty.");
    }

    @Test
    @DisplayName("내구성 옵션은 기본이 없고 수준 이름을 받는다 — durabilityAcceptsProcessCrash")
    void durabilityAcceptsProcessCrash() {
        assertEquals(
                Durability.PROCESS_CRASH,
                Options.parse(new String[] {"--durability", "PROCESS_CRASH"}, 0)
                        .durability()
                        .orElseThrow(),
                "Durability should match the expected value.");
    }

    @Test
    @DisplayName("내구성 옵션은 기본이 없고 수준 이름을 받는다 — durabilityAcceptsPowerLossIgnoringCase")
    void durabilityAcceptsPowerLossIgnoringCase() {
        assertEquals(
                Durability.POWER_LOSS,
                Options.parse(new String[] {"--durability", "power_loss"}, 0)
                        .durability()
                        .orElseThrow(),
                "Durability should match the expected value.");
    }

    @Test
    @DisplayName("내구성 옵션은 기본이 없고 수준 이름을 받는다 — durabilityRejectsUnknownValue")
    void durabilityRejectsUnknownValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--durability", "sometimes"}, 0),
                "Parse should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("min achieved는 고정 속도 모드에서 설정된다")
    void minimumAchievementIsParsedWithRate() {
        assertEquals(
                0.95,
                Options.parse(new String[] {"--rate", "500", "--min-achieved", "0.95"}, 0)
                        .minAchieved(),
                "The configured minimum achievement should be preserved.");
    }

    @Test
    @DisplayName("min achieved의 기본값은 0이다")
    void minimumAchievementDefaultsToZero() {
        assertEquals(
                0,
                Options.parse(new String[] {"--rate", "500"}, 0).minAchieved(),
                "Minimum achievement should default to zero.");
    }

    @Test
    @DisplayName("min achieved는 rate가 필요하다")
    void minimumAchievementRequiresRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--min-achieved", "0.9"}, 0),
                "Minimum achievement should require a rate.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "1.01", "-0.5", "abc", "NaN"})
    @DisplayName("min achieved는 0 초과 1 이하의 숫자여야 한다")
    void minimumAchievementRejectsInvalidValues(final String bad) {
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "500", "--min-achieved", bad}, 0),
                "Minimum achievement should reject invalid value " + bad + ".");
    }
}
