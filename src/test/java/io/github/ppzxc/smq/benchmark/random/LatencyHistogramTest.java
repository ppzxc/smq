package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LatencyHistogramTest {

    @ParameterizedTest
    @CsvSource({"50, 500", "99, 990", "100, 1000"})
    @DisplayName("백분위수는 nearest rank다")
    void thePercentileIsTheNearestRank(final int percentile, final long expected) {
        final LatencyHistogram h;
        h = new LatencyHistogram();
        for (long v = 1; v <= 100; v++) {
            h.add(v * 10);
        }
        assertEquals(expected, h.percentile(percentile), "Percentile should match the expected value.");
    }

    @Test
    @DisplayName("표본 수를 센다")
    void countsSamples() {
        final LatencyHistogram h;
        h = new LatencyHistogram();
        for (long v = 1; v <= 100; v++) {
            h.add(v * 10);
        }
        assertEquals(100, h.total(), "Total should match the expected value.");
    }

    @ParameterizedTest
    @CsvSource({"50, 0", "100, " + LatencyHistogram.MAX_MILLIS})
    @DisplayName("한계를 넘는 값은 한계로 세고 음수는 0으로 센다")
    void valuesExceedingTheLimitAreCountedAsTheLimitAndNegativeValuesAreCountedAs0(
            final int percentile, final long expected) {
        final LatencyHistogram h;
        h = new LatencyHistogram();
        h.add(-5);
        h.add(Long.MAX_VALUE);
        assertEquals(expected, h.percentile(percentile), "Percentile should match the expected value.");
    }

    @Test
    @DisplayName("표본이 없으면 백분위수를 거부한다")
    void ifThereIsNoSampleThePercentileIsRejected() {
        assertThrows(
                IllegalStateException.class,
                () -> new LatencyHistogram().percentile(50),
                "Percentile should throw IllegalStateException.");
    }
}
