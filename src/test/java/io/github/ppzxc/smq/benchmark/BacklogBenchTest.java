package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BacklogBenchTest {
    @Test
    void publicationStartsAtInitialRate() {
        final var rates = new BacklogBench.Rates(10_000, 5_000, 2_000, 60);
        assertEquals(10_000, rates.publishAt(0), "Publish at should match the expected value.");
    }

    @Test
    void publicationInterpolatesMidRamp() {
        final var rates = new BacklogBench.Rates(10_000, 5_000, 2_000, 60);
        assertEquals(6_000, rates.publishAt(30), "Publish at should match the expected value.");
    }

    @Test
    void publicationReachesFinalRate() {
        final var rates = new BacklogBench.Rates(10_000, 5_000, 2_000, 60);
        assertEquals(2_000, rates.publishAt(60), "Publish at should match the expected value.");
    }

    @Test
    void publicationRemainsAtFinalRateAfterRamp() {
        final var rates = new BacklogBench.Rates(10_000, 5_000, 2_000, 60);
        assertEquals(2_000, rates.publishAt(90), "Publish at should match the expected value.");
    }

    @Test
    void ratesRejectEqualInitialAndConsumerRates() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BacklogBench.Rates(5_000, 5_000, 2_000, 60),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    void ratesRejectEqualConsumerAndFinalRates() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BacklogBench.Rates(10_000, 5_000, 5_000, 60),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    void ratesRejectZeroFinalRate() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BacklogBench.Rates(10_000, 5_000, 0, 60),
                "Construction should throw IllegalArgumentException.");
    }
}
