package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BacklogBenchTest {
    @Test
    void publicationSlowsContinuouslyAndRemainsPositiveAfterRamp() {
        var rates = new BacklogBench.Rates(10_000, 5_000, 2_000, 60);
        assertEquals(10_000, rates.publishAt(0));
        assertEquals(6_000, rates.publishAt(30));
        assertEquals(2_000, rates.publishAt(60));
        assertEquals(2_000, rates.publishAt(90));
    }

    @Test
    void requiresGrowthAndRecoveryWithTheSameConsumerLimit() {
        assertThrows(IllegalArgumentException.class, () -> new BacklogBench.Rates(5_000, 5_000, 2_000, 60));
        assertThrows(IllegalArgumentException.class, () -> new BacklogBench.Rates(10_000, 5_000, 5_000, 60));
        assertThrows(IllegalArgumentException.class, () -> new BacklogBench.Rates(10_000, 5_000, 0, 60));
    }
}
