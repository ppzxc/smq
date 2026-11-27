package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ChunkTableTest {

    @Test
    void chunkTableIncludesSmallPayloadSize() {
        final Map<String, Double> r;
        final String t;
        r = new HashMap<>();
        r.put("sqlite/S1/P0/POWER_LOSS/empty:msgPerSec", 500.0);
        r.put("sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 15_000.0);
        r.put("sqlite/S1/P4/PROCESS_CRASH/empty:msgPerSec", 1_000.0);
        r.put("sqlite/S1/P5/PROCESS_CRASH/empty:msgPerSec", 4_000.0);
        t = ChunkTable.render(r);
        assertTrue(t.contains("256B"), t);
    }

    @Test
    void chunkTableIncludesLargePayloadSize() {
        final Map<String, Double> r;
        final String t;
        r = new HashMap<>();
        r.put("sqlite/S1/P0/POWER_LOSS/empty:msgPerSec", 500.0);
        r.put("sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 15_000.0);
        r.put("sqlite/S1/P4/PROCESS_CRASH/empty:msgPerSec", 1_000.0);
        r.put("sqlite/S1/P5/PROCESS_CRASH/empty:msgPerSec", 4_000.0);
        t = ChunkTable.render(r);
        assertTrue(t.contains("4KB"), t);
    }

    @Test
    void chunkTableIncludesSmallPayloadRatio() {
        final Map<String, Double> r;
        final String t;
        r = new HashMap<>();
        r.put("sqlite/S1/P0/POWER_LOSS/empty:msgPerSec", 500.0);
        r.put("sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 15_000.0);
        r.put("sqlite/S1/P4/PROCESS_CRASH/empty:msgPerSec", 1_000.0);
        r.put("sqlite/S1/P5/PROCESS_CRASH/empty:msgPerSec", 4_000.0);
        t = ChunkTable.render(r);
        assertTrue(t.contains("30.0x"), "15000/500 = 30배\n" + t);
    }

    @Test
    void chunkTableIncludesLargePayloadRatio() {
        final Map<String, Double> r;
        final String t;
        r = new HashMap<>();
        r.put("sqlite/S1/P0/POWER_LOSS/empty:msgPerSec", 500.0);
        r.put("sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 15_000.0);
        r.put("sqlite/S1/P4/PROCESS_CRASH/empty:msgPerSec", 1_000.0);
        r.put("sqlite/S1/P5/PROCESS_CRASH/empty:msgPerSec", 4_000.0);
        t = ChunkTable.render(r);
        assertTrue(t.contains("4.0x"), "4000/1000 = 4배\n" + t);
    }

    @Test
    void usesFormatLocaleForRatios() {
        final var old = java.util.Locale.getDefault(java.util.Locale.Category.FORMAT);
        try {
            java.util.Locale.setDefault(java.util.Locale.Category.FORMAT, java.util.Locale.GERMANY);
            final var results = Map.of(
                    "sqlite/S1/P0/POWER_LOSS/empty:msgPerSec", 500.0, "sqlite/S1/P1/POWER_LOSS/empty:msgPerSec", 750.0);
            assertTrue(ChunkTable.render(results).contains("1,5x"), "Render should contain the expected value.");
        } finally {
            java.util.Locale.setDefault(java.util.Locale.Category.FORMAT, old);
        }
    }

    @Test
    void missingPairsRenderAsDashes() {
        final String t;
        t = ChunkTable.render(Map.of());
        assertTrue(t.contains("-"), t);
    }

    @Test
    void missingPairsDoNotRenderNonFiniteRatios() {
        final String t;
        t = ChunkTable.render(Map.of());
        assertFalse(t.contains("NaN") || t.contains("Infinity"), t);
    }

    @ParameterizedTest
    @ValueSource(strings = {"256B", "2KB", "4KB", "6KB"})
    void rowsCoverAllFourSizes(final String size) {
        assertTrue(ChunkTable.render(Map.of()).contains(size), "Table should include size " + size + ".");
    }

    @Test
    void rowsCoverThreeConditionsForEachSize() {
        final String t;
        t = ChunkTable.render(Map.of());
        assertEquals(
                12,
                t.lines()
                        .filter(l -> l.startsWith("S1 ") || l.startsWith("S2 "))
                        .count(),
                "Count should match the expected value.");
    }
}
