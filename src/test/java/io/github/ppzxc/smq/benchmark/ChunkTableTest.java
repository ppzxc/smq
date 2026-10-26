package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChunkTableTest {

    @Test
    void showsSingleVersusBatchRatioPerPayloadSize() {
        Map<String, Double> r = new HashMap<>();
        r.put("sqlite/S1/P0/proc1/POWER_LOSS/empty:msgPerSec", 500.0);
        r.put("sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec", 15_000.0);
        r.put("sqlite/S1/P4S/proc1/PROCESS_CRASH/empty:msgPerSec", 1_000.0);
        r.put("sqlite/S1/P4/proc1/PROCESS_CRASH/empty:msgPerSec", 4_000.0);
        String t = ChunkTable.render(r);
        assertTrue(t.contains("256B"), t);
        assertTrue(t.contains("4KB"), t);
        assertTrue(t.contains("30.0x"), "15000/500 = 30배\n" + t);
        assertTrue(t.contains("4.0x"), "4000/1000 = 4배\n" + t);
    }

    @Test
    void missingPairsShowDashes() {
        String t = ChunkTable.render(Map.of());
        assertTrue(t.contains("-"), t);
        assertFalse(t.contains("NaN") || t.contains("Infinity"), t);
    }

    @Test
    void rowsCoverAllFourSizesAndThreeConditions() {
        String t = ChunkTable.render(Map.of());
        for (String size : new String[] {"256B", "2KB", "4KB", "6KB"}) assertTrue(t.contains(size), size);
        assertEquals(12, t.lines().filter(l -> l.startsWith("S1 ") || l.startsWith("S2 ")).count());
    }
}
