package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.ppzxc.smq.Durability;
import java.util.List;
import org.junit.jupiter.api.Test;

class DurabilitySelectionTest {

    @Test
    void parsesBothDurabilityModesCaseInsensitively() {
        assertEquals(List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS), DurabilitySelection.parse("bOtH"));
    }

    @Test
    void parsesEachDurabilityMode() {
        assertEquals(List.of(Durability.PROCESS_CRASH), DurabilitySelection.parse("PROCESS_CRASH"));
        assertEquals(List.of(Durability.POWER_LOSS), DurabilitySelection.parse("power_loss"));
    }

    @Test
    void rejectsUnknownDurabilitySelection() {
        assertEquals(
                "--durability must be PROCESS_CRASH, POWER_LOSS, or BOTH",
                assertThrows(IllegalArgumentException.class, () -> DurabilitySelection.parse("unknown"))
                        .getMessage());
    }
}
