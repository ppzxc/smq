package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.util.List;
import java.util.Locale;

final class DurabilitySelection {

    private DurabilitySelection() {}

    static List<Durability> parse(final String selection) {
        return switch (selection.toUpperCase(Locale.ROOT)) {
            case "BOTH" -> List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS);
            case "PROCESS_CRASH" -> List.of(Durability.PROCESS_CRASH);
            case "POWER_LOSS" -> List.of(Durability.POWER_LOSS);
            default -> throw new IllegalArgumentException("--durability must be PROCESS_CRASH, POWER_LOSS, or BOTH");
        };
    }
}
