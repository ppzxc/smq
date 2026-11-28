package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

record RecoveryOptions(
        List<Durability> durabilities,
        int messageCount,
        long leaseMillis,
        String negativeControl,
        Path output,
        Path workDir) {

    static RecoveryOptions parse(final String[] args) {
        final Map<String, String> values = new HashMap<>();
        for (int index = 1; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected recovery --option value pairs");
            }
            final String key = args[index].substring(2);
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        for (final String key : values.keySet()) {
            if (!List.of("durability", "message-count", "lease-ms", "negative-control", "output", "work-dir")
                    .contains(key)) {
                throw new IllegalArgumentException("unknown recovery option --" + key);
            }
        }

        final List<Durability> durabilities = switch (values.getOrDefault("durability", "BOTH")
                .toUpperCase(Locale.ROOT)) {
            case "BOTH" -> List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS);
            case "PROCESS_CRASH" -> List.of(Durability.PROCESS_CRASH);
            case "POWER_LOSS" -> List.of(Durability.POWER_LOSS);
            default -> throw new IllegalArgumentException("--durability must be BOTH, PROCESS_CRASH, or POWER_LOSS");
        };
        final int messageCount = positiveInt(values, "message-count", 4);
        final long leaseMillis = positiveLong(values, "lease-ms", 200);
        final String negativeControl = values.getOrDefault("negative-control", "none");
        if (!List.of("none", "drop-confirmed-publish").contains(negativeControl)) {
            throw new IllegalArgumentException("--negative-control must be none or drop-confirmed-publish");
        }
        return new RecoveryOptions(
                durabilities,
                messageCount,
                leaseMillis,
                negativeControl,
                Path.of(values.getOrDefault("output", "build/smoke-harness/recovery-report.json"))
                        .toAbsolutePath(),
                Path.of(values.getOrDefault("work-dir", "build/smoke-harness/recovery-work"))
                        .toAbsolutePath());
    }

    private static int positiveInt(final Map<String, String> values, final String name, final int fallback) {
        final int value = Integer.parseInt(values.getOrDefault(name, Integer.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }

    private static long positiveLong(final Map<String, String> values, final String name, final long fallback) {
        final long value = Long.parseLong(values.getOrDefault(name, Long.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }
}
