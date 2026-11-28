package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

record SoakOptions(
        long seed,
        int budget,
        int delayMillis,
        List<Durability> durabilities,
        String negativeControl,
        Path output,
        Path workDir) {

    static SoakOptions parse(final String[] args) {
        final Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected soak --option value pairs");
            }
            final String key = args[index].substring(2);
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        for (final String key : values.keySet()) {
            if (!List.of("seed", "budget", "delay-ms", "durability", "negative-control", "output", "work-dir")
                    .contains(key)) {
                throw new IllegalArgumentException("unknown soak option --" + key);
            }
        }
        if (!values.containsKey("seed") || !values.containsKey("budget")) {
            throw new IllegalArgumentException("soak requires --seed and --budget");
        }

        final long seed = Long.parseLong(values.get("seed"));
        final int budget = positiveInt(values, "budget", 1);
        final int delayMillis = positiveInt(values, "delay-ms", 50);
        if (delayMillis > 60_000) {
            throw new IllegalArgumentException("--delay-ms must not exceed 60000");
        }
        final List<Durability> durabilities = switch (values.getOrDefault("durability", "PROCESS_CRASH")
                .toUpperCase(Locale.ROOT)) {
            case "BOTH" -> List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS);
            case "PROCESS_CRASH" -> List.of(Durability.PROCESS_CRASH);
            case "POWER_LOSS" -> List.of(Durability.POWER_LOSS);
            default -> throw new IllegalArgumentException("--durability must be PROCESS_CRASH, POWER_LOSS, or BOTH");
        };
        final String negativeControl = values.getOrDefault("negative-control", "none");
        if (!List.of("none", "drop-first-delivery").contains(negativeControl)) {
            throw new IllegalArgumentException("--negative-control must be none or drop-first-delivery");
        }
        return new SoakOptions(
                seed,
                budget,
                delayMillis,
                durabilities,
                negativeControl,
                Path.of(values.getOrDefault("output", "build/smoke-harness/soak-report.json"))
                        .toAbsolutePath(),
                Path.of(values.getOrDefault("work-dir", "build/smoke-harness/soak-work"))
                        .toAbsolutePath());
    }

    private static int positiveInt(final Map<String, String> values, final String name, final int fallback) {
        final int value = Integer.parseInt(values.getOrDefault(name, Integer.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }
}
