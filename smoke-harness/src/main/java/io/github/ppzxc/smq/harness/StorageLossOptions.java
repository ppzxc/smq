package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

record StorageLossOptions(
        List<Durability> durabilities,
        boolean simulateStorageLoss,
        boolean requireStorageLoss,
        String negativeControl,
        long leaseMillis,
        Path output,
        Path workDir) {

    static StorageLossOptions parse(final String[] args) {
        final Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected storage-loss --option value pairs");
            }
            final String key = args[index].substring(2);
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        for (final String key : values.keySet()) {
            if (!List.of(
                            "durability",
                            "simulate-storage-loss",
                            "require-storage-loss",
                            "negative-control",
                            "lease-ms",
                            "output",
                            "work-dir")
                    .contains(key)) {
                throw new IllegalArgumentException("unknown storage-loss option --" + key);
            }
        }

        final List<Durability> durabilities = DurabilitySelection.parse(values.getOrDefault("durability", "BOTH"));
        final boolean simulate = booleanValue(values, "simulate-storage-loss", true);
        final boolean require = booleanValue(values, "require-storage-loss", false);
        final String negativeControl = values.getOrDefault("negative-control", "none");
        if (!List.of("none", "drop-confirmed-publish").contains(negativeControl)) {
            throw new IllegalArgumentException("--negative-control must be none or drop-confirmed-publish");
        }
        final long leaseMillis = positiveLong(values, "lease-ms", 200L);
        return new StorageLossOptions(
                durabilities,
                simulate,
                require,
                negativeControl,
                leaseMillis,
                Path.of(values.getOrDefault("output", "build/smoke-harness/storage-loss-report.json"))
                        .toAbsolutePath(),
                Path.of(values.getOrDefault("work-dir", "build/smoke-harness/storage-loss-work"))
                        .toAbsolutePath());
    }

    private static boolean booleanValue(final Map<String, String> values, final String name, final boolean fallback) {
        final String value = values.getOrDefault(name, Boolean.toString(fallback));
        if (!List.of("true", "false").contains(value.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("--" + name + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }

    private static long positiveLong(final Map<String, String> values, final String name, final long fallback) {
        final long value = Long.parseLong(values.getOrDefault(name, Long.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }
}
