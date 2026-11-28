package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

record BacklogOptions(
        List<Durability> durabilities,
        int backlogCount,
        long durationMillis,
        long warmupMillis,
        int repeats,
        int payloadBytes,
        int batchSize,
        int chunkSize,
        int publishers,
        int consumers,
        Path output,
        Path workDir) {

    static BacklogOptions parse(final String[] args) {
        final Map<String, String> values = new HashMap<>();
        for (int index = 1; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected backlog --option value pairs");
            }
            final String key = args[index].substring(2);
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        for (final String key : values.keySet()) {
            if (!List.of(
                            "durability",
                            "backlog-count",
                            "duration-ms",
                            "warmup-ms",
                            "repeat",
                            "payload-bytes",
                            "batch-size",
                            "chunk-size",
                            "publishers",
                            "consumers",
                            "output",
                            "work-dir")
                    .contains(key)) {
                throw new IllegalArgumentException("unknown backlog option --" + key);
            }
        }

        final List<Durability> durabilities = switch (value(values, "durability", "BOTH")
                .toUpperCase(Locale.ROOT)) {
            case "BOTH" -> List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS);
            case "PROCESS_CRASH" -> List.of(Durability.PROCESS_CRASH);
            case "POWER_LOSS" -> List.of(Durability.POWER_LOSS);
            default -> throw new IllegalArgumentException("--durability must be BOTH, PROCESS_CRASH, or POWER_LOSS");
        };
        if (!values.containsKey("backlog-count")) {
            throw new IllegalArgumentException("--backlog-count is required");
        }
        final int backlogCount = positiveInt(values, "backlog-count", 1);
        final long durationMillis = positiveLong(values, "duration-ms", 10_000);
        final long warmupMillis = nonNegativeLong(values, "warmup-ms", 3_000);
        final int repeats = positiveInt(values, "repeat", 3);
        final int payloadBytes = positiveInt(values, "payload-bytes", 256);
        final int batchSize = positiveInt(values, "batch-size", 100);
        final int chunkSize = positiveInt(values, "chunk-size", 100);
        final int publishers = positiveInt(values, "publishers", 1);
        final int consumers = positiveInt(values, "consumers", 1);
        return new BacklogOptions(
                durabilities,
                backlogCount,
                durationMillis,
                warmupMillis,
                repeats,
                payloadBytes,
                batchSize,
                chunkSize,
                publishers,
                consumers,
                Path.of(value(values, "output", "build/smoke-harness/backlog-report.json"))
                        .toAbsolutePath(),
                Path.of(value(values, "work-dir", "build/smoke-harness/backlog-work"))
                        .toAbsolutePath());
    }

    List<String> reproductionArguments() {
        final List<String> arguments = new ArrayList<>();
        arguments.add("backlog");
        arguments.add("--durability");
        arguments.add(
                this.durabilities.size() == Durability.values().length
                        ? "BOTH"
                        : this.durabilities.getFirst().name());
        arguments.add("--backlog-count");
        arguments.add(Integer.toString(this.backlogCount));
        arguments.add("--duration-ms");
        arguments.add(Long.toString(this.durationMillis));
        arguments.add("--warmup-ms");
        arguments.add(Long.toString(this.warmupMillis));
        arguments.add("--repeat");
        arguments.add(Integer.toString(this.repeats));
        arguments.add("--payload-bytes");
        arguments.add(Integer.toString(this.payloadBytes));
        arguments.add("--batch-size");
        arguments.add(Integer.toString(this.batchSize));
        arguments.add("--chunk-size");
        arguments.add(Integer.toString(this.chunkSize));
        arguments.add("--publishers");
        arguments.add(Integer.toString(this.publishers));
        arguments.add("--consumers");
        arguments.add(Integer.toString(this.consumers));
        arguments.add("--output");
        arguments.add(this.output.toString());
        arguments.add("--work-dir");
        arguments.add(this.workDir.toString());
        return List.copyOf(arguments);
    }

    private static String value(final Map<String, String> values, final String key, final String fallback) {
        return values.getOrDefault(key, fallback);
    }

    private static int positiveInt(final Map<String, String> values, final String key, final int fallback) {
        final int value = Integer.parseInt(value(values, key, Integer.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return value;
    }

    private static long positiveLong(final Map<String, String> values, final String key, final long fallback) {
        final long value = Long.parseLong(value(values, key, Long.toString(fallback)));
        if (value < 1) {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLong(final Map<String, String> values, final String key, final long fallback) {
        final long value = Long.parseLong(value(values, key, Long.toString(fallback)));
        if (value < 0) {
            throw new IllegalArgumentException("--" + key + " must not be negative");
        }
        return value;
    }
}
