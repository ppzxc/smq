package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

record Options(
        List<Durability> durabilities,
        List<Mode> modes,
        int messageCount,
        int warmupMessages,
        int repeats,
        int payloadBytes,
        int batchSize,
        int maxInFlight,
        int chunkSize,
        Path output,
        Path workDir) {

    enum Mode {
        SINGLE,
        BATCH
    }

    static Options parse(final String[] args) {
        final Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected --option value pairs");
            }
            final String key = args[index].substring(2);
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        final List<String> allowed = List.of(
                "durability",
                "mode",
                "count",
                "warmup",
                "repeat",
                "payload-bytes",
                "batch-size",
                "max-in-flight",
                "chunk-size",
                "output",
                "work-dir");
        for (final String key : values.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("unknown option --" + key);
            }
        }

        final List<Durability> durabilities = switch (value(values, "durability", "BOTH")
                .toUpperCase(Locale.ROOT)) {
            case "BOTH" -> List.of(Durability.PROCESS_CRASH, Durability.POWER_LOSS);
            case "PROCESS_CRASH" -> List.of(Durability.PROCESS_CRASH);
            case "POWER_LOSS" -> List.of(Durability.POWER_LOSS);
            default -> throw new IllegalArgumentException("--durability must be BOTH, PROCESS_CRASH, or POWER_LOSS");
        };
        final List<Mode> modes = switch (value(values, "mode", "both").toLowerCase(Locale.ROOT)) {
            case "both" -> List.of(Mode.SINGLE, Mode.BATCH);
            case "single" -> List.of(Mode.SINGLE);
            case "batch" -> List.of(Mode.BATCH);
            default -> throw new IllegalArgumentException("--mode must be both, single, or batch");
        };

        final int messageCount = positiveInt(values, "count", 1000);
        final int warmupMessages = nonNegativeInt(values, "warmup", 100);
        final int repeats = positiveInt(values, "repeat", 3);
        final int payloadBytes = positiveInt(values, "payload-bytes", 256);
        final int batchSize = positiveInt(values, "batch-size", 100);
        final int maxInFlight = positiveInt(values, "max-in-flight", 1000);
        final int chunkSize = positiveInt(values, "chunk-size", 100);
        if (batchSize > maxInFlight) {
            throw new IllegalArgumentException("--batch-size must not exceed --max-in-flight");
        }

        return new Options(
                durabilities,
                modes,
                messageCount,
                warmupMessages,
                repeats,
                payloadBytes,
                batchSize,
                maxInFlight,
                chunkSize,
                Path.of(value(values, "output", "build/smoke-harness/report.json"))
                        .toAbsolutePath(),
                Path.of(value(values, "work-dir", "build/smoke-harness/work")).toAbsolutePath());
    }

    List<String> reproductionArguments() {
        final List<String> arguments = new ArrayList<>();
        arguments.add("--durability");
        arguments.add(
                this.durabilities.size() == Durability.values().length
                        ? "BOTH"
                        : this.durabilities.getFirst().name());
        arguments.add("--mode");
        arguments.add(
                this.modes.size() == Mode.values().length
                        ? "both"
                        : this.modes.getFirst().name().toLowerCase(Locale.ROOT));
        arguments.add("--count");
        arguments.add(Integer.toString(this.messageCount));
        arguments.add("--warmup");
        arguments.add(Integer.toString(this.warmupMessages));
        arguments.add("--repeat");
        arguments.add(Integer.toString(this.repeats));
        arguments.add("--payload-bytes");
        arguments.add(Integer.toString(this.payloadBytes));
        arguments.add("--batch-size");
        arguments.add(Integer.toString(this.batchSize));
        arguments.add("--max-in-flight");
        arguments.add(Integer.toString(this.maxInFlight));
        arguments.add("--chunk-size");
        arguments.add(Integer.toString(this.chunkSize));
        arguments.add("--output");
        arguments.add(this.output.toString());
        arguments.add("--work-dir");
        arguments.add(this.workDir.toString());
        return List.copyOf(arguments);
    }

    private static String value(final Map<String, String> values, final String key, final String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    private static int positiveInt(final Map<String, String> values, final String key, final int defaultValue) {
        final int result = Integer.parseInt(value(values, key, Integer.toString(defaultValue)));
        if (result < 1) {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return result;
    }

    private static int nonNegativeInt(final Map<String, String> values, final String key, final int defaultValue) {
        final int result = Integer.parseInt(value(values, key, Integer.toString(defaultValue)));
        if (result < 0) {
            throw new IllegalArgumentException("--" + key + " must not be negative");
        }
        return result;
    }
}
