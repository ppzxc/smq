package io.github.ppzxc.smq.harness.rabbit;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public record RabbitOptions(
        String host,
        int port,
        List<RabbitOptions.Mode> modes,
        int messageCount,
        int warmupMessages,
        int repeats,
        int payloadBytes,
        int batchSize,
        int maxInFlight,
        int chunkSize,
        Path output,
        Path workDir) {

    public enum Mode {
        SINGLE,
        BATCH
    }

    public static RabbitOptions parse(final String[] args, final int start) {
        final Map<String, String> values = new HashMap<>();
        for (int index = start; index < args.length; index += 2) {
            if (index + 1 == args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("expected --option value pairs");
            }
            final String key = args[index].substring(2);
            if (!List.of(
                            "host",
                            "port",
                            "mode",
                            "count",
                            "warmup",
                            "repeat",
                            "payload-bytes",
                            "batch-size",
                            "max-in-flight",
                            "chunk-size",
                            "output",
                            "work-dir")
                    .contains(key)) {
                throw new IllegalArgumentException("unknown option --" + key);
            }
            if (values.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate --" + key);
            }
        }
        final List<RabbitOptions.Mode> modes =
                switch (value(values, "mode", "both").toLowerCase(Locale.ROOT)) {
                    case "both" -> List.of(RabbitOptions.Mode.SINGLE, RabbitOptions.Mode.BATCH);
                    case "single" -> List.of(RabbitOptions.Mode.SINGLE);
                    case "batch" -> List.of(RabbitOptions.Mode.BATCH);
                    default -> throw new IllegalArgumentException("--mode must be both, single, or batch");
                };
        final int batchSize = positiveInt(values, "batch-size", 100);
        final int maxInFlight = positiveInt(values, "max-in-flight", 1000);
        if (batchSize > maxInFlight) {
            throw new IllegalArgumentException("--batch-size must not exceed --max-in-flight");
        }
        final int port = positiveInt(values, "port", 5672);
        if (port > 65535) {
            throw new IllegalArgumentException("--port must not exceed 65535");
        }
        return new RabbitOptions(
                value(values, "host", "localhost"),
                port,
                modes,
                positiveInt(values, "count", 1000),
                nonNegativeInt(values, "warmup", 100),
                positiveInt(values, "repeat", 3),
                positiveInt(values, "payload-bytes", 256),
                batchSize,
                maxInFlight,
                positiveInt(values, "chunk-size", 100),
                Path.of(value(values, "output", "build/smoke-harness/rabbit-report.json"))
                        .toAbsolutePath(),
                Path.of(value(values, "work-dir", "build/smoke-harness/rabbit-work"))
                        .toAbsolutePath());
    }

    List<String> reproductionArguments() {
        final List<String> arguments = new ArrayList<>();
        arguments.add("rabbit");
        arguments.add("run");
        arguments.add("--host");
        arguments.add(this.host);
        arguments.add("--port");
        arguments.add(Integer.toString(this.port));
        arguments.add("--mode");
        arguments.add(
                this.modes.size() == RabbitOptions.Mode.values().length
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
