package io.github.ppzxc.smq.harness.kafka;

import java.nio.file.Path;

public record KafkaRunOptions(
        int count, int warmup, int repeat, int batchSize, int payloadBytes, int port, Path workDir, Path output) {

    public static KafkaRunOptions parse(final String[] args) {
        int count = 1_000;
        int warmup = 100;
        int repeat = 3;
        int batchSize = 100;
        int payloadBytes = 256;
        int port = 19_092;
        Path workDir = Path.of("build", "kafka-harness", "work");
        Path output = Path.of("build", "kafka-harness", "report.json");

        for (int index = 2; index < args.length; index++) {
            final String name = args[index];
            if (index + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for " + name);
            }
            final String value = args[++index];
            try {
                switch (name) {
                    case "--count" -> count = positive(name, value);
                    case "--warmup" -> warmup = nonNegative(name, value);
                    case "--repeat" -> repeat = positive(name, value);
                    case "--batch-size" -> batchSize = positive(name, value);
                    case "--payload-bytes" -> payloadBytes = nonNegative(name, value);
                    case "--port" -> port = port(value);
                    case "--work-dir" -> workDir = Path.of(value);
                    case "--output" -> output = Path.of(value);
                    default -> throw new IllegalArgumentException("unknown Kafka run option: " + name);
                }
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(name + " must be an integer", exception);
            }
        }
        return new KafkaRunOptions(count, warmup, repeat, batchSize, payloadBytes, port, workDir, output);
    }

    private static int positive(final String name, final String value) {
        final int result = Integer.parseInt(value);
        if (result <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return result;
    }

    private static int nonNegative(final String name, final String value) {
        final int result = Integer.parseInt(value);
        if (result < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return result;
    }

    private static int port(final String value) {
        final int result = Integer.parseInt(value);
        if (result < 1 || result > 65_535) {
            throw new IllegalArgumentException("--port must be between 1 and 65535");
        }
        return result;
    }
}
