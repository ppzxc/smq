package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

final class JsonReport {

    private JsonReport() {}

    static void write(
            final Options options, final List<SmokeRunner.Sample> results, final String status, final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(4096);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":").append(quote(status)).append(',');
        json.append("\n  \"revision\":").append(quote(revision())).append(',');
        json.append("\n  \"productVersion\":").append(quote(productVersion())).append(',');
        json.append("\n  \"environment\":{");
        json.append("\"os\":").append(quote(System.getProperty("os.name") + " " + System.getProperty("os.version")));
        json.append(",\"architecture\":").append(quote(System.getProperty("os.arch")));
        json.append(",\"javaVersion\":").append(quote(Runtime.version().toString()));
        json.append(",\"processors\":").append(Runtime.getRuntime().availableProcessors());
        json.append(",\"databasePath\":").append(quote(options.workDir().toString()));
        json.append(",\"fileSystem\":")
                .append(quote(fileSystem(options.workDir())))
                .append("},");
        json.append("\n  \"config\":{");
        json.append("\"durabilities\":")
                .append(strings(options.durabilities().stream().map(Enum::name).toList()));
        json.append(",\"modes\":")
                .append(strings(options.modes().stream()
                        .map(mode -> mode.name().toLowerCase(Locale.ROOT))
                        .toList()));
        json.append(",\"count\":").append(options.messageCount());
        json.append(",\"warmupMessages\":").append(options.warmupMessages());
        json.append(",\"repeats\":").append(options.repeats());
        json.append(",\"payloadBytes\":").append(options.payloadBytes());
        json.append(",\"batchSize\":").append(options.batchSize());
        json.append(",\"maxInFlight\":").append(options.maxInFlight());
        json.append(",\"chunkSize\":").append(options.chunkSize()).append("},");
        json.append("\n  \"reproduction\":{");
        json.append("\"command\":").append(quote(reproductionCommand(options))).append(',');
        json.append("\"args\":")
                .append(strings(options.reproductionArguments()))
                .append("},");
        json.append("\n  \"completed\":").append(completed(results));
        json.append(",\n  \"failed\":").append(diagnostic == null ? 0 : 1);
        json.append(",\n  \"results\":[");
        for (int index = 0; index < results.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            appendSample(json, results.get(index));
        }
        json.append(']');
        if (diagnostic != null) {
            json.append(",\n  \"diagnostics\":[").append(quote(diagnostic)).append(']');
        } else {
            json.append(",\n  \"diagnostics\":[]");
        }
        json.append("\n}\n");
        Files.writeString(options.output(), json.toString());
    }

    private static void appendSample(final StringBuilder json, final SmokeRunner.Sample sample) {
        json.append("{\"durability\":").append(quote(sample.durability().name()));
        json.append(",\"mode\":").append(quote(sample.mode().name().toLowerCase(Locale.ROOT)));
        json.append(",\"repeat\":").append(sample.repeat());
        json.append(",\"completed\":").append(sample.completed());
        json.append(",\"failed\":").append(sample.failed());
        json.append(",\"maxObservedBatchSize\":").append(sample.maxObservedBatchSize());
        json.append(",\"maxObservedInFlight\":").append(sample.maxObservedInFlight());
        json.append(",\"elapsedNanos\":").append(sample.elapsedNanos());
        json.append(",\"throughputMessagesPerSecond\":")
                .append(String.format(Locale.ROOT, "%.3f", sample.throughputMessagesPerSecond()));
        json.append(",\"publishLatencyP50Nanos\":").append(sample.publishLatencyP50Nanos());
        json.append(",\"publishLatencyP99Nanos\":").append(sample.publishLatencyP99Nanos());
        json.append(",\"settleLatencyP50Nanos\":").append(sample.settleLatencyP50Nanos());
        json.append(",\"settleLatencyP99Nanos\":").append(sample.settleLatencyP99Nanos());
        json.append(",\"rawSamples\":{\"publishLatencyNanos\":").append(numbers(sample.publishLatencyNanos()));
        json.append(",\"settleLatencyNanos\":")
                .append(numbers(sample.settleLatencyNanos()))
                .append("}}");
    }

    private static long completed(final List<SmokeRunner.Sample> results) {
        return results.stream().mapToLong(SmokeRunner.Sample::completed).sum();
    }

    private static String fileSystem(final Path path) {
        try {
            return Files.getFileStore(path).type();
        } catch (final IOException exception) {
            return "unknown";
        }
    }

    private static String productVersion() {
        final String implementationVersion = Smq.class.getPackage().getImplementationVersion();
        return implementationVersion == null ? System.getProperty("smq.version", "unknown") : implementationVersion;
    }

    private static String reproductionCommand(final Options options) {
        final String arguments = options.reproductionArguments().stream()
                .map(JsonReport::gradleArgument)
                .collect(Collectors.joining(" "));
        return "./gradlew -p smoke-harness run --args='" + arguments.replace("'", "'\\''") + "'";
    }

    private static String gradleArgument(final String argument) {
        if (argument.chars()
                .allMatch(character -> Character.isLetterOrDigit(character) || "_./:-".indexOf(character) >= 0)) {
            return argument;
        }
        return "\"" + argument.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String revision() {
        try {
            final Process process = new ProcessBuilder("git", "rev-parse", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            final String revision = new String(process.getInputStream().readAllBytes()).trim();
            return process.waitFor() == 0 ? revision : "unknown";
        } catch (final IOException exception) {
            return "unknown";
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            return "unknown";
        }
    }

    private static String strings(final List<String> values) {
        final StringBuilder json = new StringBuilder("[");
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            json.append(quote(values.get(index)));
        }
        return json.append(']').toString();
    }

    private static String numbers(final List<Long> values) {
        final StringBuilder json = new StringBuilder("[");
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            json.append(values.get(index));
        }
        return json.append(']').toString();
    }

    static String quote(final String value) {
        final StringBuilder result = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            final char character = value.charAt(index);
            switch (character) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (character < 0x20) {
                        result.append(String.format(Locale.ROOT, "\\u%04x", (int) character));
                    } else {
                        result.append(character);
                    }
                }
            }
        }
        return result.append('"').toString();
    }
}
