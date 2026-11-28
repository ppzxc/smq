package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

final class BacklogReport {

    private BacklogReport() {}

    static void write(
            final BacklogOptions options,
            final List<BacklogRunner.Sample> results,
            final String status,
            final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(8_192);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"scenario\":\"backlog\",");
        json.append("\n  \"status\":").append(JsonReport.quote(status)).append(',');
        json.append("\n  \"revision\":").append(JsonReport.quote(revision())).append(',');
        json.append("\n  \"productVersion\":")
                .append(JsonReport.quote(productVersion()))
                .append(',');
        json.append("\n  \"environment\":{");
        json.append("\"os\":")
                .append(JsonReport.quote(System.getProperty("os.name") + " " + System.getProperty("os.version")));
        json.append(",\"architecture\":").append(JsonReport.quote(System.getProperty("os.arch")));
        json.append(",\"javaVersion\":")
                .append(JsonReport.quote(Runtime.version().toString()));
        json.append(",\"processors\":").append(Runtime.getRuntime().availableProcessors());
        json.append(",\"workDir\":")
                .append(JsonReport.quote(options.workDir().toString()))
                .append("},");
        json.append("\n  \"config\":{");
        json.append("\"durabilities\":")
                .append(strings(options.durabilities().stream().map(Enum::name).toList()));
        json.append(",\"backlogCount\":").append(options.backlogCount());
        json.append(",\"durationMillis\":").append(options.durationMillis());
        json.append(",\"warmupMillis\":").append(options.warmupMillis());
        json.append(",\"repeats\":").append(options.repeats());
        json.append(",\"payloadBytes\":").append(options.payloadBytes());
        json.append(",\"batchSize\":").append(options.batchSize());
        json.append(",\"chunkSize\":").append(options.chunkSize());
        json.append(",\"publishers\":").append(options.publishers());
        json.append(",\"consumers\":").append(options.consumers());
        json.append(",\"queueKinds\":")
                .append(strings(
                        BacklogRunner.queueKinds().stream().map(Enum::name).toList()))
                .append("},");
        json.append("\n  \"reproduction\":{\"command\":")
                .append(JsonReport.quote(reproductionCommand(options)))
                .append(",\"args\":")
                .append(strings(options.reproductionArguments()))
                .append("},");
        json.append("\n  \"publishedMessages\":")
                .append(results.stream()
                        .mapToLong(BacklogRunner.Sample::publishedMessages)
                        .sum());
        json.append(",\n  \"settledMessages\":")
                .append(results.stream()
                        .mapToLong(BacklogRunner.Sample::settledMessages)
                        .sum());
        json.append(",\n  \"results\":[");
        for (int index = 0; index < results.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            appendSample(json, results.get(index));
        }
        json.append("],\n  \"depthRatios\":[");
        appendRatios(json, options, results);
        json.append(']');
        json.append(",\n  \"diagnostics\":")
                .append(diagnostic == null ? "[]" : "[" + JsonReport.quote(diagnostic) + "]");
        json.append("\n}\n");
        Files.writeString(options.output(), json.toString());
    }

    private static void appendSample(final StringBuilder json, final BacklogRunner.Sample sample) {
        json.append("{\"durability\":")
                .append(JsonReport.quote(sample.durability().name()));
        json.append(",\"queueKind\":")
                .append(JsonReport.quote(sample.queueKind().name()));
        json.append(",\"queueConfig\":{");
        json.append("\"maxDeliveries\":")
                .append(sample.queueKind() == BacklogRunner.QueueKind.MAX_DELIVERIES ? 5 : "null");
        json.append(",\"deadLetterQueue\":")
                .append(
                        sample.queueKind() == BacklogRunner.QueueKind.MAX_DELIVERIES
                                ? JsonReport.quote("backlog-dlq")
                                : "null");
        json.append(",\"singleActiveConsumer\":")
                .append(sample.queueKind() == BacklogRunner.QueueKind.SINGLE_ACTIVE_CONSUMER);
        json.append(",\"leasedHead\":")
                .append(sample.queueKind() == BacklogRunner.QueueKind.LEASED_HEAD)
                .append('}');
        json.append(",\"preloadedMessages\":").append(sample.backlogCount());
        json.append(",\"repeat\":").append(sample.repeat());
        json.append(",\"startDepth\":").append(sample.startDepth());
        json.append(",\"startCounts\":");
        appendCounts(json, sample.startCounts());
        json.append(",\"endDepth\":").append(sample.endDepth());
        json.append(",\"endCounts\":");
        appendCounts(json, sample.endCounts());
        json.append(",\"publishedMessages\":").append(sample.publishedMessages());
        json.append(",\"settledMessages\":").append(sample.settledMessages());
        json.append(",\"elapsedNanos\":").append(sample.elapsedNanos());
        json.append(",\"publishThroughputMessagesPerSecond\":")
                .append(number(sample.publishThroughputMessagesPerSecond()));
        json.append(",\"settleThroughputMessagesPerSecond\":")
                .append(number(sample.settleThroughputMessagesPerSecond()));
        json.append(",\"deliveryLatencyP50Nanos\":").append(nullable(sample.deliveryLatencyP50Nanos()));
        json.append(",\"deliveryLatencyP99Nanos\":").append(nullable(sample.deliveryLatencyP99Nanos()));
        json.append(",\"databasePath\":")
                .append(JsonReport.quote(sample.databasePath()))
                .append('}');
    }

    private static void appendCounts(final StringBuilder json, final QueueCounts counts) {
        json.append("{\"ready\":").append(counts.ready());
        json.append(",\"leased\":").append(counts.leased());
        json.append(",\"delayed\":").append(counts.delayed()).append('}');
    }

    private static void appendRatios(
            final StringBuilder json, final BacklogOptions options, final List<BacklogRunner.Sample> results) {
        boolean first = true;
        for (final BacklogRunner.Sample backlog : results) {
            if (backlog.backlogCount() == 0) {
                continue;
            }
            final BacklogRunner.Sample empty = results.stream()
                    .filter(sample -> sample.backlogCount() == 0
                            && sample.durability() == backlog.durability()
                            && sample.queueKind() == backlog.queueKind()
                            && sample.repeat() == backlog.repeat())
                    .findFirst()
                    .orElse(null);
            if (empty == null) {
                continue;
            }
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append("{\"durability\":")
                    .append(JsonReport.quote(backlog.durability().name()));
            json.append(",\"queueKind\":")
                    .append(JsonReport.quote(backlog.queueKind().name()));
            json.append(",\"repeat\":").append(backlog.repeat());
            json.append(",\"backlogCount\":").append(options.backlogCount());
            json.append(",\"publishThroughput\":")
                    .append(ratio(
                            backlog.publishThroughputMessagesPerSecond(), empty.publishThroughputMessagesPerSecond()));
            json.append(",\"settleThroughput\":")
                    .append(ratio(
                            backlog.settleThroughputMessagesPerSecond(), empty.settleThroughputMessagesPerSecond()));
            json.append(",\"deliveryLatencyP50\":")
                    .append(ratio(backlog.deliveryLatencyP50Nanos(), empty.deliveryLatencyP50Nanos()));
            json.append(",\"deliveryLatencyP99\":")
                    .append(ratio(backlog.deliveryLatencyP99Nanos(), empty.deliveryLatencyP99Nanos()))
                    .append('}');
        }
    }

    private static Double ratio(final double numerator, final double denominator) {
        return denominator == 0 ? null : numerator / denominator;
    }

    private static Double ratio(final Long numerator, final Long denominator) {
        return numerator == null || denominator == null || denominator == 0
                ? null
                : numerator.doubleValue() / denominator;
    }

    private static String nullable(final Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String number(final double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String strings(final List<String> values) {
        return values.stream().map(JsonReport::quote).collect(Collectors.joining(",", "[", "]"));
    }

    private static String reproductionCommand(final BacklogOptions options) {
        final String args = options.reproductionArguments().stream()
                .map(BacklogReport::commandArgument)
                .collect(Collectors.joining(" "));
        return "./gradlew smokeHarness -Psmoke.args='" + args.replace("'", "'\\''") + "'";
    }

    private static String commandArgument(final String argument) {
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

    private static String productVersion() {
        final String version = Smq.class.getPackage().getImplementationVersion();
        return version == null ? System.getProperty("smq.version", "unknown") : version;
    }
}
