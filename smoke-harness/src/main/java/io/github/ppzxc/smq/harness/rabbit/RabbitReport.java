package io.github.ppzxc.smq.harness.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class RabbitReport {

    private RabbitReport() {}

    public static void write(
            final RabbitOptions options,
            final List<RabbitRunner.Sample> samples,
            final String actualServerVersion,
            final String status,
            final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(4096);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":").append(quote(status)).append(',');
        json.append("\n  \"revision\":").append(quote(revision())).append(',');
        json.append("\n  \"environment\":{");
        json.append("\"os\":").append(quote(System.getProperty("os.name") + " " + System.getProperty("os.version")));
        json.append(",\"architecture\":").append(quote(System.getProperty("os.arch")));
        json.append(",\"javaVersion\":").append(quote(Runtime.version().toString()));
        json.append(",\"processors\":").append(Runtime.getRuntime().availableProcessors());
        json.append(",\"workDir\":").append(quote(options.workDir().toString()));
        json.append(",\"fileSystem\":")
                .append(quote(fileSystem(options.workDir())))
                .append("},");
        json.append("\n  \"broker\":{");
        json.append("\"image\":").append(quote(RabbitPlan.SERVER_IMAGE));
        json.append(",\"expectedVersion\":").append(quote(RabbitPlan.SERVER_VERSION));
        json.append(",\"actualVersion\":").append(quote(actualServerVersion)).append("},");
        json.append("\n  \"client\":{");
        json.append("\"artifact\":\"com.rabbitmq:amqp-client\",");
        json.append("\"expectedVersion\":").append(quote(RabbitPlan.CLIENT_VERSION));
        json.append(",\"actualVersion\":").append(quote(clientVersion())).append("},");
        json.append("\n  \"completionCriteria\":{");
        json.append("\"publish\":\"only routed persistent publishes with publisher confirms count complete\",");
        json.append(
                "\"routingErrors\":\"mandatory publishes are observed through ReturnListener and count as errors\",");
        json.append("\"rejections\":\"negative publisher confirms count as errors\",");
        json.append("\"delivery\":\"RabbitMQ basic.get manual deliveries\",");
        json.append(
                "\"settlement\":\"consumer basic.ack send time is measured; basic.ack has no broker confirmation\",");
        json.append(
                "\"brokerState\":\"queueDeclarePassive ready-message count is observed after ACK sends; it does not count unacked deliveries\"");
        json.append("},");
        json.append("\n  \"config\":{");
        json.append("\"host\":").append(quote(options.host()));
        json.append(",\"port\":").append(options.port());
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
        json.append(",\"chunkSize\":").append(options.chunkSize());
        json.append(",\"durable\":true,\"persistentDeliveryMode\":2,\"mandatory\":true");
        json.append(",\"queueConfigurations\":[");
        for (int index = 0; index < RabbitPlan.QueueKind.values().length; index++) {
            if (index > 0) json.append(',');
            appendQueueKind(json, RabbitPlan.QueueKind.values()[index]);
        }
        json.append("]},");
        json.append("\n  \"reproduction\":{");
        json.append("\"command\":").append(quote(reproductionCommand(options))).append(',');
        json.append("\"args\":")
                .append(strings(options.reproductionArguments()))
                .append("},");
        json.append("\n  \"completed\":")
                .append(samples.stream()
                        .mapToLong(RabbitRunner.Sample::completedMessages)
                        .sum());
        json.append(",\n  \"failed\":")
                .append(samples.stream()
                                .mapToLong(sample -> sample.routingFailures() + sample.rejectedConfirms())
                                .sum()
                        + (diagnostic == null ? 0 : 1));
        json.append(",\n  \"results\":[");
        for (int index = 0; index < samples.size(); index++) {
            if (index > 0) json.append(',');
            appendSample(json, samples.get(index));
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

    private static void appendQueueKind(final StringBuilder json, final RabbitPlan.QueueKind kind) {
        json.append("{\"queueType\":").append(quote(kind.queueType()));
        json.append(",\"durable\":true,\"exclusive\":false,\"autoDelete\":false,\"arguments\":{");
        int index = 0;
        for (final var entry : kind.arguments().entrySet()) {
            if (index++ > 0) json.append(',');
            json.append(quote(entry.getKey())).append(':');
            if (entry.getValue() instanceof Number number) {
                json.append(number);
            } else {
                json.append(quote(entry.getValue().toString()));
            }
        }
        json.append("}}");
    }

    private static void appendSample(final StringBuilder json, final RabbitRunner.Sample sample) {
        json.append("{\"queueType\":").append(quote(sample.queueKind().queueType()));
        json.append(",\"mode\":").append(quote(sample.mode().name().toLowerCase(Locale.ROOT)));
        json.append(",\"repeat\":").append(sample.repeat());
        json.append(",\"completedMessages\":").append(sample.completedMessages());
        json.append(",\"publisherConfirmsObserved\":").append(sample.completedMessages());
        json.append(",\"deliveryCount\":").append(sample.deliveryCount());
        json.append(",\"consumerAckSends\":").append(sample.ackSends());
        json.append(",\"readyMessagesAfterAckObservation\":").append(sample.readyMessagesAfterAckObservation());
        json.append(",\"elapsedNanos\":").append(sample.elapsedNanos());
        json.append(",\"throughputMessagesPerSecond\":")
                .append(String.format(Locale.ROOT, "%.3f", sample.throughputMessagesPerSecond()));
        json.append(",\"publishLatencyP50Nanos\":").append(sample.publishLatencyP50Nanos());
        json.append(",\"publishLatencyP99Nanos\":").append(sample.publishLatencyP99Nanos());
        json.append(",\"ackSendLatencyP50Nanos\":").append(sample.ackSendLatencyP50Nanos());
        json.append(",\"ackSendLatencyP99Nanos\":").append(sample.ackSendLatencyP99Nanos());
        json.append(",\"maxObservedBatchSize\":").append(sample.maxObservedBatchSize());
        json.append(",\"maxObservedInFlight\":").append(sample.maxObservedInFlight());
        json.append(",\"routingFailures\":").append(sample.routingFailures());
        json.append(",\"rejectedConfirms\":").append(sample.rejectedConfirms());
        json.append(",\"rawSamples\":{\"publishLatencyNanos\":").append(numbers(sample.publishLatencyNanos()));
        json.append(",\"ackSendLatencyNanos\":")
                .append(numbers(sample.ackSendLatencyNanos()))
                .append("}}");
    }

    private static String clientVersion() {
        final String version = ConnectionFactory.class.getPackage().getImplementationVersion();
        if (version != null) return version;
        return ConnectionFactory.class.getModule().getDescriptor().rawVersion().orElse("unknown");
    }

    private static String reproductionCommand(final RabbitOptions options) {
        final String args = options.reproductionArguments().stream()
                .map(RabbitReport::shellArgument)
                .collect(Collectors.joining(" "));
        return "./gradlew -p smoke-harness run --args='" + args.replace("'", "'\\''") + "'";
    }

    private static String shellArgument(final String argument) {
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

    private static String fileSystem(final Path path) {
        try {
            return Files.getFileStore(path).type();
        } catch (final IOException exception) {
            return "unknown";
        }
    }

    private static String strings(final List<String> values) {
        return values.stream().map(RabbitReport::quote).collect(Collectors.joining(",", "[", "]"));
    }

    private static String numbers(final List<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
    }

    private static String quote(final String value) {
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
