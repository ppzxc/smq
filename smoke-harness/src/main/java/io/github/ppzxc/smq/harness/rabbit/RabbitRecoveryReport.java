package io.github.ppzxc.smq.harness.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class RabbitRecoveryReport {

    private RabbitRecoveryReport() {}

    public static void write(
            final RabbitRecoveryOptions options,
            final RabbitRecoveryRunner runner,
            final List<RabbitRecoveryRunner.Observation> observations,
            final String status,
            final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(4096);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":").append(quote(status)).append(',');
        json.append("\n  \"revision\":").append(quote(revision())).append(',');
        json.append("\n  \"broker\":{");
        json.append("\"container\":").append(quote(options.container()));
        json.append(",\"containerId\":").append(quote(runner.containerId()));
        json.append(",\"image\":").append(quote(runner.containerImage()));
        json.append(",\"expectedVersion\":").append(quote(RabbitPlan.SERVER_VERSION));
        json.append(",\"actualVersion\":").append(quote(runner.actualServerVersion()));
        json.append(",\"dataMount\":").append(quote(runner.dataMount()));
        json.append(",\"sameContainerRestarts\":").append(runner.restartCount()).append("},");
        json.append("\n  \"client\":{");
        json.append("\"artifact\":\"com.rabbitmq:amqp-client\",");
        json.append("\"expectedVersion\":").append(quote(RabbitPlan.CLIENT_VERSION));
        json.append(",\"actualVersion\":").append(quote(clientVersion())).append("},");
        json.append("\n  \"method\":{");
        json.append("\"stop\":\"docker kill --signal=KILL\",");
        json.append(
                "\"requestInProgress\":\"pause broker container, send basic.publish, wait for confirm, then force kill\",");
        json.append("\"queueStableForMs\":")
                .append(RabbitPlan.RECOVERY_QUEUE_STABILIZATION_MILLIS)
                .append(',');
        json.append("\"storage\":\"same named container and /var/lib/rabbitmq mount restarted for every point\",");
        json.append("\"consumerAck\":\"basic.ack send is recorded; no consumer ACK confirmation exists\",");
        json.append(
                "\"brokerObservation\":\"after recovery ACK send and consumer connection close, a new basic.get checks whether the message reappears; ready-message count is also recorded\"");
        json.append("},");
        json.append("\n  \"config\":{");
        json.append("\"host\":").append(quote(options.host()));
        json.append(",\"port\":").append(options.port());
        json.append(",\"durable\":true,\"persistentDeliveryMode\":2,\"mandatory\":true");
        json.append(",\"queueConfigurations\":[");
        for (int index = 0; index < RabbitPlan.QueueKind.values().length; index++) {
            if (index > 0) json.append(',');
            appendQueueKind(json, RabbitPlan.QueueKind.values()[index]);
        }
        json.append("]},");
        json.append("\n  \"classificationRules\":{");
        json.append("\"CONFIRMED\":\"publisher confirm was observed before forced broker stop\",");
        json.append(
                "\"UNKNOWN\":\"request was sent but publisher confirm was not observed before forced broker stop\",");
        json.append(
                "\"consumerAck\":\"ACK send is not treated as broker-confirmed; post-restart queue state is recorded separately\"");
        json.append("},");
        json.append("\n  \"reproduction\":{");
        json.append("\"command\":").append(quote(reproductionCommand(options))).append("},");
        json.append("\n  \"results\":[");
        for (int index = 0; index < observations.size(); index++) {
            if (index > 0) json.append(',');
            appendObservation(json, observations.get(index));
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

    private static void appendObservation(
            final StringBuilder json, final RabbitRecoveryRunner.Observation observation) {
        json.append("{\"queueType\":").append(quote(observation.queueKind().queueType()));
        json.append(",\"queueName\":").append(quote(observation.queueName()));
        json.append(",\"scenario\":").append(quote(observation.scenario()));
        json.append(",\"classification\":").append(quote(observation.classification()));
        json.append(",\"correlationId\":").append(quote(observation.correlationId()));
        json.append(",\"queueStableForMs\":").append(observation.queueStableForMs());
        json.append(",\"confirmObservedBeforeCrash\":").append(observation.confirmObservedBeforeCrash());
        json.append(",\"confirmationObservedAt\":").append(quote(observation.confirmationObservedAt()));
        json.append(",\"deliveryObservedBeforeCrash\":").append(observation.deliveryObservedBeforeCrash());
        json.append(",\"deliveryObservedAt\":").append(quote(observation.deliveryObservedAt()));
        json.append(",\"ackSentBeforeCrash\":").append(observation.ackSentBeforeCrash());
        json.append(",\"requestSentToSocket\":").append(observation.requestSentToSocket());
        json.append(",\"requestState\":").append(quote(observation.requestState()));
        json.append(",\"requestStartedAt\":").append(quote(observation.requestStartedAt()));
        json.append(",\"requestSentAt\":").append(quote(observation.requestSentAt()));
        json.append(",\"shutdownAt\":").append(quote(observation.shutdownAt()));
        json.append(",\"restartAt\":").append(quote(observation.restartAt()));
        json.append(",\"recovered\":").append(observation.recovered());
        json.append(",\"redeliveredAfterRestart\":").append(observation.redeliveredAfterRestart());
        json.append(",\"recoveryObservation\":").append(quote(observation.recoveryObservation()));
        json.append(",\"recoveryObservationStatus\":").append(quote(observation.recoveryObservationStatus()));
        json.append(",\"ackSentAt\":")
                .append(quote(observation.ackSentAt() == null ? "not-sent" : observation.ackSentAt()));
        json.append(",\"ackApplicationIndependentlyObserved\":")
                .append(observation.ackApplicationIndependentlyObserved());
        json.append(",\"ackApplicationObservation\":").append(quote(observation.ackApplicationObservation()));
        json.append(",\"readyMessagesAfterAckObservation\":")
                .append(observation.readyMessagesAfterAckObservation())
                .append('}');
    }

    private static String clientVersion() {
        final String version = ConnectionFactory.class.getPackage().getImplementationVersion();
        if (version != null) return version;
        return ConnectionFactory.class.getModule().getDescriptor().rawVersion().orElse("unknown");
    }

    private static String reproductionCommand(final RabbitRecoveryOptions options) {
        final List<String> arguments = List.of(
                "rabbit",
                "recovery",
                "--host",
                options.host(),
                "--port",
                Integer.toString(options.port()),
                "--container",
                options.container(),
                "--output",
                options.output().toString(),
                "--work-dir",
                options.workDir().toString());
        final String args =
                arguments.stream().map(RabbitRecoveryReport::shellArgument).collect(Collectors.joining(" "));
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
