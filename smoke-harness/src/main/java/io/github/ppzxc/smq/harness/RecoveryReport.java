package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

final class RecoveryReport {

    private RecoveryReport() {}

    static void write(
            final RecoveryOptions options,
            final List<RecoveryResult> results,
            final String status,
            final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(2048);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":").append(quote(status)).append(',');
        json.append("\n  \"config\":{\"durabilities\":")
                .append(strings(options.durabilities().stream().map(Enum::name).toList()));
        json.append(",\"messageCount\":").append(options.messageCount());
        json.append(",\"leaseMillis\":").append(options.leaseMillis());
        json.append(",\"negativeControl\":").append(quote(options.negativeControl()));
        json.append(",\"output\":").append(quote(options.output().toString()));
        json.append(",\"workDir\":").append(quote(options.workDir().toString())).append("},");
        json.append("\n  \"reproduction\":{\"command\":")
                .append(quote(reproductionCommand(options)))
                .append(",\"args\":")
                .append(strings(reproductionArguments(options)))
                .append("},");
        json.append("\n  \"javaVersion\":")
                .append(quote(Runtime.version().toString()))
                .append(',');
        json.append("\n  \"results\":[");
        for (int index = 0; index < results.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            final RecoveryResult result = results.get(index);
            json.append("{\"durability\":").append(quote(result.durability.name()));
            json.append(",\"scenario\":").append(quote(result.scenario));
            json.append(",\"classification\":").append(quote(result.classification));
            json.append(",\"requestId\":").append(quote(result.requestId));
            json.append(",\"correlationId\":").append(quote(result.correlationId));
            json.append(",\"requestStarted\":").append(quote(result.requestStarted));
            json.append(",\"observedResponse\":").append(quote(result.observedResponse));
            json.append(",\"recoveryObservation\":").append(quote(result.recoveryObservation));
            json.append(",\"shutdownAt\":")
                    .append(result.shutdownAt == null ? "null" : quote(result.shutdownAt.toString()));
            json.append('}');
        }
        json.append(']');
        json.append(",\n  \"failed\":").append(diagnostic == null ? 0 : 1);
        json.append(",\n  \"diagnostics\":").append(diagnostic == null ? "[]" : "[" + quote(diagnostic) + "]");
        json.append("\n}\n");
        Files.writeString(options.output(), json.toString());
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

    private static String strings(final List<String> values) {
        return values.stream().map(RecoveryReport::quote).collect(Collectors.joining(",", "[", "]"));
    }

    private static List<String> reproductionArguments(final RecoveryOptions options) {
        final List<String> args = new java.util.ArrayList<>(List.of("recovery", "--durability"));
        args.add(
                options.durabilities().size() == Durability.values().length
                        ? "BOTH"
                        : options.durabilities().getFirst().name());
        args.add("--message-count");
        args.add(Integer.toString(options.messageCount()));
        args.add("--lease-ms");
        args.add(Long.toString(options.leaseMillis()));
        if (!"none".equals(options.negativeControl())) {
            args.add("--negative-control");
            args.add(options.negativeControl());
        }
        args.add("--output");
        args.add(options.output().toString());
        args.add("--work-dir");
        args.add(options.workDir().toString());
        return List.copyOf(args);
    }

    private static String reproductionCommand(final RecoveryOptions options) {
        final String args = reproductionArguments(options).stream()
                .map(RecoveryReport::commandArgument)
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
}
