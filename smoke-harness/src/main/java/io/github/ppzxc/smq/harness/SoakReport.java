package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class SoakReport {

    private record Invariant(String id, String scenario, String source, String test, String contract) {}

    private static final List<Invariant> INVARIANTS = List.of(
            new Invariant(
                    "LOSS",
                    "message-accounting",
                    "SoakRunner#checkMessageAccounting",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "StorageConservationPropertiesTest#commandSequencesPreserveSqliteQueueState"),
            new Invariant(
                    "GHOST_DELIVERY",
                    "message-accounting",
                    "SoakRunner#checkMessageAccounting",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "StorageConservationPropertiesTest#pull"),
            new Invariant(
                    "DUPLICATE_LIMIT",
                    "delivery-limit",
                    "SoakRunner#checkDeliveryLimit",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "SmqConformanceTest#messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck1"),
            new Invariant(
                    "DEAD_LETTER_MOVEMENT",
                    "delivery-limit",
                    "SoakRunner#checkDeliveryLimit",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "SmqConformanceTest#messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck1"),
            new Invariant(
                    "DELAY",
                    "delayed-publication",
                    "SoakRunner#checkDelayAndCounts",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "StorageContractTest#delayedMessageCannotBePulledBeforeDeadline; StorageContractTest#delayedMessageCanBePulledAtDeadline"),
            new Invariant(
                    "SINGLE_ACTIVE_CONSUMER",
                    "single-active-lease",
                    "SoakRunner#checkSingleActiveConsumer",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "StorageContractTest#ifASingleActiveLeasePreventsAFetchTheCompletedDelayAndExpiredLeaseCountsAreMaintainedCheck1"),
            new Invariant(
                    "APPROXIMATE_COUNTS",
                    "delayed-publication",
                    "SoakRunner#checkDelayAndCounts",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "StorageContractTest#recordTheDelayAfterTheFetchIsCompletedAsReadyAndUpdateTheCountCheck1; ADR-0018"),
            new Invariant(
                    "CHUNK_ORDER",
                    "message-accounting",
                    "SoakRunner#checkMessageAccounting",
                    "SoakCliTest#seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule",
                    "SmqConformanceTest#fifoOrderFollowsPublishOrderAcrossBatches"));

    private SoakReport() {}

    static void write(
            final SoakOptions options,
            final String workloadDigest,
            final List<SoakResult> results,
            final List<String> diagnostics)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final boolean failed = !diagnostics.isEmpty() || results.stream().anyMatch(result -> !result.passed());
        final StringBuilder json = new StringBuilder(8192);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":")
                .append(RecoveryReport.quote(failed ? "FAILURE" : "SUCCESS"))
                .append(',');
        json.append("\n  \"revision\":")
                .append(RecoveryReport.quote(revision()))
                .append(',');
        json.append("\n  \"productVersion\":")
                .append(RecoveryReport.quote(productVersion()))
                .append(',');
        json.append("\n  \"environment\":{");
        json.append("\"os\":")
                .append(RecoveryReport.quote(System.getProperty("os.name") + " " + System.getProperty("os.version")));
        json.append(",\"architecture\":").append(RecoveryReport.quote(System.getProperty("os.arch")));
        json.append(",\"javaVersion\":")
                .append(RecoveryReport.quote(Runtime.version().toString()));
        json.append(",\"processors\":").append(Runtime.getRuntime().availableProcessors());
        json.append(",\"workDir\":")
                .append(RecoveryReport.quote(options.workDir().toString()));
        json.append(",\"fileSystem\":")
                .append(RecoveryReport.quote(fileSystem(options.workDir())))
                .append("},");
        json.append("\n  \"seed\":").append(options.seed()).append(',');
        json.append("\n  \"workloadDigest\":")
                .append(RecoveryReport.quote(workloadDigest))
                .append(',');
        json.append("\n  \"config\":{");
        json.append("\"budget\":").append(options.budget());
        json.append(",\"delayMillis\":").append(options.delayMillis());
        json.append(",\"durabilities\":")
                .append(strings(options.durabilities().stream().map(Enum::name).toList()));
        json.append(",\"negativeControl\":").append(RecoveryReport.quote(options.negativeControl()));
        json.append(",\"output\":").append(RecoveryReport.quote(options.output().toString()));
        json.append(",\"workDir\":")
                .append(RecoveryReport.quote(options.workDir().toString()))
                .append("},");
        json.append("\n  \"reproduction\":{\"command\":")
                .append(RecoveryReport.quote(reproductionCommand(options)))
                .append(",\"args\":")
                .append(strings(reproductionArguments(options)))
                .append("},");
        json.append("\n  \"invariants\":[");
        for (int index = 0; index < INVARIANTS.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            final Invariant invariant = INVARIANTS.get(index);
            json.append("{\"id\":").append(RecoveryReport.quote(invariant.id()));
            json.append(",\"scenario\":").append(RecoveryReport.quote(invariant.scenario()));
            json.append(",\"source\":").append(RecoveryReport.quote(invariant.source()));
            json.append(",\"test\":").append(RecoveryReport.quote(invariant.test()));
            json.append(",\"retainedContract\":")
                    .append(RecoveryReport.quote(invariant.contract()))
                    .append('}');
        }
        json.append("],\n  \"results\":[");
        for (int index = 0; index < results.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            appendResult(json, results.get(index));
        }
        json.append(']');
        json.append(",\n  \"failed\":").append(failureCount(results, diagnostics));
        json.append(",\n  \"diagnostics\":").append(strings(diagnostics));
        json.append("\n}\n");
        Files.writeString(options.output(), json.toString());
    }

    private static void appendResult(final StringBuilder json, final SoakResult result) {
        json.append("{\"durability\":")
                .append(RecoveryReport.quote(result.durability().name()));
        json.append(",\"round\":").append(result.round());
        json.append(",\"status\":").append(RecoveryReport.quote(result.passed() ? "SUCCESS" : "FAILURE"));
        json.append(",\"published\":").append(result.published());
        json.append(",\"delivered\":").append(result.delivered());
        json.append(",\"elapsedNanos\":").append(result.elapsedNanos());
        json.append(",\"violations\":[");
        for (int index = 0; index < result.violations().size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            final SoakViolation violation = result.violations().get(index);
            json.append("{\"invariant\":").append(RecoveryReport.quote(violation.invariant()));
            json.append(",\"scenario\":").append(RecoveryReport.quote(violation.scenario()));
            json.append(",\"seed\":").append(violation.seed());
            json.append(",\"round\":").append(violation.round());
            json.append(",\"messageIds\":").append(numbers(violation.messageIds()));
            json.append(",\"observations\":")
                    .append(stringMap(violation.observations()))
                    .append('}');
        }
        json.append("]}");
    }

    private static int failureCount(final List<SoakResult> results, final List<String> diagnostics) {
        return (int) results.stream().filter(result -> !result.passed()).count() + diagnostics.size();
    }

    private static String fileSystem(final Path path) {
        try {
            return Files.getFileStore(path).type();
        } catch (final IOException exception) {
            return "unknown";
        }
    }

    private static String productVersion() {
        final String version = Smq.class.getPackage().getImplementationVersion();
        return version == null ? System.getProperty("smq.version", "unknown") : version;
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

    private static List<String> reproductionArguments(final SoakOptions options) {
        return List.of(
                "soak",
                "--seed",
                Long.toString(options.seed()),
                "--budget",
                Integer.toString(options.budget()),
                "--delay-ms",
                Integer.toString(options.delayMillis()),
                "--durability",
                durabilityOption(options),
                "--negative-control",
                options.negativeControl(),
                "--output",
                options.output().toString(),
                "--work-dir",
                options.workDir().toString());
    }

    private static String durabilityOption(final SoakOptions options) {
        return options.durabilities().size() == Durability.values().length
                ? "BOTH"
                : options.durabilities().getFirst().name();
    }

    private static String reproductionCommand(final SoakOptions options) {
        final String args = reproductionArguments(options).stream()
                .map(SoakReport::commandArgument)
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

    private static String strings(final List<String> values) {
        return values.stream().map(RecoveryReport::quote).collect(Collectors.joining(",", "[", "]"));
    }

    private static String numbers(final List<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
    }

    private static String stringMap(final Map<String, String> values) {
        final StringBuilder json = new StringBuilder("{");
        int index = 0;
        for (final var entry : values.entrySet()) {
            if (index++ > 0) {
                json.append(',');
            }
            json.append(RecoveryReport.quote(entry.getKey()))
                    .append(':')
                    .append(RecoveryReport.quote(entry.getValue()));
        }
        return json.append('}').toString();
    }
}
