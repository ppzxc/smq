package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

final class StorageLossReport {

    private StorageLossReport() {}

    static void write(
            final StorageLossOptions options,
            final List<StorageLossResult> results,
            final String status,
            final String runDir,
            final String ledgerPath,
            final String notRunReason,
            final String diagnostic)
            throws IOException {
        Files.createDirectories(options.output().getParent());
        final StringBuilder json = new StringBuilder(3072);
        json.append("{\n  \"schemaVersion\":1,");
        json.append("\n  \"status\":").append(RecoveryReport.quote(status)).append(',');
        json.append("\n  \"faultModel\":")
                .append(RecoveryReport.quote(StorageLossResult.SIMULATED_STORAGE_LOSS))
                .append(',');
        json.append("\n  \"storageModel\":{\"operation\":\"restore-last-synchronized-snapshot\",");
        json.append("\"scope\":\"throwaway-work-directory\",");
        json.append("\"snapshotSource\":\"public Smq.snapshot API after successful POWER_LOSS operation\",");
        json.append("\"restoreAction\":\"copy snapshot over isolated database and discard WAL/SHM sidecars\",");
        json.append("\"syncBoundary\":\"POWER_LOSS successful operation plus controller snapshot\",");
        json.append("\"unsyncedWrite\":\"PROCESS_CRASH successful operation after snapshot\"},");
        json.append("\n  \"physicalPowerCut\":\"NOT_PERFORMED\",");
        json.append("\n  \"physicalPowerCutReason\":\"not initiated by the application harness\",");
        json.append("\n  \"observationCoverage\":{\"jvmTermination\":")
                .append(RecoveryReport.quote(hasJvmTermination(results) ? "EXECUTED" : "NOT_RUN"))
                .append(',');
        json.append("\"simulatedStorageLoss\":")
                .append(RecoveryReport.quote(simulationCoverage(results)))
                .append(',');
        json.append("\"physicalPowerCut\":\"NOT_PERFORMED\",");
        json.append("\"completedOperationResponses\":")
                .append(completedResponses(results))
                .append(',');
        json.append("\"recoveredScenarios\":").append(passedScenarios(results)).append("},");
        json.append("\n  \"config\":{\"durabilities\":")
                .append(strings(options.durabilities().stream().map(Enum::name).toList()));
        json.append(",\"leaseMillis\":").append(options.leaseMillis());
        json.append(",\"simulateStorageLoss\":").append(options.simulateStorageLoss());
        json.append(",\"requireStorageLoss\":").append(options.requireStorageLoss());
        json.append(",\"negativeControl\":").append(RecoveryReport.quote(options.negativeControl()));
        json.append(",\"workDir\":")
                .append(RecoveryReport.quote(options.workDir().toString()));
        json.append(",\"output\":")
                .append(RecoveryReport.quote(options.output().toString()))
                .append("},");
        json.append("\n  \"runDir\":").append(RecoveryReport.quote(runDir)).append(',');
        json.append("\n  \"completionLedger\":")
                .append(RecoveryReport.quote(ledgerPath))
                .append(',');
        json.append("\n  \"javaVersion\":")
                .append(RecoveryReport.quote(Runtime.version().toString()))
                .append(',');
        json.append("\n  \"smqVersion\":")
                .append(RecoveryReport.quote(System.getProperty("smq.version", "unknown")))
                .append(',');
        json.append("\n  \"reproduction\":{\"command\":")
                .append(RecoveryReport.quote(reproductionCommand(options)))
                .append(",\"args\":")
                .append(strings(reproductionArguments(options)))
                .append("},");
        json.append("\n  \"notRunReason\":")
                .append(notRunReason == null ? "null" : RecoveryReport.quote(notRunReason))
                .append(',');
        json.append("\n  \"results\":[");
        for (int index = 0; index < results.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            final StorageLossResult result = results.get(index);
            json.append("{\"scenario\":").append(RecoveryReport.quote(result.scenario));
            json.append(",\"faultType\":").append(RecoveryReport.quote(result.faultType));
            json.append(",\"durability\":").append(RecoveryReport.quote(result.durability.name()));
            json.append(",\"requestId\":").append(RecoveryReport.quote(result.requestId));
            json.append(",\"completionRecord\":").append(RecoveryReport.quote(result.completionRecord));
            json.append(",\"recoveryObservation\":").append(RecoveryReport.quote(result.recoveryObservation));
            json.append(",\"status\":").append(RecoveryReport.quote(result.status));
            json.append(",\"shutdownAt\":")
                    .append(result.shutdownAt == null ? "null" : RecoveryReport.quote(result.shutdownAt.toString()));
            json.append('}');
        }
        json.append(']');
        json.append(",\n  \"failed\":").append("FAILURE".equals(status) ? 1 : 0);
        json.append(",\n  \"diagnostics\":")
                .append(diagnostic == null ? "[]" : "[" + RecoveryReport.quote(diagnostic) + "]");
        json.append("\n}\n");
        Files.writeString(options.output(), json.toString());
    }

    private static String strings(final List<String> values) {
        return values.stream().map(RecoveryReport::quote).collect(Collectors.joining(",", "[", "]"));
    }

    private static boolean hasJvmTermination(final List<StorageLossResult> results) {
        return results.stream().anyMatch(result -> result.shutdownAt != null);
    }

    private static String simulationCoverage(final List<StorageLossResult> results) {
        if (results.stream()
                .anyMatch(result -> StorageLossResult.SIMULATED_STORAGE_LOSS.equals(result.faultType)
                        && List.of("PASSED", "FAILED", "NEGATIVE_CONTROL").contains(result.status))) {
            return "EXECUTED";
        }
        if (results.stream()
                .anyMatch(result -> StorageLossResult.SIMULATED_STORAGE_LOSS.equals(result.faultType)
                        && "PENDING".equals(result.status))) {
            return "ATTEMPTED";
        }
        return "NOT_RUN";
    }

    private static long completedResponses(final List<StorageLossResult> results) {
        return results.stream()
                .filter(result -> !result.completionRecord.isEmpty())
                .count();
    }

    private static long passedScenarios(final List<StorageLossResult> results) {
        return results.stream().filter(result -> "PASSED".equals(result.status)).count();
    }

    private static List<String> reproductionArguments(final StorageLossOptions options) {
        final List<String> args = new java.util.ArrayList<>(List.of("storage-loss", "--durability"));
        args.add(
                options.durabilities().size() == Durability.values().length
                        ? "BOTH"
                        : options.durabilities().getFirst().name());
        args.add("--simulate-storage-loss");
        args.add(Boolean.toString(options.simulateStorageLoss()));
        args.add("--require-storage-loss");
        args.add(Boolean.toString(options.requireStorageLoss()));
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

    private static String reproductionCommand(final StorageLossOptions options) {
        final String args = reproductionArguments(options).stream()
                .map(StorageLossReport::commandArgument)
                .collect(Collectors.joining(" "));
        return "./gradlew smokeHarness -Psmoke.args=\""
                + args.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String commandArgument(final String argument) {
        if (argument.chars()
                .allMatch(character -> Character.isLetterOrDigit(character) || "_./:-".indexOf(character) >= 0)) {
            return argument;
        }
        return "\"" + argument.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
