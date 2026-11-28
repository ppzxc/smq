package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageLossCliTest {

    @TempDir
    Path directory;

    @Test
    void storageLossCommandRecordsDurableRecoveryAndFaultBoundaries() throws Exception {
        final Path report = this.directory.resolve("storage-loss.json");
        final Path work = this.directory.resolve("storage-loss-work");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "storage-loss",
                        "--durability",
                        "BOTH",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        work.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"SUCCESS\""), json);
        assertTrue(json.contains("\"faultModel\":\"SIMULATED_STORAGE_LOSS\""), json);
        assertTrue(json.contains("\"physicalPowerCut\":\"NOT_PERFORMED\""), json);
        assertTrue(json.contains("\"physicalPowerCutReason\":\"not initiated by the application harness\""), json);
        assertTrue(json.contains("\"scenario\":\"power-loss-publish\""), json);
        assertTrue(json.contains("\"scenario\":\"power-loss-settlement\""), json);
        assertTrue(json.contains("no redelivery after successful POWER_LOSS settlement"), json);
        assertTrue(json.contains("\"scenario\":\"process-crash-publish\""), json);
        assertTrue(json.contains("\"scenario\":\"process-crash-settlement\""), json);
        assertTrue(json.contains("\"faultType\":\"JVM_TERMINATION\""), json);
        assertTrue(json.contains("\"faultType\":\"SIMULATED_STORAGE_LOSS\""), json);
        assertTrue(json.contains("successful publish recovered after JVM termination"), json);
        assertTrue(json.contains("no redelivery after successful settlement and JVM termination"), json);
        assertTrue(json.contains("\"observationCoverage\":"), json);
        assertTrue(json.contains("\"completionLedger\":"), json);
        assertTrue(json.contains("\"smqVersion\":\"0.1.0-SNAPSHOT\""), json);
        assertTrue(json.contains("\"reproduction\":{\"command\":"), json);
    }

    @Test
    void storageLossNegativeControlFailsWhenConfirmedPublishIsRemoved() throws Exception {
        final Path report = this.directory.resolve("storage-loss-negative.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "storage-loss",
                        "--durability",
                        "POWER_LOSS",
                        "--negative-control",
                        "drop-confirmed-publish",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("storage-loss-negative-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"FAILURE\""), json);
        assertTrue(json.contains("drop-confirmed-publish"), json);
        assertTrue(json.contains("\"simulatedStorageLoss\":\"EXECUTED\""), json);
        assertTrue(json.contains("\"status\":\"NEGATIVE_CONTROL\""), json);
        assertTrue(json.contains("confirmed publish was not recovered after storage loss"), json);
    }

    @Test
    void requiredStorageLossRunFailsAndRecordsWhyTheCheckDidNotRun() throws Exception {
        final Path report = this.directory.resolve("storage-loss-required-skip.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "storage-loss",
                        "--simulate-storage-loss",
                        "false",
                        "--require-storage-loss",
                        "true",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("storage-loss-required-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"FAILURE\""), json);
        assertTrue(json.contains("\"simulatedStorageLoss\":\"NOT_RUN\""), json);
        assertTrue(json.contains("simulated storage-loss run was disabled"), json);
        assertTrue(json.contains("\"physicalPowerCut\":\"NOT_PERFORMED\""), json);
    }

    @Test
    void optionalStorageLossSkipIsReportedWithoutClaimingJvmOrStorageObservation() throws Exception {
        final Path report = this.directory.resolve("storage-loss-optional-skip.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "storage-loss",
                        "--durability",
                        "POWER_LOSS",
                        "--simulate-storage-loss",
                        "false",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("storage-loss-optional-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"SKIPPED\""), json);
        assertTrue(json.contains("\"jvmTermination\":\"NOT_RUN\""), json);
        assertTrue(json.contains("\"simulatedStorageLoss\":\"NOT_RUN\""), json);
        assertTrue(json.contains("\"physicalPowerCut\":\"NOT_PERFORMED\""), json);
        assertTrue(json.contains("simulated storage-loss run was disabled"), json);
    }

    @Test
    void requiredStorageLossRunRecordsMissingWorkDirectoryAsNotRun() throws Exception {
        final Path report = this.directory.resolve("storage-loss-missing-work.json");
        final Path workFile = this.directory.resolve("work-is-a-file");
        Files.writeString(workFile, "not a directory");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "storage-loss",
                        "--require-storage-loss",
                        "true",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        workFile.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"FAILURE\""), json);
        assertTrue(json.contains("\"simulatedStorageLoss\":\"NOT_RUN\""), json);
        assertTrue(json.contains("simulated storage-loss verification did not run"), json);
    }

    @Test
    void simulatedStorageLossPreservesSyncedSnapshotAndDiscardsProcessOnlyWrite() throws Exception {
        final Path report = this.directory.resolve("storage-loss-model.json");
        final Path work = this.directory.resolve("storage-loss-model-work");
        final Process process = new ProcessBuilder(
                        this.javaCommand("storage-loss", "--output", report.toString(), "--work-dir", work.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"scenario\":\"simulated-storage-loss-synced-write\""), json);
        assertTrue(json.contains("baseline and POWER_LOSS write preserved in last synchronized snapshot"), json);
        assertTrue(json.contains("\"scenario\":\"simulated-storage-loss-synced-settlement\""), json);
        assertTrue(json.contains("POWER_LOSS settlement remained applied in last synchronized snapshot"), json);
        assertTrue(json.contains("\"scenario\":\"simulated-storage-loss-unsynced-write\""), json);
        assertTrue(json.contains("PROCESS_CRASH write discarded by restoring last synchronized snapshot"), json);
        final String ledger = json.replaceFirst("(?s).*\\\"completionLedger\\\":\\\"([^\\\"]+)\\\".*", "$1");
        final Path ledgerPath = Path.of(ledger);
        assertTrue(ledgerPath.startsWith(work), ledgerPath.toString());
        assertTrue(!ledgerPath.startsWith(ledgerPath.getParent().resolve("stores")), ledgerPath.toString());
        final List<String> completionRecords = Files.readAllLines(ledgerPath);
        assertEquals(8, completionRecords.size(), completionRecords.toString());
        assertTrue(
                completionRecords.stream()
                        .anyMatch(line ->
                                line.contains("|power-loss-settlement-simulation|SETTLE|settled-before-storage-loss|")),
                completionRecords.toString());
    }

    private List<String> javaCommand(final String... arguments) {
        final String java =
                Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String modulePath = System.getProperty("smoke.modulePath");
        return Stream.concat(
                        List.of(
                                java,
                                "-Dsmq.version=" + System.getProperty("smoke.smqVersion"),
                                "-Dsmoke.modulePath=" + modulePath,
                                "--enable-native-access=org.xerial.sqlitejdbc",
                                "--module-path",
                                modulePath,
                                "-m",
                                "io.github.ppzxc.smq.harness/io.github.ppzxc.smq.harness.Main")
                                .stream(),
                        List.of(arguments).stream())
                .toList();
    }
}
