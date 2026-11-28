package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SmokeCliTest {

    @TempDir
    Path directory;

    @Test
    void rabbitPlanDescribesPinnedBrokerAndQueueConfigurationsWithoutDocker() throws Exception {
        final Path report = this.directory.resolve("rabbit-plan.json");
        final Process process = new ProcessBuilder(this.javaCommand("rabbit", "plan", "--output", report.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"rabbitmqVersion\":\"4.4.1\""), json);
        assertTrue(json.contains("\"javaClientVersion\":\"5.36.0\""), json);
        assertTrue(json.contains("\"queueType\":\"classic\""), json);
        assertTrue(json.contains("\"durable\":true"), json);
        assertTrue(json.contains("\"deliveryMode\":2"), json);
        assertTrue(json.contains("\"queueType\":\"quorum\""), json);
        assertTrue(json.contains("\"x-quorum-initial-group-size\":1"), json);
        assertTrue(json.contains("mandatory publishes with ReturnListener"), json);
        assertTrue(json.contains("consumer ACK sends are not publisher confirms"), json);
        assertTrue(json.contains("\"recoveryQueueStabilizationMillis\":2000"), json);
    }

    @Test
    void rabbitRunRejectsInvalidWorkloadBeforeConnectingToABroker() throws Exception {
        final Process process = new ProcessBuilder(this.javaCommand("rabbit", "run", "--count", "0"))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        assertTrue(diagnostics.contains("--count must be positive"), diagnostics);
    }

    @Test
    void rabbitRecoveryRejectsInvalidPortBeforeCallingDocker() throws Exception {
        final Process process = new ProcessBuilder(this.javaCommand("rabbit", "recovery", "--port", "0"))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        assertTrue(diagnostics.contains("--port must be positive"), diagnostics);
    }

    @Test
    void cliPublishesPullsSettlesAndWritesAReportThroughThePackagedModule() throws Exception {
        final Path report = this.directory.resolve("report.json");
        final Path database = this.directory.resolve("work");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "--durability",
                        "PROCESS_CRASH",
                        "--mode",
                        "single",
                        "--count",
                        "3",
                        "--warmup",
                        "1",
                        "--repeat",
                        "1",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        database.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"schemaVersion\":1"), json);
        assertTrue(json.contains("\"durability\":\"PROCESS_CRASH\""), json);
        assertTrue(json.contains("\"completed\":3"), json);
        assertTrue(json.contains("\"failed\":0"), json);
        assertTrue(json.contains("\"productVersion\":\"0.1.0-SNAPSHOT\""), json);
        assertTrue(json.contains("\"fileSystem\":"), json);
        assertTrue(json.contains("\"reproduction\":{\"command\":"), json);
        assertTrue(json.contains("\"rawSamples\":{\"publishLatencyNanos\":["), json);
        assertTrue(json.contains("\"settleLatencyNanos\":["), json);
        assertTrue(!json.contains("\"revision\":\"unknown\""), json);
    }

    @Test
    void cliRunsSingleAndBoundedBatchWorkloadsForBothDurabilityModes() throws Exception {
        final Path report = this.directory.resolve("both.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "--durability",
                        "BOTH",
                        "--mode",
                        "both",
                        "--count",
                        "5",
                        "--warmup",
                        "2",
                        "--repeat",
                        "2",
                        "--payload-bytes",
                        "64",
                        "--batch-size",
                        "3",
                        "--max-in-flight",
                        "4",
                        "--chunk-size",
                        "2",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("both-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"schemaVersion\":1"), json);
        assertTrue(json.contains("\"durability\":\"PROCESS_CRASH\""), json);
        assertTrue(json.contains("\"durability\":\"POWER_LOSS\""), json);
        assertTrue(json.contains("\"mode\":\"single\""), json);
        assertTrue(json.contains("\"mode\":\"batch\""), json);
        assertTrue(json.contains("\"batchSize\":3"), json);
        assertTrue(json.contains("\"maxInFlight\":4"), json);
        assertTrue(json.contains("\"chunkSize\":2"), json);
        assertTrue(json.contains("\"payloadBytes\":64"), json);
        assertTrue(json.contains("\"maxObservedBatchSize\":1"), json);
        assertTrue(json.contains("\"maxObservedBatchSize\":3"), json);
        assertTrue(json.contains("\"maxObservedInFlight\":4"), json);
        assertTrue(json.contains("\"publishLatencyP50Nanos\":"), json);
        assertTrue(json.contains("\"settleLatencyP99Nanos\":"), json);
        assertTrue(json.contains("\"throughputMessagesPerSecond\":"), json);
        assertEquals(8, json.split("\\\"repeat\\\":").length - 1, json);
    }

    @Test
    void recoveryCommandVerifiesCrashRecoveryForBothDurabilityModes() throws Exception {
        final Path report = this.directory.resolve("recovery.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "recovery",
                        "--durability",
                        "BOTH",
                        "--message-count",
                        "3",
                        "--lease-ms",
                        "100",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("recovery-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"SUCCESS\""), json);
        assertTrue(json.contains("\"durability\":\"PROCESS_CRASH\""), json);
        assertTrue(json.contains("\"durability\":\"POWER_LOSS\""), json);
        assertTrue(json.contains("\"classification\":\"CONFIRMED\""), json);
        assertTrue(json.contains("\"correlationId\":"), json);
        assertTrue(json.contains("\"recoveryObservation\":"), json);
        assertTrue(json.contains("\"shutdownAt\":"), json);
        assertTrue(json.contains("\"reproduction\":{\"command\":"), json);
    }

    @Test
    void recoveryCommandKeepsAcknowledgedMessagesDeletedAndRedeliversExpiredLeases() throws Exception {
        final Path report = this.directory.resolve("recovery-settlement.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "recovery",
                        "--durability",
                        "BOTH",
                        "--message-count",
                        "3",
                        "--lease-ms",
                        "100",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("recovery-settlement-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"scenario\":\"confirmed-settle\""), json);
        assertTrue(json.contains("\"recoveryObservation\":\"no redelivery after successful ACK\""), json);
        assertTrue(json.contains("\"scenario\":\"unacked-delivery\""), json);
        assertTrue(json.contains("redelivered correlation"), json);
        assertTrue(json.contains("\"durability\":\"PROCESS_CRASH\""), json);
        assertTrue(json.contains("\"durability\":\"POWER_LOSS\""), json);
    }

    @Test
    void recoveryNegativeControlReturnsFailureWhenConfirmedPublishIsRemoved() throws Exception {
        final Path report = this.directory.resolve("recovery-negative.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "recovery",
                        "--durability",
                        "PROCESS_CRASH",
                        "--negative-control",
                        "drop-confirmed-publish",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("recovery-negative-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"FAILURE\""), json);
        assertTrue(json.contains("confirmed publish was not recovered"), json);
    }

    @Test
    void recoveryCommandClassifiesInterruptedBatchesAsUnknownAndChecksAtomicity() throws Exception {
        final Path report = this.directory.resolve("recovery-atomicity.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "recovery",
                        "--durability",
                        "BOTH",
                        "--message-count",
                        "8",
                        "--lease-ms",
                        "100",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("recovery-atomicity-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"scenario\":\"ambiguous-publish-batch\""), json);
        assertTrue(json.contains("\"scenario\":\"ambiguous-settle-batch\""), json);
        assertTrue(json.contains("\"classification\":\"UNKNOWN\""), json);
        assertTrue(json.contains("\"requestStarted\":\"START|"), json);
        assertTrue(
                json.contains("publish batch absent; no messages committed")
                        || json.contains("publish batch recovered all 8 messages"),
                json);
        assertTrue(
                json.contains("settlement batch applied; no messages redelivered")
                        || json.contains("settlement batch unapplied; all 8 messages redelivered"),
                json);
        assertTrue(json.contains("\"status\":\"SUCCESS\""), json);
    }

    @Test
    void operationFailureReturnsNonzeroAndWritesDiagnostics() throws Exception {
        final Path report = this.directory.resolve("failure.json");
        final Path work = this.directory.resolve("failure-work");
        final Path database = work.resolve("process_crash/single/measurement-1.db");
        Files.createDirectories(database.getParent());
        final Path lockPath = database.resolveSibling(database.getFileName() + ".lock");

        try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            final Process process = new ProcessBuilder(this.javaCommand(
                            "--durability",
                            "PROCESS_CRASH",
                            "--mode",
                            "single",
                            "--count",
                            "1",
                            "--warmup",
                            "0",
                            "--repeat",
                            "1",
                            "--output",
                            report.toString(),
                            "--work-dir",
                            work.toString()))
                    .redirectErrorStream(true)
                    .start();
            final String diagnostics = new String(process.getInputStream().readAllBytes());

            assertEquals(1, process.waitFor(), diagnostics);
            final String json = Files.readString(report);
            assertTrue(json.contains("\"status\":\"FAILURE\""), json);
            assertTrue(json.contains("\"failed\":1"), json);
            assertTrue(json.contains("StorageLockedException"), json);
            assertTrue(json.contains("\"diagnostics\":["), json);
        }
    }

    @Test
    void failureReportKeepsCompletedResultsFromEarlierScenarios() throws Exception {
        final Path report = this.directory.resolve("partial-failure.json");
        final Path work = this.directory.resolve("partial-failure-work");
        final Path database = work.resolve("process_crash/batch/measurement-1.db");
        Files.createDirectories(database.getParent());
        final Path lockPath = database.resolveSibling(database.getFileName() + ".lock");

        try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            final Process process = new ProcessBuilder(this.javaCommand(
                            "--durability",
                            "PROCESS_CRASH",
                            "--mode",
                            "both",
                            "--count",
                            "1",
                            "--warmup",
                            "0",
                            "--repeat",
                            "1",
                            "--output",
                            report.toString(),
                            "--work-dir",
                            work.toString()))
                    .redirectErrorStream(true)
                    .start();
            final String diagnostics = new String(process.getInputStream().readAllBytes());

            assertEquals(1, process.waitFor(), diagnostics);
            final String json = Files.readString(report);
            assertTrue(json.contains("\"completed\":1"), json);
            assertTrue(json.contains("\"mode\":\"single\""), json);
            assertTrue(json.contains("\"failed\":1"), json);
        }
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
