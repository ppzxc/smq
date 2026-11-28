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
