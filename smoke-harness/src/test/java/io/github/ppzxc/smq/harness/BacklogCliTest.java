package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BacklogCliTest {

    @TempDir
    Path directory;

    @Test
    void backlogCommandComparesDepthsAndReportsMetricsThroughThePackagedModule() throws Exception {
        final Path report = this.directory.resolve("backlog.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "backlog",
                        "--durability",
                        "BOTH",
                        "--backlog-count",
                        "8",
                        "--duration-ms",
                        "40",
                        "--warmup-ms",
                        "0",
                        "--repeat",
                        "1",
                        "--payload-bytes",
                        "32",
                        "--batch-size",
                        "2",
                        "--chunk-size",
                        "2",
                        "--publishers",
                        "1",
                        "--consumers",
                        "1",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("backlog-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"scenario\":\"backlog\""), json);
        assertTrue(json.contains("\"backlogCount\":8"), json);
        assertTrue(json.contains("\"durationMillis\":40"), json);
        assertTrue(json.contains("\"publishers\":1"), json);
        assertTrue(json.contains("\"consumers\":1"), json);
        assertTrue(json.contains("\"queueKind\":\"DEFAULT\""), json);
        assertTrue(json.contains("\"queueKind\":\"MAX_DELIVERIES\""), json);
        assertTrue(json.contains("\"queueKind\":\"SINGLE_ACTIVE_CONSUMER\""), json);
        assertTrue(json.contains("\"queueKind\":\"LEASED_HEAD\""), json);
        assertTrue(json.contains("\"maxDeliveries\":5"), json);
        assertTrue(json.contains("\"singleActiveConsumer\":true"), json);
        assertTrue(json.contains("\"leasedHead\":true"), json);
        assertTrue(json.contains("\"startDepth\":8"), json);
        assertTrue(json.contains("\"endDepth\":"), json);
        assertTrue(json.contains("\"publishThroughputMessagesPerSecond\":"), json);
        assertTrue(json.contains("\"deliveryLatencyP50Nanos\":"), json);
        assertTrue(json.contains("\"deliveryLatencyP99Nanos\":"), json);
        assertTrue(json.contains("\"settleThroughputMessagesPerSecond\":"), json);
        assertTrue(json.contains("\"depthRatios\":["), json);
        assertTrue(json.contains("\"publishThroughput\":"), json);
        assertTrue(json.contains("\"settleThroughput\":"), json);
        assertTrue(json.contains("\"reproduction\":{"), json);
        assertEquals(16, json.split("\\\"databasePath\\\"").length - 1, json);
    }

    @Test
    void backlogCanRunOneIsolatedConditionAndReportsHostAndFilesystem() throws Exception {
        final Path report = this.directory.resolve("single-backlog.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "backlog",
                        "--durability",
                        "PROCESS_CRASH",
                        "--queue-kind",
                        "DEFAULT",
                        "--depth",
                        "backlog",
                        "--backlog-count",
                        "8",
                        "--duration-ms",
                        "40",
                        "--warmup-ms",
                        "0",
                        "--repeat",
                        "1",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("single-backlog-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"host\":"), json);
        assertTrue(json.contains("\"workDirFileSystem\":"), json);
        assertFalse(json.contains("\"host\":\"unknown\""), json);
        assertFalse(json.contains("\"workDirFileSystem\":\"unknown\""), json);
        assertTrue(json.contains("\"durability\":\"PROCESS_CRASH\""), json);
        assertTrue(json.contains("\"queueKind\":\"DEFAULT\""), json);
        assertTrue(json.contains("\"preloadedMessages\":8"), json);
        assertEquals(1, json.split("\\\"databasePath\\\"").length - 1, json);
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
