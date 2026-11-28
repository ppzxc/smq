package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SoakCliTest {

    @TempDir
    Path directory;

    @Test
    void seededSoakRunsAndReportsRepeatableInvariantScenariosThroughThePackagedModule() throws Exception {
        final Path firstReport = this.directory.resolve("first.json");
        final Path secondReport = this.directory.resolve("second.json");
        final Process first = this.soakProcess(firstReport, this.directory.resolve("first-work"));
        final String firstOutput = new String(first.getInputStream().readAllBytes());

        assertEquals(0, first.waitFor(), firstOutput);
        final String firstJson = java.nio.file.Files.readString(firstReport);
        assertTrue(firstJson.contains("\"schemaVersion\":1"), firstJson);
        assertTrue(firstJson.contains("\"status\":\"SUCCESS\""), firstJson);
        assertTrue(firstJson.contains("\"seed\":42"), firstJson);
        assertTrue(firstJson.contains("\"budget\":2"), firstJson);
        assertTrue(firstJson.contains("\"round\":1"), firstJson);
        assertTrue(firstJson.contains("\"round\":2"), firstJson);
        assertTrue(firstJson.contains("\"workloadDigest\":"), firstJson);
        assertTrue(firstJson.contains("LOSS"), firstJson);
        assertTrue(firstJson.contains("GHOST_DELIVERY"), firstJson);
        assertTrue(firstJson.contains("DUPLICATE_LIMIT"), firstJson);
        assertTrue(firstJson.contains("DEAD_LETTER_MOVEMENT"), firstJson);
        assertTrue(firstJson.contains("DELAY"), firstJson);
        assertTrue(firstJson.contains("SINGLE_ACTIVE_CONSUMER"), firstJson);
        assertTrue(firstJson.contains("APPROXIMATE_COUNTS"), firstJson);
        assertTrue(firstJson.contains("CHUNK_ORDER"), firstJson);

        final Process second = this.soakProcess(secondReport, this.directory.resolve("second-work"));
        final String secondOutput = new String(second.getInputStream().readAllBytes());
        assertEquals(0, second.waitFor(), secondOutput);
        final String secondJson = java.nio.file.Files.readString(secondReport);
        assertEquals(jsonValue(firstJson, "workloadDigest"), jsonValue(secondJson, "workloadDigest"));
    }

    @Test
    void seededSoakReturnsFailureWithInvariantSeedMessageAndObservationDetails() throws Exception {
        final Path report = this.directory.resolve("negative.json");
        final Process process = new ProcessBuilder(this.javaCommand(
                        "soak",
                        "--seed",
                        "43",
                        "--budget",
                        "1",
                        "--delay-ms",
                        "20",
                        "--durability",
                        "PROCESS_CRASH",
                        "--negative-control",
                        "drop-first-delivery",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        this.directory.resolve("negative-work").toString()))
                .redirectErrorStream(true)
                .start();
        final String output = new String(process.getInputStream().readAllBytes());

        assertEquals(1, process.waitFor(), output);
        final String json = java.nio.file.Files.readString(report);
        assertTrue(json.contains("\"status\":\"FAILURE\""), json);
        assertTrue(json.contains("\"failed\":1"), json);
        assertTrue(json.contains("\"invariant\":\"LOSS\""), json);
        assertTrue(json.contains("\"seed\":43"), json);
        assertTrue(json.contains("\"messageIds\":["), json);
        assertTrue(json.contains("\"observations\":{"), json);
        assertTrue(json.contains("publishedIds"), json);
        assertTrue(json.contains("observedDeliveryIds"), json);
    }

    private Process soakProcess(final Path report, final Path workDir) throws Exception {
        return new ProcessBuilder(this.javaCommand(
                        "soak",
                        "--seed",
                        "42",
                        "--budget",
                        "2",
                        "--delay-ms",
                        "20",
                        "--durability",
                        "PROCESS_CRASH",
                        "--output",
                        report.toString(),
                        "--work-dir",
                        workDir.toString()))
                .redirectErrorStream(true)
                .start();
    }

    private static String jsonValue(final String json, final String key) {
        final String marker = "\"" + key + "\":\"";
        final int start = json.indexOf(marker);
        if (start < 0) {
            throw new AssertionError("missing " + key + " in " + json);
        }
        final int valueStart = start + marker.length();
        final int valueEnd = json.indexOf('"', valueStart);
        return json.substring(valueStart, valueEnd);
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
