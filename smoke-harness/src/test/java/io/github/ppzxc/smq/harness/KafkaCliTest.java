package io.github.ppzxc.smq.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

class KafkaCliTest {

    @TempDir
    Path directory;

    @Test
    void kafkaPlanReportsPinnedSingleBrokerAndFlushConfigurationsWithoutDocker() throws Exception {
        final Process process = new ProcessBuilder(this.javaCommand("kafka", "plan"))
                .redirectErrorStream(true)
                .start();
        final String output = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), output);
        assertTrue(output.contains("\"serverImage\":\"apache/kafka:4.4.0\""), output);
        assertTrue(output.contains("\"clientVersion\":\"4.4.0\""), output);
        assertTrue(output.contains("\"replicationFactor\":1"), output);
        assertTrue(output.contains("\"min.insync.replicas\":\"1\""), output);
        assertTrue(output.contains("\"acks\":\"all\""), output);
        assertTrue(output.contains("\"flush.messages\":\"9223372036854775807\""), output);
        assertTrue(output.contains("\"flush.messages\":\"1\""), output);
        assertTrue(output.contains("\"requestInFlightDisposition\":\"UNKNOWN\""), output);
    }

    @Test
    void kafkaRunRejectsInvalidWorkloadOptionsBeforeContactingDocker() throws Exception {
        final Process process = new ProcessBuilder(this.javaCommand("kafka", "run", "--count", "0"))
                .redirectErrorStream(true)
                .start();
        final String output = new String(process.getInputStream().readAllBytes());

        assertEquals(2, process.waitFor(), output);
        assertTrue(output.contains("--count must be greater than zero"), output);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SMQ_KAFKA_DOCKER_TESTS", matches = "true")
    void kafkaRunMeasuresBothFlushModesAndChecksFourRecoveryWindowsAgainstTheSameDataVolumes() throws Exception {
        final Path report = this.directory.resolve("kafka-report.json");
        final Path workDir = this.directory.resolve("kafka-work");
        final int port = this.unusedPort();
        final Process process = new ProcessBuilder(this.javaCommand(
                        "kafka",
                        "run",
                        "--count",
                        "2",
                        "--warmup",
                        "0",
                        "--repeat",
                        "1",
                        "--batch-size",
                        "2",
                        "--payload-bytes",
                        "16",
                        "--port",
                        Integer.toString(port),
                        "--work-dir",
                        workDir.toString(),
                        "--output",
                        report.toString()))
                .redirectErrorStream(true)
                .start();
        final String diagnostics = new String(process.getInputStream().readAllBytes());

        assertEquals(0, process.waitFor(), diagnostics);
        final String json = Files.readString(report);
        assertTrue(json.contains("\"status\":\"SUCCESS\""), json);
        assertTrue(json.contains("\"serverVersion\":\"4.4.0\""), json);
        assertTrue(json.contains("\"clientVersion\":\"4.4.0\""), json);
        assertTrue(
                json.contains(
                        "\"flush.messages\":{\"value\":\"9223372036854775807\",\"source\":\"DEFAULT_CONFIG\",\"isDefault\":true}"),
                json);
        assertTrue(
                json.contains(
                        "\"flush.messages\":{\"value\":\"1\",\"source\":\"DYNAMIC_TOPIC_CONFIG\",\"isDefault\":false}"),
                json);
        assertTrue(json.contains("\"confirmedPublishes\":2"), json);
        assertTrue(json.contains("\"settlementMethod\":\"consumer-group offset commit response\""), json);
        assertTrue(json.contains("\"lagAfter\":0"), json);
        assertTrue(json.contains("\"scenario\":\"publish-confirmed\""), json);
        assertTrue(json.contains("\"scenario\":\"delivery-before-commit\""), json);
        assertTrue(json.contains("\"groupOffsetsBeforeStop\":{}"), json);
        assertTrue(json.contains("\"groupOffsetsAfterRestart\":{}"), json);
        assertTrue(json.contains("\"scenario\":\"commit-confirmed\""), json);
        assertTrue(json.contains("\"committedOffsetAfterRestart\":1"), json);
        assertTrue(json.contains("\"groupOffsetsAfterRestart\":{\"smq_harness_commit_confirmed_"), json);
        assertTrue(json.contains("\"scenario\":\"request-in-flight\""), json);
        assertTrue(json.contains("\"classification\":\"UNKNOWN\""), json);
        assertTrue(json.contains("\"unknownRequests\":2"), json);
        assertTrue(json.contains("\"responseBeforeDisconnect\":false"), json);
        assertTrue(json.contains("\"restartReadiness\""), json);
        assertTrue(json.contains("\"controllerId\":1"), json);
        assertTrue(json.contains("\"leaderId\":1"), json);
        assertTrue(json.contains("\"replicas\":[1]"), json);
        assertTrue(json.contains("\"isr\":[1]"), json);
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

    private int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
