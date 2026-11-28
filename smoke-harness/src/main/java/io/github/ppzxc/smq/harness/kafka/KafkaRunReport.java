package io.github.ppzxc.smq.harness.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class KafkaRunReport {

    private final Instant startedAt = Instant.now();
    private final List<Map<String, Object>> brokerConfigurations = new ArrayList<>();
    private final List<Map<String, Object>> measurements = new ArrayList<>();
    private final List<Map<String, Object>> recoveryObservations = new ArrayList<>();
    private final List<String> dataVolumes = new ArrayList<>();
    private final List<String> diagnostics = new ArrayList<>();
    private String serverVersion = "unknown";

    void setServerVersion(final String version) {
        if (!"unknown".equals(this.serverVersion) && !this.serverVersion.equals(version)) {
            throw new IllegalStateException("Kafka server version changed during the run: " + version);
        }
        this.serverVersion = version;
    }

    void addBrokerConfiguration(final Map<String, Object> configuration) {
        this.brokerConfigurations.add(configuration);
    }

    void addMeasurement(final Map<String, Object> measurement) {
        this.measurements.add(measurement);
    }

    void addRecoveryObservation(final Map<String, Object> observation) {
        this.recoveryObservations.add(observation);
    }

    void addDataVolume(final String volume) {
        this.dataVolumes.add(volume);
    }

    void addDiagnostic(final String diagnostic) {
        this.diagnostics.add(diagnostic);
    }

    int unknownRequests() {
        return (int) this.recoveryObservations.stream()
                .filter(observation -> "request-in-flight".equals(observation.get("scenario")))
                .mapToInt(observation -> ((Number) observation.getOrDefault("unknownRequests", 0)).intValue())
                .sum();
    }

    void write(final Path output, final KafkaRunOptions options, final String status) throws IOException {
        final Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("schemaVersion", 1);
        root.put("status", status);
        root.put("serverImage", KafkaPlan.SERVER_IMAGE);
        root.put("serverVersion", this.serverVersion);
        root.put("clientVersion", KafkaPlan.clientVersion());
        root.put("startedAt", this.startedAt.toString());
        root.put("finishedAt", Instant.now().toString());
        root.put(
                "configuration",
                Map.of(
                        "replicationFactor", 1,
                        "min.insync.replicas", "1",
                        "acks", "all",
                        "settlementMethod", "consumer-group offset commit response",
                        "brokerDataDirectory", "/var/lib/kafka/data",
                        "sameDataDirectoryAcrossRestart", true,
                        "dataVolumes", List.copyOf(this.dataVolumes)));
        root.put(
                "options",
                Map.of(
                        "count", options.count(),
                        "warmup", options.warmup(),
                        "repeat", options.repeat(),
                        "batchSize", options.batchSize(),
                        "payloadBytes", options.payloadBytes(),
                        "port", options.port(),
                        "workDir", options.workDir().toString()));
        root.put("brokerConfigurations", List.copyOf(this.brokerConfigurations));
        root.put("measurements", List.copyOf(this.measurements));
        root.put("recoveryObservations", List.copyOf(this.recoveryObservations));
        root.put("unknownRequests", this.unknownRequests());
        root.put(
                "reproduction",
                Map.of(
                        "command",
                        "./gradlew -p smoke-harness run --args='kafka run ...'",
                        "flushConfigurations",
                        List.of("default", "flush-every-message")));
        root.put("diagnostics", List.copyOf(this.diagnostics));
        Files.writeString(output, KafkaJson.encode(root) + System.lineSeparator());
    }
}
