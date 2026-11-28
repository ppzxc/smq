package io.github.ppzxc.smq.harness.kafka;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;

public final class KafkaRunner {

    public int run(final KafkaRunOptions options) {
        final KafkaRunReport report = new KafkaRunReport();
        try {
            for (final KafkaFlushMode flushMode : KafkaFlushMode.all()) {
                final KafkaDockerBroker broker = new KafkaDockerBroker(options.port());
                report.addDataVolume(broker.volumeName());
                try {
                    broker.create();
                    KafkaClientSupport.awaitReady(broker);
                    final String serverVersion = broker.serverVersion();
                    report.setServerVersion(serverVersion);
                    if (!"4.4.0".equals(serverVersion)) {
                        throw new IllegalStateException("Expected Kafka server 4.4.0, observed " + serverVersion);
                    }
                    try (AdminClient admin =
                            KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-config-inspector")) {
                        final Map<String, Object> configuration =
                                new LinkedHashMap<>(KafkaClientSupport.brokerConfiguration(
                                        admin, broker.bootstrapServers(), broker.volumeName()));
                        configuration.put("flushConfiguration", flushMode.label());
                        configuration.put("serverVersion", serverVersion);
                        report.addBrokerConfiguration(configuration);
                    }
                    for (int repeat = 1; repeat <= options.repeat(); repeat++) {
                        for (final String workload : new String[] {"single", "batch"}) {
                            report.addMeasurement(
                                    KafkaWorkloadRunner.measure(broker, options, flushMode, workload, repeat));
                        }
                    }
                    for (final Map<String, Object> observation : KafkaRecoveryRunner.run(
                            broker, flushMode, Path.of(options.workDir().toString(), flushMode.label()))) {
                        report.addRecoveryObservation(observation);
                    }
                } finally {
                    broker.close();
                }
            }
            report.write(options.output(), options, "SUCCESS");
            System.out.println("Kafka workload and recovery completed: " + options.output());
            return 0;
        } catch (final Exception exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            report.addDiagnostic(exception.toString());
            System.err.println("Kafka workload or recovery failed: " + exception.getMessage());
            try {
                report.write(options.output(), options, "FAILURE");
            } catch (final Exception reportException) {
                System.err.println("could not write Kafka failure report: " + reportException.getMessage());
            }
            return 1;
        }
    }
}
