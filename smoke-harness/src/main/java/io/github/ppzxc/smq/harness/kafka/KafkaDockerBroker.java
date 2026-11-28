package io.github.ppzxc.smq.harness.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

final class KafkaDockerBroker implements AutoCloseable {

    static final String DATA_DIRECTORY = "/var/lib/kafka/data";
    private static final Duration DOCKER_COMMAND_TIMEOUT = Duration.ofSeconds(30);

    private final int hostPort;
    private final String containerName;
    private final String volumeName;
    private boolean volumeCreated;
    private boolean containerCreated;

    KafkaDockerBroker(final int hostPort) {
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        this.hostPort = hostPort;
        this.containerName = "smq-kafka-harness-" + suffix;
        this.volumeName = "smq-kafka-harness-" + suffix;
    }

    String bootstrapServers() {
        return "localhost:" + this.hostPort;
    }

    String volumeName() {
        return this.volumeName;
    }

    String containerName() {
        return this.containerName;
    }

    String dataDirectory() {
        return DATA_DIRECTORY;
    }

    void create() throws IOException, InterruptedException {
        this.docker("volume", "create", this.volumeName);
        this.volumeCreated = true;

        final List<String> command = new ArrayList<>(List.of(
                "run",
                "--detach",
                "--name",
                this.containerName,
                "--hostname",
                "broker",
                "--publish",
                "127.0.0.1:" + this.hostPort + ":19092",
                "--mount",
                "type=volume,source=" + this.volumeName + ",target=" + DATA_DIRECTORY));
        this.addEnvironment(command, "KAFKA_NODE_ID", "1");
        this.addEnvironment(
                command,
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT");
        this.addEnvironment(
                command,
                "KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT_HOST://localhost:" + this.hostPort + ",PLAINTEXT://broker:19093");
        this.addEnvironment(command, "KAFKA_PROCESS_ROLES", "broker,controller");
        this.addEnvironment(command, "KAFKA_CONTROLLER_QUORUM_VOTERS", "1@broker:29093");
        this.addEnvironment(
                command, "KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT_HOST://:19092,PLAINTEXT://:19093");
        this.addEnvironment(command, "KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        this.addEnvironment(command, "KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        this.addEnvironment(command, "CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw");
        this.addEnvironment(command, "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        this.addEnvironment(command, "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        this.addEnvironment(command, "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        this.addEnvironment(command, "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        this.addEnvironment(command, "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1");
        this.addEnvironment(command, "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
        this.addEnvironment(command, "KAFKA_MIN_INSYNC_REPLICAS", "1");
        this.addEnvironment(command, "KAFKA_LOG_DIRS", DATA_DIRECTORY);
        command.add(KafkaPlan.SERVER_IMAGE);
        this.docker(command.toArray(String[]::new));
        this.containerCreated = true;
    }

    void pause() throws IOException, InterruptedException {
        this.docker("pause", this.containerName);
    }

    void forceStop() throws IOException, InterruptedException {
        this.docker("kill", this.containerName);
    }

    void restart() throws IOException, InterruptedException {
        this.docker("start", this.containerName);
    }

    String serverVersion() throws IOException, InterruptedException {
        return this.docker("exec", this.containerName, "/opt/kafka/bin/kafka-topics.sh", "--version")
                .trim();
    }

    String logs() {
        try {
            return this.docker("logs", "--tail", "60", this.containerName);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            return "could not read Kafka container logs: " + exception.getMessage();
        } catch (final IOException exception) {
            return "could not read Kafka container logs: " + exception.getMessage();
        }
    }

    @Override
    public void close() {
        if (this.containerCreated) {
            try {
                this.docker("rm", "--force", this.containerName);
            } catch (final IOException exception) {
                System.err.println(
                        "could not remove Kafka container " + this.containerName + ": " + exception.getMessage());
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            this.containerCreated = false;
        }
        if (this.volumeCreated) {
            try {
                this.docker("volume", "rm", this.volumeName);
            } catch (final IOException exception) {
                System.err.println("could not remove Kafka volume " + this.volumeName + ": " + exception.getMessage());
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            this.volumeCreated = false;
        }
    }

    private void addEnvironment(final List<String> command, final String name, final String value) {
        command.add("--env");
        command.add(name + "=" + value);
    }

    private String docker(final String... arguments) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>(arguments.length + 1);
        command.add("docker");
        command.addAll(List.of(arguments));
        final Process process =
                new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!process.waitFor(DOCKER_COMMAND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IOException("Docker command timed out: " + command);
        }
        final String output = new String(process.getInputStream().readAllBytes()).trim();
        if (process.exitValue() != 0) {
            throw new IOException("Docker command failed (" + process.exitValue() + "): " + output);
        }
        return output;
    }
}
