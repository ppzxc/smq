package io.github.ppzxc.smq.harness.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

final class KafkaClientSupport {

    private static final Duration API_TIMEOUT = Duration.ofSeconds(10);
    private static final List<String> TOPIC_CONFIGS = List.of("min.insync.replicas", "flush.messages");
    private static final List<String> BROKER_CONFIGS = List.of(
            "node.id",
            "process.roles",
            "log.dirs",
            "min.insync.replicas",
            "log.flush.interval.messages",
            "offsets.topic.replication.factor",
            "transaction.state.log.min.isr",
            "transaction.state.log.replication.factor",
            "share.coordinator.state.topic.min.isr",
            "share.coordinator.state.topic.replication.factor");

    private KafkaClientSupport() {}

    static Properties adminProperties(final String bootstrapServers, final String clientId) {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("client.id", clientId);
        properties.put("default.api.timeout.ms", "5000");
        properties.put("request.timeout.ms", "2000");
        properties.put("reconnect.backoff.ms", "100");
        properties.put("reconnect.backoff.max.ms", "250");
        return properties;
    }

    static AdminClient admin(final String bootstrapServers, final String clientId) {
        return AdminClient.create(adminProperties(bootstrapServers, clientId));
    }

    static Map<String, Object> awaitReady(final KafkaDockerBroker broker) throws IOExceptionLike {
        return awaitReady(broker, null);
    }

    static Map<String, Object> awaitReady(final KafkaDockerBroker broker, final String topicName)
            throws IOExceptionLike {
        Exception lastFailure = null;
        Map<String, Object> lastObservation = Map.of("status", "no broker metadata observed");
        final long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IOExceptionLike("interrupted while waiting for Kafka broker");
            }
            try (AdminClient admin = admin(broker.bootstrapServers(), "smq-kafka-readiness")) {
                final var cluster = admin.describeCluster();
                final List<Node> nodes = cluster.nodes().get(2, TimeUnit.SECONDS).stream()
                        .sorted((left, right) -> Integer.compare(left.id(), right.id()))
                        .toList();
                final Node controller = cluster.controller().get(2, TimeUnit.SECONDS);
                final Map<String, Object> observation = new LinkedHashMap<>();
                final List<Integer> nodeIds = nodes.stream().map(Node::id).toList();
                observation.put("controllerId", controller == null ? null : controller.id());
                observation.put("brokerNodeIds", nodeIds);
                boolean ready = nodeIds.contains(1) && controller != null && controller.id() == 1;
                if (topicName != null) {
                    observation.put("topic", topicName);
                    final var description = admin.describeTopics(List.of(topicName))
                            .allTopicNames()
                            .get(2, TimeUnit.SECONDS)
                            .get(topicName);
                    final List<Map<String, Object>> partitions = new ArrayList<>();
                    for (final var partition : description.partitions()) {
                        final Integer leaderId = partition.leader() == null
                                ? null
                                : partition.leader().id();
                        final List<Integer> replicas =
                                partition.replicas().stream().map(Node::id).toList();
                        final List<Integer> isr =
                                partition.isr().stream().map(Node::id).toList();
                        final Map<String, Object> partitionObservation = new LinkedHashMap<>();
                        partitionObservation.put("partition", partition.partition());
                        partitionObservation.put("leaderId", leaderId);
                        partitionObservation.put("replicas", replicas);
                        partitionObservation.put("isr", isr);
                        partitions.add(partitionObservation);
                        ready &= leaderId != null
                                && leaderId == 1
                                && replicas.equals(List.of(1))
                                && isr.equals(List.of(1));
                    }
                    observation.put("partitions", partitions);
                    ready &= !partitions.isEmpty();
                }
                lastObservation = observation;
                if (ready) {
                    return observation;
                }
            } catch (final Exception exception) {
                lastFailure = exception;
            }
            try {
                Thread.sleep(200);
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOExceptionLike("interrupted while waiting for Kafka broker", exception);
            }
        }
        throw new IOExceptionLike(
                "Kafka broker did not become ready; last observed metadata: " + KafkaJson.encode(lastObservation) + ". "
                        + broker.logs(),
                lastFailure);
    }

    static void createTopic(final AdminClient admin, final String topicName, final String flushMessages)
            throws Exception {
        final Map<String, String> configs = new LinkedHashMap<>();
        configs.put("min.insync.replicas", "1");
        if (flushMessages != null) {
            configs.put("flush.messages", flushMessages);
        }
        final NewTopic topic = new NewTopic(topicName, 1, (short) 1).configs(configs);
        admin.createTopics(List.of(topic)).all().get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static Map<String, Object> brokerConfiguration(
            final AdminClient admin, final String bootstrapServers, final String volumeName) throws Exception {
        final ConfigResource broker = new ConfigResource(Type.BROKER, "1");
        final Map<ConfigResource, Config> described =
                admin.describeConfigs(List.of(broker)).all().get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        final Config config = described.get(broker);
        final Map<String, Object> settings = configValues(config, BROKER_CONFIGS);
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("bootstrapServers", bootstrapServers);
        result.put("dataVolume", volumeName);
        result.put("dataDirectory", KafkaDockerBroker.DATA_DIRECTORY);
        result.put("sameDataDirectoryAcrossRestart", true);
        result.put("settings", settings);
        return result;
    }

    static Map<String, Object> topicConfiguration(final AdminClient admin, final String topicName) throws Exception {
        final ConfigResource topic = new ConfigResource(Type.TOPIC, topicName);
        final Map<ConfigResource, Config> described =
                admin.describeConfigs(List.of(topic)).all().get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        final Map<String, Object> settings = configValues(described.get(topic), TOPIC_CONFIGS);
        final TopicDescription description = admin.describeTopics(List.of(topicName))
                .allTopicNames()
                .get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .get(topicName);
        final int replicationFactor = description.partitions().stream()
                .mapToInt(partition -> partition.replicas().size())
                .min()
                .orElseThrow(() -> new IllegalStateException("Kafka topic has no partitions: " + topicName));
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("topic", topicName);
        result.put("partitionCount", description.partitions().size());
        result.put("replicationFactor", replicationFactor);
        result.put("settings", settings);
        return result;
    }

    static long lag(final AdminClient admin, final String topicName, final String groupId) throws Exception {
        final TopicPartition partition = new TopicPartition(topicName, 0);
        final ListOffsetsResultInfo end = admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                .all()
                .get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .get(partition);
        final OffsetAndMetadata committed = offsets(admin, groupId).get(partition);
        final long committedOffset = committed == null ? 0 : committed.offset();
        return Math.max(0, end.offset() - committedOffset);
    }

    static Map<TopicPartition, OffsetAndMetadata> offsets(final AdminClient admin, final String groupId)
            throws InterruptedException, ExecutionException, TimeoutException {
        return admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static Map<String, Object> offsetObservation(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        final Map<String, Object> result = new LinkedHashMap<>();
        offsets.forEach((partition, metadata) ->
                result.put(partition.topic() + ":" + partition.partition(), metadata.offset()));
        return result;
    }

    private static Map<String, Object> configValues(final Config config, final List<String> names) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final String name : names) {
            final ConfigEntry entry = config.get(name);
            if (entry != null) {
                final Map<String, Object> value = new LinkedHashMap<>();
                value.put("value", entry.value());
                value.put("source", entry.source().name());
                value.put("isDefault", entry.isDefault());
                values.put(name, value);
            }
        }
        return values;
    }

    static final class IOExceptionLike extends Exception {

        private static final long serialVersionUID = 1L;

        IOExceptionLike(final String message) {
            super(message);
        }

        IOExceptionLike(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
