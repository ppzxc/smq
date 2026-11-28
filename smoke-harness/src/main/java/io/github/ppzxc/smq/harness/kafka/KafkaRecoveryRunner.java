package io.github.ppzxc.smq.harness.kafka;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

final class KafkaRecoveryRunner {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    private KafkaRecoveryRunner() {}

    static List<Map<String, Object>> run(
            final KafkaDockerBroker broker, final KafkaFlushMode flushMode, final Path workDir) throws Exception {
        Files.createDirectories(workDir);
        final List<Map<String, Object>> observations = new ArrayList<>();
        observations.add(confirmedPublish(broker, flushMode, workDir));
        observations.add(deliveryBeforeCommit(broker, flushMode, workDir));
        observations.add(commitConfirmed(broker, flushMode, workDir));
        observations.add(requestInFlight(broker, flushMode, workDir));
        return observations;
    }

    private static Map<String, Object> confirmedPublish(
            final KafkaDockerBroker broker, final KafkaFlushMode flushMode, final Path workDir) throws Exception {
        final String topic = topicName("publish_confirmed");
        final String key = UUID.randomUUID().toString();
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            KafkaClientSupport.createTopic(admin, topic, flushMode.topicFlushMessages());
        }
        final RecordMetadata metadata = publishConfirmed(broker.bootstrapServers(), topic, key);
        final Path ledger = writeLedger(workDir, "publish-confirmed", key, "broker-response", metadata.offset());
        final Map<String, Object> restartReadiness = stopAndRestart(broker, topic);
        final ConsumerRecord<String, byte[]> recovered = KafkaWorkloadRunner.receive(
                broker.bootstrapServers(), topic, "smq-kafka-audit-" + UUID.randomUUID(), READ_TIMEOUT);
        final boolean found = recovered != null && key.equals(recovered.key());
        if (!found) {
            throw new IllegalStateException("Kafka lost a confirmed publish after a broker restart: " + key);
        }
        return observation(
                flushMode,
                "publish-confirmed",
                broker,
                topic,
                key,
                ledger,
                true,
                true,
                0,
                null,
                null,
                List.of(key),
                "CONFIRMED_AND_RECOVERED",
                "restartReadiness",
                restartReadiness);
    }

    private static Map<String, Object> deliveryBeforeCommit(
            final KafkaDockerBroker broker, final KafkaFlushMode flushMode, final Path workDir) throws Exception {
        final String topic = topicName("delivery_before_commit");
        final String group = "smq-kafka-uncommitted-" + UUID.randomUUID();
        final String key = UUID.randomUUID().toString();
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            KafkaClientSupport.createTopic(admin, topic, flushMode.topicFlushMessages());
        }
        final RecordMetadata metadata = publishConfirmed(broker.bootstrapServers(), topic, key);
        final Path publishLedger =
                writeLedger(workDir, "delivery-before-commit-publish", key, "broker-response", metadata.offset());
        final ConsumerRecord<String, byte[]> firstDelivery =
                KafkaWorkloadRunner.receive(broker.bootstrapServers(), topic, group, READ_TIMEOUT);
        if (firstDelivery == null || !key.equals(firstDelivery.key())) {
            throw new IllegalStateException("Kafka did not deliver the expected record before broker stop: " + key);
        }
        final Path deliveryLedger = writeLedger(
                workDir, "delivery-before-commit-observed", key, "consumer-delivery", firstDelivery.offset());
        final Map<String, Object> offsetsBeforeStop;
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            offsetsBeforeStop = KafkaClientSupport.offsetObservation(KafkaClientSupport.offsets(admin, group));
        }
        if (!offsetsBeforeStop.isEmpty()) {
            throw new IllegalStateException("Kafka group offset advanced before commit in the recovery test: " + group);
        }
        final Path beforeStopOffsetLedger = appendLedger(
                workDir,
                "delivery-before-commit-offset-before-stop",
                Map.of("group", group, "offsets", offsetsBeforeStop, "evidence", "AdminClient response"));
        final Map<String, Object> restartReadiness = stopAndRestart(broker, topic);
        final Map<String, Object> offsetsAfterRestart;
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            offsetsAfterRestart = KafkaClientSupport.offsetObservation(KafkaClientSupport.offsets(admin, group));
        }
        if (!offsetsAfterRestart.isEmpty()) {
            throw new IllegalStateException("Kafka group offset appeared without a commit after restart: " + group);
        }
        final Path afterRestartOffsetLedger = appendLedger(
                workDir,
                "delivery-before-commit-offset-after-restart",
                Map.of("group", group, "offsets", offsetsAfterRestart, "evidence", "AdminClient response"));
        final ConsumerRecord<String, byte[]> redelivery =
                KafkaWorkloadRunner.receive(broker.bootstrapServers(), topic, group, READ_TIMEOUT);
        final boolean redelivered = redelivery != null && key.equals(redelivery.key());
        if (!redelivered) {
            throw new IllegalStateException("Kafka did not redeliver an uncommitted record after restart: " + key);
        }
        return observation(
                flushMode,
                "delivery-before-commit",
                broker,
                topic,
                key,
                publishLedger,
                true,
                true,
                0,
                group,
                null,
                List.of(key, redelivery.key()),
                "REDELIVERED_WITHOUT_COMMITTED_OFFSET",
                "deliveryLedger",
                deliveryLedger.toString(),
                "redeliveredOffset",
                redelivery.offset(),
                "groupOffsetsBeforeStop",
                offsetsBeforeStop,
                "groupOffsetsAfterRestart",
                offsetsAfterRestart,
                "groupOffsetLedgerBeforeStop",
                beforeStopOffsetLedger.toString(),
                "groupOffsetLedgerAfterRestart",
                afterRestartOffsetLedger.toString(),
                "restartReadiness",
                restartReadiness);
    }

    private static Map<String, Object> commitConfirmed(
            final KafkaDockerBroker broker, final KafkaFlushMode flushMode, final Path workDir) throws Exception {
        final String topic = topicName("commit_confirmed");
        final String group = "smq-kafka-committed-" + UUID.randomUUID();
        final String key = UUID.randomUUID().toString();
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            KafkaClientSupport.createTopic(admin, topic, flushMode.topicFlushMessages());
        }
        final RecordMetadata metadata = publishConfirmed(broker.bootstrapServers(), topic, key);
        final Path publishLedger =
                writeLedger(workDir, "commit-confirmed-publish", key, "broker-response", metadata.offset());
        final ConsumerRecord<String, byte[]> delivered =
                KafkaWorkloadRunner.receive(broker.bootstrapServers(), topic, group, READ_TIMEOUT);
        if (delivered == null || !key.equals(delivered.key())) {
            throw new IllegalStateException("Kafka did not deliver the expected record before commit: " + key);
        }
        final Path commitLedger =
                writeLedger(workDir, "commit-confirmed-settlement", key, "delivery", delivered.offset());
        final TopicPartition partition = new TopicPartition(topic, 0);
        final long commitStarted = System.nanoTime();
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]>(
                KafkaWorkloadRunner.consumerProperties(broker.bootstrapServers(), group, "smq-kafka-commit", 1))) {
            consumer.assign(List.of(partition));
            consumer.commitSync(
                    Map.of(partition, new OffsetAndMetadata(delivered.offset() + 1)), Duration.ofSeconds(10));
        }
        final long commitResponseNanos = System.nanoTime() - commitStarted;
        final Path committedLedger = appendLedger(
                workDir,
                "commit-confirmed-offset-response",
                Map.of(
                        "group",
                        group,
                        "partition",
                        0,
                        "offset",
                        delivered.offset() + 1,
                        "responseNanos",
                        commitResponseNanos));
        final Map<TopicPartition, OffsetAndMetadata> beforeStop;
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            beforeStop = KafkaClientSupport.offsets(admin, group);
        }
        final Map<String, Object> offsetsBeforeStop = KafkaClientSupport.offsetObservation(beforeStop);
        final OffsetAndMetadata committedBefore = beforeStop.get(partition);
        if (committedBefore == null || committedBefore.offset() != delivered.offset() + 1) {
            throw new IllegalStateException("Kafka did not apply the successful commit before broker stop: " + group);
        }
        final Path beforeStopOffsetLedger = appendLedger(
                workDir,
                "commit-confirmed-offset-before-stop",
                Map.of("group", group, "offsets", offsetsBeforeStop, "evidence", "AdminClient response"));
        final Map<String, Object> restartReadiness = stopAndRestart(broker, topic);
        final Map<TopicPartition, OffsetAndMetadata> afterRestart;
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            afterRestart = KafkaClientSupport.offsets(admin, group);
        }
        final Map<String, Object> offsetsAfterRestart = KafkaClientSupport.offsetObservation(afterRestart);
        final OffsetAndMetadata committedAfter = afterRestart.get(partition);
        if (committedAfter == null || committedAfter.offset() != delivered.offset() + 1) {
            throw new IllegalStateException("Kafka lost an applied group offset after restart: " + group);
        }
        final Path afterRestartOffsetLedger = appendLedger(
                workDir,
                "commit-confirmed-offset-after-restart",
                Map.of("group", group, "offsets", offsetsAfterRestart, "evidence", "AdminClient response"));
        final ConsumerRecord<String, byte[]> redelivery =
                KafkaWorkloadRunner.receive(broker.bootstrapServers(), topic, group, Duration.ofSeconds(1));
        if (redelivery != null) {
            throw new IllegalStateException("Kafka redelivered a record after its committed offset: " + key);
        }
        final ConsumerRecord<String, byte[]> audited = KafkaWorkloadRunner.receive(
                broker.bootstrapServers(), topic, "smq-kafka-audit-" + UUID.randomUUID(), READ_TIMEOUT);
        if (audited == null || !key.equals(audited.key())) {
            throw new IllegalStateException("Kafka lost a record whose consumer-group offset was committed: " + key);
        }
        return observation(
                flushMode,
                "commit-confirmed",
                broker,
                topic,
                key,
                publishLedger,
                true,
                true,
                0,
                group,
                committedAfter.offset(),
                List.of(audited.key()),
                "COMMIT_RESPONSE_AND_OFFSET_RECOVERED",
                "deliveryLedger",
                commitLedger.toString(),
                "commitResponseLedger",
                committedLedger.toString(),
                "redeliveredAfterCommit",
                false,
                "commitResponseNanos",
                commitResponseNanos,
                "groupOffsetsBeforeStop",
                offsetsBeforeStop,
                "groupOffsetsAfterRestart",
                offsetsAfterRestart,
                "groupOffsetLedgerBeforeStop",
                beforeStopOffsetLedger.toString(),
                "groupOffsetLedgerAfterRestart",
                afterRestartOffsetLedger.toString(),
                "restartReadiness",
                restartReadiness);
    }

    private static Map<String, Object> requestInFlight(
            final KafkaDockerBroker broker, final KafkaFlushMode flushMode, final Path workDir) throws Exception {
        final String topic = topicName("request_in_flight");
        final String key = UUID.randomUUID().toString();
        try (AdminClient admin = KafkaClientSupport.admin(broker.bootstrapServers(), "smq-kafka-recovery-admin")) {
            KafkaClientSupport.createTopic(admin, topic, flushMode.topicFlushMessages());
        }
        final Properties properties =
                KafkaWorkloadRunner.producerProperties(broker.bootstrapServers(), "smq-kafka-in-flight-producer");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "11000");
        final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        producer.partitionsFor(topic);
        final Path ledger = appendLedger(
                workDir,
                "request-in-flight-attempt",
                Map.of("topic", topic, "key", key, "classification", "UNKNOWN", "requestSubmitted", true));
        broker.pause();
        final Future<RecordMetadata> pending = producer.send(new ProducerRecord<>(topic, key, new byte[512 * 1024]));
        Thread.sleep(250);
        if (pending.isDone()) {
            String completedResponse;
            try {
                completedResponse = "success offset " + pending.get().offset();
            } catch (final Exception exception) {
                completedResponse = "exception " + exception;
            }
            producer.close(Duration.ZERO);
            throw new IllegalStateException(
                    "Kafka in-flight test request completed while the broker was paused: " + completedResponse);
        }
        broker.forceStop();
        final Optional<RecordMetadata> responseBeforeDisconnect = completedResponse(pending);
        producer.close(Duration.ZERO);
        broker.restart();
        final Map<String, Object> restartReadiness = KafkaClientSupport.awaitReady(broker, topic);
        final ConsumerRecord<String, byte[]> recovered = KafkaWorkloadRunner.receive(
                broker.bootstrapServers(), topic, "smq-kafka-audit-" + UUID.randomUUID(), READ_TIMEOUT);
        final boolean recoveredPresent = recovered != null && key.equals(recovered.key());
        if (responseBeforeDisconnect.isPresent()) {
            throw new IllegalStateException("Kafka returned a publish success after the forced broker stop began");
        }
        return observation(
                flushMode,
                "request-in-flight",
                broker,
                topic,
                key,
                ledger,
                true,
                false,
                1,
                null,
                null,
                recoveredPresent ? List.of(key) : List.of(),
                "UNKNOWN",
                "requestStartedBeforeStop",
                true,
                "responseBeforeDisconnect",
                responseBeforeDisconnect.isPresent(),
                "recoveredRecordPresent",
                recoveredPresent,
                "restartReadiness",
                restartReadiness);
    }

    private static Optional<RecordMetadata> completedResponse(final Future<RecordMetadata> pending)
            throws InterruptedException {
        try {
            return Optional.ofNullable(pending.get(250, TimeUnit.MILLISECONDS));
        } catch (final ExecutionException | TimeoutException expected) {
            return Optional.empty();
        }
    }

    private static RecordMetadata publishConfirmed(final String bootstrap, final String topic, final String key)
            throws Exception {
        try (KafkaProducer<String, byte[]> producer =
                new KafkaProducer<>(KafkaWorkloadRunner.producerProperties(bootstrap, "smq-kafka-recovery-producer"))) {
            return producer.send(new ProducerRecord<>(topic, key, new byte[128]))
                    .get(Duration.ofSeconds(15).toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static Map<String, Object> stopAndRestart(final KafkaDockerBroker broker, final String topic)
            throws Exception {
        broker.forceStop();
        broker.restart();
        return KafkaClientSupport.awaitReady(broker, topic);
    }

    private static Path writeLedger(
            final Path workDir, final String name, final String key, final String kind, final long observedOffset)
            throws IOException {
        return appendLedger(
                workDir,
                name,
                Map.of(
                        "key",
                        key,
                        "evidence",
                        kind,
                        "offset",
                        observedOffset,
                        "recordedAt",
                        java.time.Instant.now().toString()));
    }

    private static Path appendLedger(final Path workDir, final String name, final Map<String, ?> record)
            throws IOException {
        final Path ledger = workDir.resolve("external-ledgers").resolve(name + ".jsonl");
        Files.createDirectories(ledger.getParent());
        try (FileChannel channel = FileChannel.open(
                ledger, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            final byte[] line = (KafkaJson.encode(record) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(line);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        return ledger;
    }

    private static Map<String, Object> observation(
            final KafkaFlushMode flushMode,
            final String scenario,
            final KafkaDockerBroker broker,
            final String topic,
            final String key,
            final Path externalLedger,
            final boolean externalRecordWritten,
            final boolean brokerAckReceivedBeforeStop,
            final int unknownRequests,
            final String groupId,
            final Long committedOffsetAfterRestart,
            final List<String> recoveredKeys,
            final String classification,
            final Object... extra) {
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("flushConfiguration", flushMode.label());
        result.put("scenario", scenario);
        result.put("container", broker.containerName());
        result.put("dataVolume", broker.volumeName());
        result.put("dataDirectory", broker.dataDirectory());
        result.put("sameDataDirectoryAcrossRestart", true);
        result.put("forcedStop", "docker kill");
        result.put("topic", topic);
        result.put("key", key);
        result.put("externalLedger", externalLedger.toString());
        result.put("externalRecordWritten", externalRecordWritten);
        result.put("brokerAckReceivedBeforeStop", brokerAckReceivedBeforeStop);
        result.put("unknownRequests", unknownRequests);
        result.put("groupId", groupId);
        result.put("committedOffsetAfterRestart", committedOffsetAfterRestart);
        result.put("recoveredKeys", recoveredKeys);
        result.put("classification", classification);
        if (extra.length % 2 != 0) {
            throw new IllegalArgumentException("recovery observation extras need key/value pairs");
        }
        for (int index = 0; index < extra.length; index += 2) {
            result.put((String) extra[index], extra[index + 1]);
        }
        return result;
    }

    private static String topicName(final String prefix) {
        return "smq_harness_" + prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }
}
