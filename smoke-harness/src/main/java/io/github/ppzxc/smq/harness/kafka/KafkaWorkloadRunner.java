package io.github.ppzxc.smq.harness.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

final class KafkaWorkloadRunner {

    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    private static final String SETTLEMENT_METHOD = "consumer-group offset commit response";

    private KafkaWorkloadRunner() {}

    static Map<String, Object> measure(
            final KafkaDockerBroker broker,
            final KafkaRunOptions options,
            final KafkaFlushMode flushMode,
            final String workload,
            final int repeat)
            throws Exception {
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final String topic = "smq_harness_" + suffix;
        final String group = "smq-harness-" + suffix;
        final String bootstrap = broker.bootstrapServers();
        final int windowSize = "single".equals(workload) ? 1 : options.batchSize();
        final List<Long> publishLatencies = new ArrayList<>();
        final List<Long> deliveryLatencies = new ArrayList<>();
        final List<Long> commitLatencies = new ArrayList<>();
        int commitResponses = 0;
        long publishStarted = 0;
        long publishElapsed = 0;
        long settleStarted = 0;
        long settleElapsed = 0;
        long lagBefore;
        long lagAfter;
        Map<String, Object> topicConfiguration;
        final Map<String, Object> producerConfiguration = producerConfiguration();

        try (AdminClient admin = KafkaClientSupport.admin(bootstrap, "smq-kafka-benchmark-admin")) {
            KafkaClientSupport.createTopic(admin, topic, flushMode.topicFlushMessages());
            topicConfiguration = KafkaClientSupport.topicConfiguration(admin, topic);
            final Properties producerProperties = producerProperties(bootstrap, "smq-kafka-benchmark-producer");
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties)) {
                if (options.warmup() > 0) {
                    send(
                            producer,
                            topic,
                            suffix + "-warmup",
                            options.warmup(),
                            windowSize,
                            options.payloadBytes(),
                            null);
                    consumeAndCommit(bootstrap, topic, group, options.warmup(), windowSize, null, null);
                }
                publishStarted = System.nanoTime();
                send(
                        producer,
                        topic,
                        suffix + "-measured",
                        options.count(),
                        windowSize,
                        options.payloadBytes(),
                        publishLatencies);
                publishElapsed = System.nanoTime() - publishStarted;
            }
            lagBefore = KafkaClientSupport.lag(admin, topic, group);
            settleStarted = System.nanoTime();
            final ConsumerSummary summary = consumeAndCommit(
                    bootstrap, topic, group, options.count(), windowSize, deliveryLatencies, commitLatencies);
            settleElapsed = System.nanoTime() - settleStarted;
            commitResponses = summary.commitResponses();
            lagAfter = KafkaClientSupport.lag(admin, topic, group);
        }

        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("flushConfiguration", flushMode.label());
        result.put("workload", workload);
        result.put("repeat", repeat);
        result.put("topic", topic);
        result.put("consumerGroup", group);
        result.put("confirmedPublishes", options.count());
        result.put("publishElapsedNanos", publishElapsed);
        result.put("publishThroughputMessagesPerSecond", rate(options.count(), publishElapsed));
        result.put("publishLatencyP50Nanos", percentile(publishLatencies, 50));
        result.put("publishLatencyP99Nanos", percentile(publishLatencies, 99));
        result.put("delivered", options.count());
        result.put("settlementMethod", SETTLEMENT_METHOD);
        result.put("commitResponses", commitResponses);
        result.put("committedMessages", options.count());
        result.put("settleElapsedNanos", settleElapsed);
        result.put("settlementThroughputMessagesPerSecond", rate(options.count(), settleElapsed));
        result.put("deliveryLatencyP50Nanos", percentile(deliveryLatencies, 50));
        result.put("deliveryLatencyP99Nanos", percentile(deliveryLatencies, 99));
        result.put("commitLatencyP50Nanos", percentile(commitLatencies, 50));
        result.put("commitLatencyP99Nanos", percentile(commitLatencies, 99));
        result.put("lagBefore", lagBefore);
        result.put("lagAfter", lagAfter);
        result.put("topicConfiguration", topicConfiguration);
        result.put("producerConfiguration", producerConfiguration);
        return result;
    }

    static Properties producerProperties(final String bootstrap, final String clientId) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    static Properties consumerProperties(
            final String bootstrap, final String group, final String clientId, final int maxPollRecords) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
        return properties;
    }

    static ConsumerRecord<String, byte[]> receive(
            final String bootstrap, final String topic, final String group, final Duration timeout) throws Exception {
        final TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap, group, "smq-kafka-recovery-consumer", 1))) {
            consumer.assign(List.of(partition));
            final long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                final ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(200));
                if (!polled.isEmpty()) {
                    return polled.records(partition).getFirst();
                }
            }
        }
        return null;
    }

    private static ConsumerSummary consumeAndCommit(
            final String bootstrap,
            final String topic,
            final String group,
            final int target,
            final int windowSize,
            final List<Long> deliveryLatencies,
            final List<Long> commitLatencies)
            throws Exception {
        return consumeAndCommit(
                bootstrap, topic, group, target, windowSize, deliveryLatencies, commitLatencies, OPERATION_TIMEOUT);
    }

    private static ConsumerSummary consumeAndCommit(
            final String bootstrap,
            final String topic,
            final String group,
            final int target,
            final int windowSize,
            final List<Long> deliveryLatencies,
            final List<Long> commitLatencies,
            final Duration timeout)
            throws Exception {
        final TopicPartition partition = new TopicPartition(topic, 0);
        final int maxPollRecords = Math.max(1, Math.min(target, windowSize));
        int delivered = 0;
        int commitResponses = 0;
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(
                consumerProperties(bootstrap, group, "smq-kafka-settlement-consumer", maxPollRecords))) {
            consumer.assign(List.of(partition));
            final long deadline = System.nanoTime() + timeout.toNanos();
            while (delivered < target && System.nanoTime() < deadline) {
                final ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(200));
                final List<ConsumerRecord<String, byte[]>> records = polled.records(partition);
                if (records.isEmpty()) {
                    continue;
                }
                int accepted = 0;
                long nextOffset = -1;
                for (final ConsumerRecord<String, byte[]> record : records) {
                    if (accepted >= target - delivered) {
                        break;
                    }
                    accepted++;
                    nextOffset = record.offset() + 1;
                    if (deliveryLatencies != null) {
                        deliveryLatencies.add(deliveryLatencyNanos(record.timestamp(), System.currentTimeMillis()));
                    }
                }
                if (accepted == 0) {
                    continue;
                }
                final long commitStarted = System.nanoTime();
                consumer.commitSync(Map.of(partition, new OffsetAndMetadata(nextOffset)), Duration.ofSeconds(10));
                if (commitLatencies != null) {
                    commitLatencies.add(System.nanoTime() - commitStarted);
                }
                delivered += accepted;
                commitResponses++;
            }
        }
        if (delivered != target) {
            throw new IllegalStateException("Kafka consumer delivered " + delivered + " of " + target + " records");
        }
        return new ConsumerSummary(commitResponses);
    }

    private static void send(
            final KafkaProducer<String, byte[]> producer,
            final String topic,
            final String keyPrefix,
            final int count,
            final int windowSize,
            final int payloadBytes,
            final List<Long> latencies)
            throws Exception {
        final byte[] payload = new byte[payloadBytes];
        Arrays.fill(payload, (byte) 'x');
        int sent = 0;
        while (sent < count) {
            final int window = Math.min(windowSize, count - sent);
            final List<PendingPublish> pending = new ArrayList<>(window);
            for (int index = 0; index < window; index++) {
                final String key = keyPrefix + "-" + sent;
                final long started = System.nanoTime();
                final ProducerRecord<String, byte[]> record =
                        new ProducerRecord<>(topic, null, System.currentTimeMillis(), key, payload);
                pending.add(new PendingPublish(producer.send(record), started));
                sent++;
            }
            for (final PendingPublish publish : pending) {
                publish.future().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                if (latencies != null) {
                    latencies.add(System.nanoTime() - publish.startedNanos());
                }
            }
        }
    }

    private static Map<String, Object> producerConfiguration() {
        return Map.of(
                "acks", "all",
                "enable.idempotence", "true",
                "batch.size", "16384",
                "linger.ms", "0",
                "request.timeout.ms", "5000",
                "delivery.timeout.ms", "15000",
                "key.serializer", "StringSerializer",
                "value.serializer", "ByteArraySerializer");
    }

    static long deliveryLatencyNanos(final long publishedAtMillis, final long deliveredAtMillis) {
        return Math.max(0L, deliveredAtMillis - publishedAtMillis) * 1_000_000L;
    }

    private static double rate(final long count, final long elapsedNanos) {
        return elapsedNanos <= 0 ? 0.0 : count * 1_000_000_000.0 / elapsedNanos;
    }

    private static Long percentile(final List<Long> values, final int percentile) {
        if (values.isEmpty()) {
            return null;
        }
        final List<Long> sorted = values.stream().sorted().toList();
        final int rank = (int) Math.ceil(percentile / 100.0 * sorted.size());
        return sorted.get(Math.max(0, rank - 1));
    }

    record ConsumerSummary(int commitResponses) {}

    private record PendingPublish(Future<RecordMetadata> future, long startedNanos) {}
}
