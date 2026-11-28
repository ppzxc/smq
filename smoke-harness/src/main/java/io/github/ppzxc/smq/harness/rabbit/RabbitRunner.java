package io.github.ppzxc.smq.harness.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class RabbitRunner {

    private static final long CONFIRM_TIMEOUT_MILLIS = 30_000;
    private static final long DELIVERY_TIMEOUT_NANOS = 15_000_000_000L;

    public record Sample(
            RabbitPlan.QueueKind queueKind,
            RabbitOptions.Mode mode,
            int repeat,
            long completedMessages,
            long deliveryCount,
            long ackSends,
            int readyMessagesAfterAckObservation,
            long elapsedNanos,
            double throughputMessagesPerSecond,
            long publishLatencyP50Nanos,
            long publishLatencyP99Nanos,
            long ackSendLatencyP50Nanos,
            long ackSendLatencyP99Nanos,
            int maxObservedBatchSize,
            int maxObservedInFlight,
            long routingFailures,
            long rejectedConfirms,
            List<Long> publishLatencyNanos,
            List<Long> ackSendLatencyNanos) {}

    private String actualServerVersion = "unconnected";

    public String actualServerVersion() {
        return this.actualServerVersion;
    }

    public void run(final RabbitOptions options, final List<Sample> results) throws Exception {
        Files.createDirectories(options.workDir());
        for (final RabbitPlan.QueueKind queueKind : RabbitPlan.QueueKind.values()) {
            for (final RabbitOptions.Mode mode : options.modes()) {
                if (options.warmupMessages() > 0) {
                    this.executeWorkload(options, queueKind, mode, options.warmupMessages(), 0);
                }
                for (int repeat = 1; repeat <= options.repeats(); repeat++) {
                    results.add(this.executeWorkload(options, queueKind, mode, options.messageCount(), repeat));
                }
            }
        }
    }

    private Sample executeWorkload(
            final RabbitOptions options,
            final RabbitPlan.QueueKind queueKind,
            final RabbitOptions.Mode mode,
            final int messageCount,
            final int repeat)
            throws IOException, TimeoutException, InterruptedException {
        final String queueName = "smq-harness-" + queueKind.queueType() + "-" + UUID.randomUUID();
        final List<Long> publishSamples = new ArrayList<>();
        final List<Long> ackSamples = new ArrayList<>();
        final AtomicLong routingFailures = new AtomicLong();
        final AtomicLong rejectedConfirms = new AtomicLong();
        long completed = 0;
        long delivered = 0;
        long ackSends = 0;
        int maxBatchSize = 0;
        int maxInFlight = 0;
        final long start = System.nanoTime();
        try (Connection connection = this.connect(options);
                Channel publisher = connection.createChannel();
                Channel consumer = connection.createChannel()) {
            this.recordServerVersion(connection);
            publisher.confirmSelect();
            publisher.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) ->
                    routingFailures.incrementAndGet());
            publisher.addConfirmListener((tag, multiple) -> {}, (tag, multiple) -> rejectedConfirms.incrementAndGet());
            RabbitPlan.declareQueue(publisher, queueName, queueKind);
            final byte[] payload = new byte[options.payloadBytes()];
            Arrays.fill(payload, (byte) 'x');
            int remaining = messageCount;
            while (remaining > 0) {
                final int windowSize = Math.min(remaining, options.maxInFlight());
                int sent = 0;
                while (sent < windowSize) {
                    final int batchSize =
                            mode == RabbitOptions.Mode.SINGLE ? 1 : Math.min(options.batchSize(), windowSize - sent);
                    maxInFlight = Math.max(maxInFlight, batchSize);
                    final int returnedBefore = Math.toIntExact(routingFailures.get());
                    final long publishStart = System.nanoTime();
                    for (int index = 0; index < batchSize; index++) {
                        final String messageId = UUID.randomUUID().toString();
                        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                .contentType("application/octet-stream")
                                .deliveryMode(2)
                                .messageId(messageId)
                                .build();
                        publisher.basicPublish("", queueName, true, properties, payload);
                    }
                    publisher.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MILLIS);
                    publishSamples.add(System.nanoTime() - publishStart);
                    final long returned = routingFailures.get() - returnedBefore;
                    if (returned > 0) {
                        throw new IllegalStateException("mandatory publish routing failures: " + returned);
                    }
                    completed += batchSize;
                    sent += batchSize;
                    maxBatchSize = Math.max(maxBatchSize, batchSize);
                }

                int unsettled = windowSize;
                final long deliveryDeadline = System.nanoTime() + DELIVERY_TIMEOUT_NANOS;
                while (unsettled > 0) {
                    final List<Long> tags = new ArrayList<>(Math.min(options.chunkSize(), unsettled));
                    while (tags.size() < options.chunkSize() && unsettled > 0) {
                        final GetResponse response = consumer.basicGet(queueName, false);
                        if (response == null) {
                            if (System.nanoTime() >= deliveryDeadline) {
                                throw new IllegalStateException("confirmed message was not delivered before timeout");
                            }
                            LockSupport.parkNanos(1_000_000L);
                            continue;
                        }
                        tags.add(response.getEnvelope().getDeliveryTag());
                        delivered++;
                        unsettled--;
                    }
                    final long ackStart = System.nanoTime();
                    consumer.basicAck(tags.getLast(), tags.size() > 1);
                    ackSamples.add(System.nanoTime() - ackStart);
                    ackSends += tags.size();
                }
                remaining -= windowSize;
            }
            final int readyMessages = publisher.queueDeclarePassive(queueName).getMessageCount();
            publisher.queueDelete(queueName);
            final long elapsed = System.nanoTime() - start;
            return new Sample(
                    queueKind,
                    mode,
                    repeat,
                    completed,
                    delivered,
                    ackSends,
                    readyMessages,
                    elapsed,
                    completed * 1_000_000_000.0 / elapsed,
                    percentile(publishSamples, 0.50),
                    percentile(publishSamples, 0.99),
                    percentile(ackSamples, 0.50),
                    percentile(ackSamples, 0.99),
                    maxBatchSize,
                    maxInFlight,
                    routingFailures.get(),
                    rejectedConfirms.get(),
                    List.copyOf(publishSamples),
                    List.copyOf(ackSamples));
        }
    }

    private Connection connect(final RabbitOptions options) throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(options.host());
        factory.setPort(options.port());
        factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest"));
        factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest"));
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        factory.setConnectionTimeout(10_000);
        return factory.newConnection("smq-rabbit-harness");
    }

    private void recordServerVersion(final Connection connection) {
        final Object version = connection.getServerProperties().get("version");
        final String actual = version == null ? "unknown" : version.toString();
        this.actualServerVersion = actual;
        if (!RabbitPlan.SERVER_VERSION.equals(actual)) {
            throw new IllegalStateException("RabbitMQ version mismatch: expected " + RabbitPlan.SERVER_VERSION
                    + ", got " + actual + " from server properties " + connection.getServerProperties());
        }
    }

    private static long percentile(final List<Long> samples, final double percentile) {
        final long[] sorted =
                samples.stream().mapToLong(Long::longValue).sorted().toArray();
        final int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }
}
