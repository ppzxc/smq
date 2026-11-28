package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

final class BacklogRunner {

    private static final String QUEUE = "backlog";
    private static final String DEAD_LETTER_QUEUE = "backlog-dlq";
    private static final String PUBLISHED_AT_HEADER = "harness-published-at-nanos";
    private static final int PREFILL_BATCH_SIZE = 1_000;
    private static final List<QueueKind> QUEUE_KINDS = List.of(
            QueueKind.DEFAULT, QueueKind.MAX_DELIVERIES, QueueKind.SINGLE_ACTIVE_CONSUMER, QueueKind.LEASED_HEAD);

    enum QueueKind {
        DEFAULT,
        MAX_DELIVERIES,
        SINGLE_ACTIVE_CONSUMER,
        LEASED_HEAD
    }

    record Sample(
            Durability durability,
            QueueKind queueKind,
            int backlogCount,
            int repeat,
            long startDepth,
            QueueCounts startCounts,
            long endDepth,
            QueueCounts endCounts,
            long publishedMessages,
            long settledMessages,
            long elapsedNanos,
            double publishThroughputMessagesPerSecond,
            double settleThroughputMessagesPerSecond,
            Long deliveryLatencyP50Nanos,
            Long deliveryLatencyP99Nanos,
            String databasePath) {}

    private record ConsumerResult(long settled, List<Long> latencies) {}

    private record Measurement(long published, long settled, List<Long> latencies, long elapsedNanos) {}

    void run(final BacklogOptions options, final List<Sample> results)
            throws IOException, InterruptedException, ExecutionException {
        Files.createDirectories(options.workDir());
        final Path runDir = Files.createTempDirectory(options.workDir(), "backlog-");
        for (final Durability durability : options.durabilities()) {
            for (final QueueKind queueKind : options.queueKinds()) {
                if (options.warmupMillis() > 0) {
                    this.warmup(options, durability, queueKind, runDir);
                }
                for (int repeat = 1; repeat <= options.repeats(); repeat++) {
                    for (final int backlogCount : options.depths()) {
                        results.add(this.measure(options, durability, queueKind, backlogCount, repeat, runDir));
                    }
                }
            }
        }
    }

    static List<QueueKind> queueKinds() {
        return QUEUE_KINDS;
    }

    private void warmup(
            final BacklogOptions options, final Durability durability, final QueueKind queueKind, final Path runDir)
            throws IOException, InterruptedException, ExecutionException {
        final Path database = databasePath(runDir, durability, queueKind, "warmup");
        Files.createDirectories(database.getParent());
        try (Smq smq = Smq.sqlite(database).durability(durability).open()) {
            final Queue queue = declareQueue(smq, queueKind);
            this.measureWorkload(queue, options, options.warmupMillis());
        }
    }

    private Sample measure(
            final BacklogOptions options,
            final Durability durability,
            final QueueKind queueKind,
            final int backlogCount,
            final int repeat,
            final Path runDir)
            throws IOException, InterruptedException {
        final String condition = "depth-" + backlogCount + "-repeat-" + repeat;
        final Path database = databasePath(runDir, durability, queueKind, condition);
        Files.createDirectories(database.getParent());
        final QueueCounts startCounts;
        final Measurement measurement;
        final QueueCounts endCounts;
        try (Smq smq = Smq.sqlite(database).durability(durability).open()) {
            final Queue queue = declareQueue(smq, queueKind);
            this.preload(queue, options.payloadBytes(), backlogCount);
            if (queueKind == QueueKind.LEASED_HEAD) {
                this.leaseHead(queue, backlogCount);
            }
            startCounts = queue.counts();
            measurement = this.measureWorkload(queue, options, options.durationMillis());
            endCounts = queue.counts();
        } catch (final ExecutionException exception) {
            throw new IllegalStateException("backlog worker failed", exception.getCause());
        }
        final double seconds = options.durationMillis() / 1_000.0;
        final List<Long> sortedLatencies =
                measurement.latencies().stream().sorted().toList();
        return new Sample(
                durability,
                queueKind,
                backlogCount,
                repeat,
                depth(startCounts),
                startCounts,
                depth(endCounts),
                endCounts,
                measurement.published(),
                measurement.settled(),
                measurement.elapsedNanos(),
                measurement.published() / seconds,
                measurement.settled() / seconds,
                percentile(sortedLatencies, 0.50),
                percentile(sortedLatencies, 0.99),
                database.toString());
    }

    private Measurement measureWorkload(final Queue queue, final BacklogOptions options, final long durationMillis)
            throws InterruptedException, ExecutionException {
        final int workerCount = options.publishers() + options.consumers();
        final ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        final CountDownLatch ready = new CountDownLatch(workerCount);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicLong deadline = new AtomicLong();
        final List<Future<Long>> publishers = new ArrayList<>();
        final List<Future<ConsumerResult>> consumers = new ArrayList<>();
        try {
            for (int index = 0; index < options.publishers(); index++) {
                publishers.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return publishUntil(queue, options, deadline.get());
                }));
            }
            for (int index = 0; index < options.consumers(); index++) {
                consumers.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return consumeUntil(queue, options, deadline.get());
                }));
            }
            ready.await();
            final long startNanos = System.nanoTime();
            deadline.set(startNanos + durationMillis * 1_000_000L);
            start.countDown();

            long published = 0;
            long settled = 0;
            final List<Long> latencies = new ArrayList<>();
            for (final Future<Long> publisher : publishers) {
                published += publisher.get();
            }
            for (final Future<ConsumerResult> consumer : consumers) {
                final ConsumerResult result = consumer.get();
                settled += result.settled();
                latencies.addAll(result.latencies());
            }
            return new Measurement(published, settled, List.copyOf(latencies), System.nanoTime() - startNanos);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private static long publishUntil(final Queue queue, final BacklogOptions options, final long deadline)
            throws InterruptedException {
        final byte[] payload = payload(options.payloadBytes());
        long published = 0;
        while (System.nanoTime() < deadline) {
            final long publishStart = System.nanoTime();
            final OutgoingMessage message =
                    new OutgoingMessage(payload, Map.of(PUBLISHED_AT_HEADER, Long.toString(publishStart)));
            final List<OutgoingMessage> batch = Collections.nCopies(options.batchSize(), message);
            final int committed = queue.publish(batch).size();
            if (System.nanoTime() <= deadline) {
                published += committed;
            }
        }
        return published;
    }

    private static ConsumerResult consumeUntil(final Queue queue, final BacklogOptions options, final long deadline)
            throws InterruptedException {
        long settled = 0;
        final List<Long> latencies = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            final Chunk chunk = queue.pull(options.chunkSize(), Duration.ofMillis(1));
            final long deliveredAt = System.nanoTime();
            if (chunk.isEmpty()) {
                continue;
            }
            if (deliveredAt <= deadline) {
                for (final var message : chunk.messages()) {
                    final String publishedAt = message.headers().get(PUBLISHED_AT_HEADER);
                    if (publishedAt != null) {
                        latencies.add(Math.max(0, deliveredAt - Long.parseLong(publishedAt)));
                    }
                }
            }
            final Outcomes outcomes = queue.settle(chunk.ackAll());
            if (!outcomes.allOk()
                    || outcomes.byMessage().size() != chunk.messages().size()) {
                throw new IllegalStateException(
                        "settlement failed for " + chunk.messages().size() + " messages");
            }
            if (System.nanoTime() <= deadline) {
                settled += chunk.messages().size();
            }
        }
        return new ConsumerResult(settled, List.copyOf(latencies));
    }

    private void preload(final Queue queue, final int payloadBytes, final int count) {
        if (count == 0) {
            return;
        }
        final OutgoingMessage message = OutgoingMessage.of(payload(payloadBytes));
        final List<OutgoingMessage> batch = Collections.nCopies(PREFILL_BATCH_SIZE, message);
        for (int done = 0; done < count; done += PREFILL_BATCH_SIZE) {
            queue.publish(batch.subList(0, Math.min(PREFILL_BATCH_SIZE, count - done)));
        }
    }

    private void leaseHead(final Queue queue, final int depth) throws InterruptedException {
        for (int done = 0; done < depth; done += 10_000) {
            final int count = Math.min(10_000, depth - done);
            final Chunk chunk = queue.pull(count, Duration.ZERO, Duration.ofHours(1));
            if (chunk.messages().size() != count) {
                throw new IllegalStateException("could not lease the preloaded queue head");
            }
        }
    }

    private static Queue declareQueue(final Smq smq, final QueueKind queueKind) {
        final QueueOptions options = QueueOptions.defaults();
        return switch (queueKind) {
            case DEFAULT, LEASED_HEAD -> smq.declare(QUEUE, options);
            case MAX_DELIVERIES -> {
                smq.declare(DEAD_LETTER_QUEUE, QueueOptions.defaults());
                yield smq.declare(QUEUE, options.maxDeliveries(5, DEAD_LETTER_QUEUE));
            }
            case SINGLE_ACTIVE_CONSUMER -> smq.declare(QUEUE, options.singleActiveConsumer(true));
        };
    }

    private static Path databasePath(
            final Path runDir, final Durability durability, final QueueKind queueKind, final String condition) {
        return runDir.resolve(durability.name().toLowerCase())
                .resolve(queueKind.name().toLowerCase())
                .resolve(condition + ".db");
    }

    private static byte[] payload(final int size) {
        final byte[] payload = new byte[size];
        Arrays.fill(payload, (byte) 'x');
        return payload;
    }

    private static long depth(final QueueCounts counts) {
        return counts.ready() + counts.leased() + counts.delayed();
    }

    private static Long percentile(final List<Long> sorted, final double percentile) {
        if (sorted.isEmpty()) {
            return null;
        }
        final int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, index));
    }
}
