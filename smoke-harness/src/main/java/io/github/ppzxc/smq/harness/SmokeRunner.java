package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
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

final class SmokeRunner {

    record Sample(
            Durability durability,
            Options.Mode mode,
            int repeat,
            long completed,
            long failed,
            int maxObservedBatchSize,
            int maxObservedInFlight,
            long elapsedNanos,
            double throughputMessagesPerSecond,
            long publishLatencyP50Nanos,
            long publishLatencyP99Nanos,
            long settleLatencyP50Nanos,
            long settleLatencyP99Nanos,
            List<Long> publishLatencyNanos,
            List<Long> settleLatencyNanos) {}

    void run(final Options options, final List<Sample> results) throws IOException, InterruptedException {
        Files.createDirectories(options.workDir());
        for (final Durability durability : options.durabilities()) {
            for (final Options.Mode mode : options.modes()) {
                if (options.warmupMessages() > 0) {
                    this.executeWorkload(options, durability, mode, options.warmupMessages(), "warmup", 0);
                }
                for (int repeat = 1; repeat <= options.repeats(); repeat++) {
                    results.add(this.executeWorkload(
                            options, durability, mode, options.messageCount(), "measurement", repeat));
                }
            }
        }
    }

    private Sample executeWorkload(
            final Options options,
            final Durability durability,
            final Options.Mode mode,
            final int messageCount,
            final String phase,
            final int repeat)
            throws IOException, InterruptedException {
        final Path database = options.workDir()
                .resolve(durability.name().toLowerCase())
                .resolve(mode.name().toLowerCase())
                .resolve(phase + "-" + repeat + ".db");
        Files.createDirectories(database.getParent());

        final List<Long> publishSamples = new ArrayList<>();
        final List<Long> settleSamples = new ArrayList<>();
        long completed = 0;
        int maxObservedBatchSize = 0;
        int maxObservedInFlight = 0;
        long elapsed = 0;
        try (Smq smq = Smq.sqlite(database).durability(durability).open()) {
            final Queue queue = smq.declare("smoke", QueueOptions.defaults());
            final byte[] payload = new byte[options.payloadBytes()];
            Arrays.fill(payload, (byte) 'x');
            final OutgoingMessage message = OutgoingMessage.of(payload);

            final long start = System.nanoTime();
            int remaining = messageCount;
            while (remaining > 0) {
                final int windowSize = Math.min(remaining, options.maxInFlight());
                maxObservedBatchSize = Math.max(
                        maxObservedBatchSize,
                        this.publish(queue, mode, message, windowSize, options.batchSize(), publishSamples));
                maxObservedInFlight = Math.max(maxObservedInFlight, windowSize);

                int unsettled = windowSize;
                while (unsettled > 0) {
                    final Chunk chunk = queue.pull(Math.min(unsettled, options.chunkSize()), Duration.ofSeconds(5));
                    if (chunk.isEmpty()) {
                        throw new IllegalStateException("pull returned no message while " + unsettled + " remain");
                    }
                    final var settlement = chunk.ackAll();
                    final long settleStart = System.nanoTime();
                    final Outcomes outcomes = queue.settle(settlement);
                    settleSamples.add(System.nanoTime() - settleStart);
                    if (!outcomes.allOk()
                            || outcomes.byMessage().size() != chunk.messages().size()) {
                        throw new IllegalStateException(
                                "settlement failed for " + chunk.messages().size() + " messages");
                    }
                    completed += chunk.messages().size();
                    unsettled -= chunk.messages().size();
                }
                remaining -= windowSize;
            }
            elapsed = System.nanoTime() - start;
        }
        final double throughput = completed * 1_000_000_000.0 / elapsed;
        return new Sample(
                durability,
                mode,
                repeat,
                completed,
                0,
                maxObservedBatchSize,
                maxObservedInFlight,
                elapsed,
                throughput,
                percentile(publishSamples, 0.50),
                percentile(publishSamples, 0.99),
                percentile(settleSamples, 0.50),
                percentile(settleSamples, 0.99),
                List.copyOf(publishSamples),
                List.copyOf(settleSamples));
    }

    private int publish(
            final Queue queue,
            final Options.Mode mode,
            final OutgoingMessage message,
            final int count,
            final int batchSize,
            final List<Long> samples) {
        if (mode == Options.Mode.SINGLE) {
            for (int index = 0; index < count; index++) {
                final long start = System.nanoTime();
                queue.publish(message);
                samples.add(System.nanoTime() - start);
            }
            return count == 0 ? 0 : 1;
        }

        int remaining = count;
        int maxBatch = 0;
        while (remaining > 0) {
            final int size = Math.min(remaining, batchSize);
            final List<OutgoingMessage> batch = Collections.nCopies(size, message);
            final long start = System.nanoTime();
            queue.publish(batch);
            samples.add(System.nanoTime() - start);
            maxBatch = Math.max(maxBatch, size);
            remaining -= size;
        }
        return maxBatch;
    }

    private static long percentile(final List<Long> samples, final double percentile) {
        final long[] sorted =
                samples.stream().mapToLong(Long::longValue).sorted().toArray();
        final int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }
}
