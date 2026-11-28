package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

final class RecoveryWorker {

    private static final String CORRELATION_HEADER = "harness-correlation-id";

    private RecoveryWorker() {}

    static int execute(final String[] args) {
        if (args.length != 9 || !"--worker".equals(args[0])) {
            System.err.println("invalid recovery worker arguments");
            return 1;
        }
        final String operation = args[1];
        final Path database = Path.of(args[2]);
        final Durability durability = Durability.valueOf(args[3]);
        final String queueName = args[4];
        final String requestId = args[5];
        final String correlationId = args[6];
        final long leaseMillis = Long.parseLong(args[7]);
        final int messageCount = Integer.parseInt(args[8]);
        try (Smq smq = Smq.sqlite(database)
                .durability(durability)
                .leaseDuration(Duration.ofMillis(leaseMillis))
                .open()) {
            final Queue queue = smq.declare(queueName, QueueOptions.defaults());
            return switch (operation) {
                case "publish" -> publish(queue, requestId, correlationId);
                case "settle" -> settle(queue, requestId, correlationId);
                case "deliver" -> deliver(queue, requestId, correlationId);
                case "publish-batch" -> publishBatch(queue, requestId, correlationId, messageCount);
                case "settle-batch" -> settleBatch(queue, requestId, correlationId, messageCount);
                default -> throw new IllegalArgumentException("unknown worker operation: " + operation);
            };
        } catch (final Exception exception) {
            exception.printStackTrace(System.err);
            return 1;
        }
    }

    private static int publish(final Queue queue, final String requestId, final String correlationId)
            throws IOException, InterruptedException {
        final var id = queue.publish(message(correlationId));
        respond("RESPONSE|" + requestId + "|PUBLISH|" + correlationId + "|" + id.value());
        awaitTermination();
        return 0;
    }

    private static int settle(final Queue queue, final String requestId, final String correlationId)
            throws IOException, InterruptedException {
        final Chunk chunk = requiredChunk(queue, 1);
        final Outcomes outcomes = queue.settle(chunk.ackAll());
        requireAllOk(outcomes, "settlement");
        respond("RESPONSE|" + requestId + "|SETTLE|" + correlationId + "|acknowledged=1");
        awaitTermination();
        return 0;
    }

    private static int deliver(final Queue queue, final String requestId, final String correlationId)
            throws IOException, InterruptedException {
        final Chunk chunk = requiredChunk(queue, 1);
        final var message = chunk.messages().getFirst();
        final String observed = message.headers().get(CORRELATION_HEADER);
        respond("RESPONSE|" + requestId + "|DELIVERY|" + observed + "|deliveryCount=" + message.deliveryCount());
        awaitTermination();
        return 0;
    }

    private static int publishBatch(
            final Queue queue, final String requestId, final String correlationId, final int count)
            throws IOException, InterruptedException {
        final List<OutgoingMessage> batch = IntStream.range(0, count)
                .mapToObj(index -> message(correlationId + "-" + index))
                .toList();
        respond("START|" + requestId + "|PUBLISH_BATCH|" + correlationId + "|count=" + count);
        final List<?> ids = queue.publish(batch);
        respond("RESPONSE|" + requestId + "|PUBLISH_BATCH|" + correlationId + "|count=" + ids.size());
        awaitTermination();
        return 0;
    }

    private static int settleBatch(
            final Queue queue, final String requestId, final String correlationId, final int count)
            throws IOException, InterruptedException {
        final Chunk chunk = requiredChunk(queue, count);
        if (chunk.messages().size() != count) {
            throw new IllegalStateException("expected " + count + " leased messages, got "
                    + chunk.messages().size());
        }
        respond("START|" + requestId + "|SETTLE_BATCH|" + correlationId + "|count=" + count);
        final Outcomes outcomes = queue.settle(chunk.ackAll());
        requireAllOk(outcomes, "batch settlement");
        respond("RESPONSE|" + requestId + "|SETTLE_BATCH|" + correlationId + "|count=" + count);
        awaitTermination();
        return 0;
    }

    private static Chunk requiredChunk(final Queue queue, final int count) throws InterruptedException {
        final Chunk chunk = queue.pull(count, Duration.ofSeconds(5));
        if (chunk.isEmpty()) {
            throw new IllegalStateException("no message available for " + queue.name());
        }
        return chunk;
    }

    private static OutgoingMessage message(final String correlationId) {
        return new OutgoingMessage(
                correlationId.getBytes(StandardCharsets.UTF_8), Map.of(CORRELATION_HEADER, correlationId));
    }

    private static void requireAllOk(final Outcomes outcomes, final String operation) {
        if (!outcomes.allOk()) {
            throw new IllegalStateException(operation + " was not fully applied");
        }
    }

    private static void respond(final String response) {
        System.out.println(response);
        System.out.flush();
    }

    private static void awaitTermination() throws IOException, InterruptedException {
        System.in.read();
    }
}
