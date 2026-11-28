package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

final class RecoveryController {

    private static final String QUEUE = "recovery";
    private static final String CORRELATION_HEADER = "harness-correlation-id";

    void run(final RecoveryOptions options, final List<RecoveryResult> results) throws Exception {
        Files.createDirectories(options.workDir());
        for (final Durability durability : options.durabilities()) {
            this.verifyConfirmedPublish(options, durability, results);
            this.verifyConfirmedSettle(options, durability, results);
            this.verifyUnackedDelivery(options, durability, results);
            this.verifyAtomicPublishBatch(options, durability, results);
            this.verifyAtomicSettleBatch(options, durability, results);
        }
    }

    private void verifyConfirmedPublish(
            final RecoveryOptions options, final Durability durability, final List<RecoveryResult> results)
            throws Exception {
        final String suffix = durability.name().toLowerCase();
        final String requestId = "confirmed-publish-" + suffix;
        final String correlationId = "corr-" + requestId;
        final Path database = this.database(options, durability, "confirmed-publish");
        this.resetStore(database);
        final RecoveryResult result = new RecoveryResult(durability, "confirmed-publish", requestId, correlationId);
        results.add(result);

        this.runTarget(options, durability, database, "publish", requestId, correlationId, 1, result, "PUBLISH");
        if ("drop-confirmed-publish".equals(options.negativeControl())) {
            this.resetStore(database);
        }

        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            final Chunk chunk = queue.pull(1, Duration.ZERO);
            if (chunk.isEmpty()
                    || !correlationId.equals(
                            chunk.messages().getFirst().headers().get(CORRELATION_HEADER))) {
                result.recoveryObservation = "confirmed publish was not recovered after storage loss";
                throw new IllegalStateException("confirmed publish was not recovered: " + correlationId);
            }
            result.recoveryObservation =
                    "recovered message " + chunk.messages().getFirst().id().value();
            this.acknowledge(queue, chunk);
        }
    }

    private void verifyConfirmedSettle(
            final RecoveryOptions options, final Durability durability, final List<RecoveryResult> results)
            throws Exception {
        final String requestId = "confirmed-settle-" + durability.name().toLowerCase();
        final String correlationId = "corr-" + requestId;
        final Path database = this.database(options, durability, "confirmed-settle");
        this.seed(database, durability, options, List.of(correlationId));
        final RecoveryResult result = new RecoveryResult(durability, "confirmed-settle", requestId, correlationId);
        results.add(result);
        this.runTarget(options, durability, database, "settle", requestId, correlationId, 1, result, "SETTLE");

        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            final Chunk chunk = queue.pull(1, Duration.ZERO);
            if (!chunk.isEmpty()) {
                result.recoveryObservation = "message was redelivered after successful ACK";
                throw new IllegalStateException("successful ACK was redelivered: " + correlationId);
            }
            result.recoveryObservation = "no redelivery after successful ACK";
        }
    }

    private void verifyUnackedDelivery(
            final RecoveryOptions options, final Durability durability, final List<RecoveryResult> results)
            throws Exception {
        final String requestId = "unacked-delivery-" + durability.name().toLowerCase();
        final String correlationId = "corr-" + requestId;
        final Path database = this.database(options, durability, "unacked-delivery");
        this.seed(database, durability, options, List.of(correlationId));
        final RecoveryResult result = new RecoveryResult(durability, "unacked-delivery", requestId, correlationId);
        results.add(result);
        this.runTarget(options, durability, database, "deliver", requestId, correlationId, 1, result, "DELIVERY");

        TimeUnit.MILLISECONDS.sleep(options.leaseMillis() + 50);
        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            final Chunk chunk = queue.pull(1, Duration.ofMillis(options.leaseMillis() + 500));
            if (chunk.isEmpty()) {
                result.recoveryObservation = "lease expired but no message was redelivered";
                throw new IllegalStateException("unacked leased delivery was not redelivered: " + correlationId);
            }
            final var message = chunk.messages().getFirst();
            if (!correlationId.equals(message.headers().get(CORRELATION_HEADER)) || message.deliveryCount() < 2) {
                result.recoveryObservation = "redelivery did not match the original correlation or delivery count";
                throw new IllegalStateException(
                        "unacked leased delivery did not redeliver correctly: " + correlationId);
            }
            result.recoveryObservation =
                    "redelivered correlation " + correlationId + " deliveryCount=" + message.deliveryCount();
            this.acknowledge(queue, chunk);
        }
    }

    private void verifyAtomicPublishBatch(
            final RecoveryOptions options, final Durability durability, final List<RecoveryResult> results)
            throws Exception {
        final String requestId = "unknown-publish-batch-" + durability.name().toLowerCase();
        final String correlationId = "corr-" + requestId;
        final Path database = this.database(options, durability, "unknown-publish-batch");
        this.seed(database, durability, options, List.of());
        final RecoveryResult result =
                new RecoveryResult(durability, "ambiguous-publish-batch", requestId, correlationId);
        results.add(result);
        this.startAndKillUnknown(
                options,
                durability,
                database,
                "publish-batch",
                requestId,
                correlationId,
                options.messageCount(),
                result);

        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            final Chunk chunk = queue.pull(options.messageCount() + 1, Duration.ZERO);
            this.requireAtomicCount(chunk, options.messageCount(), "publish batch", result);
            this.verifyCorrelations(chunk, correlationId, options.messageCount());
            result.recoveryObservation = chunk.isEmpty()
                    ? "publish batch absent; no messages committed"
                    : "publish batch recovered all " + chunk.messages().size() + " messages";
            this.acknowledgeIfPresent(queue, chunk);
        }
    }

    private void verifyAtomicSettleBatch(
            final RecoveryOptions options, final Durability durability, final List<RecoveryResult> results)
            throws Exception {
        final String requestId = "unknown-settle-batch-" + durability.name().toLowerCase();
        final String correlationId = "corr-" + requestId;
        final Path database = this.database(options, durability, "unknown-settle-batch");
        this.seed(database, durability, options, correlations(correlationId, options.messageCount()));
        final RecoveryResult result =
                new RecoveryResult(durability, "ambiguous-settle-batch", requestId, correlationId);
        results.add(result);
        this.startAndKillUnknown(
                options,
                durability,
                database,
                "settle-batch",
                requestId,
                correlationId,
                options.messageCount(),
                result);

        TimeUnit.MILLISECONDS.sleep(options.leaseMillis() + 50);
        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            final Chunk chunk = queue.pull(options.messageCount() + 1, Duration.ofMillis(options.leaseMillis() + 500));
            this.requireAtomicCount(chunk, options.messageCount(), "settlement batch", result);
            this.verifyCorrelations(chunk, correlationId, options.messageCount());
            result.recoveryObservation = chunk.isEmpty()
                    ? "settlement batch applied; no messages redelivered"
                    : "settlement batch unapplied; all " + chunk.messages().size() + " messages redelivered";
            this.acknowledgeIfPresent(queue, chunk);
        }
    }

    private void runTarget(
            final RecoveryOptions options,
            final Durability durability,
            final Path database,
            final String operation,
            final String requestId,
            final String correlationId,
            final int messageCount,
            final RecoveryResult result,
            final String responseKind)
            throws Exception {
        final Process process =
                this.startTarget(options, database, durability, operation, requestId, correlationId, messageCount);
        try {
            final String response;
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                response = reader.readLine();
            }
            final String responsePrefix = "RESPONSE|" + requestId + "|" + responseKind + "|";
            if (response == null || !response.startsWith(responsePrefix)) {
                throw new IllegalStateException("target did not return " + responseKind + " response: " + response);
            }
            result.classification = "CONFIRMED";
            result.observedResponse = response;
        } finally {
            this.forceStop(process, result);
        }
    }

    private void startAndKillUnknown(
            final RecoveryOptions options,
            final Durability durability,
            final Path database,
            final String operation,
            final String requestId,
            final String correlationId,
            final int messageCount,
            final RecoveryResult result)
            throws Exception {
        final Process process =
                this.startTarget(options, database, durability, operation, requestId, correlationId, messageCount);
        try {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                final String started = reader.readLine();
                if (started == null || !started.startsWith("START|" + requestId + "|")) {
                    throw new IllegalStateException("target did not start the in-flight request: " + started);
                }
                result.requestStarted = started;
            }
        } finally {
            this.forceStop(process, result);
        }
        result.classification = "UNKNOWN";
        result.observedResponse = "";
    }

    private void forceStop(final Process process, final RecoveryResult result) throws InterruptedException {
        result.shutdownAt = Instant.now();
        process.destroyForcibly();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("target JVM did not exit after forced termination");
        }
    }

    private List<String> workerCommand(
            final RecoveryOptions options,
            final Path database,
            final Durability durability,
            final String operation,
            final String requestId,
            final String correlationId,
            final int messageCount) {
        final String modulePath = System.getProperty("smoke.modulePath");
        if (modulePath == null || modulePath.isBlank()) {
            throw new IllegalStateException("smoke.modulePath is required to launch the target JVM");
        }
        return List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=org.xerial.sqlitejdbc",
                "--module-path",
                modulePath,
                "-m",
                "io.github.ppzxc.smq.harness/io.github.ppzxc.smq.harness.Main",
                "--worker",
                operation,
                database.toString(),
                durability.name(),
                QUEUE,
                requestId,
                correlationId,
                Long.toString(options.leaseMillis()),
                Integer.toString(messageCount));
    }

    private Process startTarget(
            final RecoveryOptions options,
            final Path database,
            final Durability durability,
            final String operation,
            final String requestId,
            final String correlationId,
            final int messageCount)
            throws IOException {
        return new ProcessBuilder(this.workerCommand(
                        options, database, durability, operation, requestId, correlationId, messageCount))
                .redirectErrorStream(true)
                .start();
    }

    private Smq open(final Path database, final Durability durability, final RecoveryOptions options) {
        return Smq.sqlite(database)
                .durability(durability)
                .leaseDuration(Duration.ofMillis(options.leaseMillis()))
                .open();
    }

    private void seed(
            final Path database, final Durability durability, final RecoveryOptions options, final List<String> ids)
            throws IOException {
        this.resetStore(database);
        Files.createDirectories(database.getParent());
        try (Smq smq = this.open(database, durability, options)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            if (!ids.isEmpty()) {
                queue.publish(ids.stream().map(this::message).toList());
            }
        }
    }

    private Path database(final RecoveryOptions options, final Durability durability, final String scenario) {
        return options.workDir().resolve(durability.name().toLowerCase()).resolve(scenario + ".db");
    }

    private void resetStore(final Path database) throws IOException {
        Files.createDirectories(database.getParent());
        Files.deleteIfExists(database);
        Files.deleteIfExists(database.resolveSibling(database.getFileName() + "-wal"));
        Files.deleteIfExists(database.resolveSibling(database.getFileName() + "-shm"));
    }

    private OutgoingMessage message(final String correlationId) {
        return new OutgoingMessage(
                correlationId.getBytes(StandardCharsets.UTF_8), Map.of(CORRELATION_HEADER, correlationId));
    }

    private static List<String> correlations(final String prefix, final int count) {
        return IntStream.range(0, count).mapToObj(index -> prefix + "-" + index).toList();
    }

    private void verifyCorrelations(final Chunk chunk, final String prefix, final int expectedCount) {
        for (int index = 0; index < chunk.messages().size(); index++) {
            final String expected = prefix + "-" + index;
            final String actual = chunk.messages().get(index).headers().get(CORRELATION_HEADER);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("recovered batch correlation mismatch: expected " + expected + ", got "
                        + actual + " (count " + expectedCount + ")");
            }
        }
    }

    private void requireAtomicCount(
            final Chunk chunk, final int expectedCount, final String operation, final RecoveryResult result) {
        final int recoveredCount = chunk.messages().size();
        if (recoveredCount != 0 && recoveredCount != expectedCount) {
            result.recoveryObservation =
                    "recovered " + recoveredCount + " of " + expectedCount + " " + operation + " messages";
            throw new IllegalStateException(operation + " was partially applied after an unknown result");
        }
    }

    private void acknowledgeIfPresent(final Queue queue, final Chunk chunk) {
        if (!chunk.isEmpty()) {
            this.acknowledge(queue, chunk);
        }
    }

    private void acknowledge(final Queue queue, final Chunk chunk) {
        final Outcomes outcomes = queue.settle(chunk.ackAll());
        if (!outcomes.allOk()) {
            throw new IllegalStateException("could not settle recovered messages from " + queue.name());
        }
    }
}
