package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class StorageLossController {

    private static final String QUEUE = "storage-loss";
    private static final String CORRELATION_HEADER = "harness-correlation-id";

    private final Path stores;
    private final Path ledger;
    private final StorageLossOptions options;

    StorageLossController(final Path stores, final Path ledger, final StorageLossOptions options) {
        this.stores = stores;
        this.ledger = ledger;
        this.options = options;
    }

    void observeProcessCrashOutcomes(final List<StorageLossResult> results) throws Exception {
        if (!this.options.durabilities().contains(Durability.PROCESS_CRASH)) {
            return;
        }
        this.observeProcessCrashPublish(results);
        this.observeProcessCrashSettlement(results);
    }

    void verifySimulatedStorageLoss(final List<StorageLossResult> results) throws Exception {
        this.verifyPowerLossPublish(results);
        this.verifyPowerLossSettlement(results);
        this.simulateLossOfUnsyncedWrites(results);
        this.simulateLossAfterAppliedSettlement(results);
    }

    StorageLossResult notRunSimulation(final String reason) {
        final StorageLossResult result = new StorageLossResult(
                "simulated-storage-loss",
                StorageLossResult.SIMULATED_STORAGE_LOSS,
                Durability.PROCESS_CRASH,
                "not-run");
        result.status = "NOT_RUN";
        result.recoveryObservation = reason;
        return result;
    }

    private void observeProcessCrashPublish(final List<StorageLossResult> results) throws Exception {
        final String correlationId = "process-crash-publish";
        final Path database = this.database("process-crash-publish.db");
        this.initialize(database, Durability.PROCESS_CRASH, List.of());
        final StorageLossResult result = this.result(
                results,
                "process-crash-publish",
                StorageLossResult.JVM_TERMINATION,
                Durability.PROCESS_CRASH,
                correlationId);
        this.runConfirmedWorker(database, Durability.PROCESS_CRASH, "publish", correlationId, result);

        try (Smq smq = this.open(database, Durability.PROCESS_CRASH)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(2, Duration.ZERO);
            if (chunk.isEmpty()) {
                result.recoveryObservation = "successful PROCESS_CRASH publish was lost after JVM termination";
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            final String observed = correlation(chunk);
            if (!correlationId.equals(observed)) {
                result.recoveryObservation = "observed another message after JVM termination: " + observed;
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            result.recoveryObservation = "successful publish recovered after JVM termination; messageId="
                    + chunk.messages().getFirst().id().value();
            result.status = "PASSED";
        }
    }

    private void observeProcessCrashSettlement(final List<StorageLossResult> results) throws Exception {
        final String correlationId = "process-crash-settlement";
        final Path database = this.database("process-crash-settlement.db");
        this.initialize(database, Durability.PROCESS_CRASH, List.of(correlationId));
        final StorageLossResult result = this.result(
                results,
                "process-crash-settlement",
                StorageLossResult.JVM_TERMINATION,
                Durability.PROCESS_CRASH,
                correlationId);
        this.runConfirmedWorker(database, Durability.PROCESS_CRASH, "settle", correlationId, result);

        try (Smq smq = this.open(database, Durability.PROCESS_CRASH)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(2, Duration.ZERO);
            if (!chunk.isEmpty()) {
                result.recoveryObservation =
                        "successful PROCESS_CRASH settlement was redelivered after JVM termination";
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            result.recoveryObservation = "no redelivery after successful settlement and JVM termination";
            result.status = "PASSED";
        }
    }

    private void verifyPowerLossPublish(final List<StorageLossResult> results) throws Exception {
        final String correlationId = "power-loss-publish";
        final Path database = this.database("power-loss-publish.db");
        final Path beforePublish = this.stores.resolve("power-loss-publish-before.db");
        this.initialize(database, Durability.POWER_LOSS, List.of());
        this.snapshot(database, beforePublish);
        final StorageLossResult result = this.result(
                results, "power-loss-publish", StorageLossResult.JVM_TERMINATION, Durability.POWER_LOSS, correlationId);
        this.runConfirmedWorker(database, Durability.POWER_LOSS, "publish", correlationId, result);

        if ("drop-confirmed-publish".equals(this.options.negativeControl())) {
            result.faultType = StorageLossResult.SIMULATED_STORAGE_LOSS;
            this.restoreSnapshot(beforePublish, database);
            result.recoveryObservation = "negative control removed a confirmed publish before recovery validation";
            result.status = "NEGATIVE_CONTROL";
        }

        try (Smq smq = this.open(database, Durability.POWER_LOSS)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(2, Duration.ZERO);
            if (chunk.isEmpty()) {
                if ("NEGATIVE_CONTROL".equals(result.status)) {
                    throw new IllegalStateException("confirmed publish was not recovered after storage loss");
                }
                result.recoveryObservation = "confirmed POWER_LOSS publish was lost after JVM termination";
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            final String observed = correlation(chunk);
            if (!correlationId.equals(observed)) {
                result.recoveryObservation = "recovered wrong correlation after confirmed publish: " + observed;
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            result.recoveryObservation = "confirmed POWER_LOSS publish recovered; messageId="
                    + chunk.messages().getFirst().id().value();
            result.status = "PASSED";
        }
    }

    private void verifyPowerLossSettlement(final List<StorageLossResult> results) throws Exception {
        final String correlationId = "power-loss-settlement";
        final Path database = this.database("power-loss-settlement.db");
        this.initialize(database, Durability.POWER_LOSS, List.of(correlationId));
        final StorageLossResult result = this.result(
                results,
                "power-loss-settlement",
                StorageLossResult.JVM_TERMINATION,
                Durability.POWER_LOSS,
                correlationId);
        this.runConfirmedWorker(database, Durability.POWER_LOSS, "settle", correlationId, result);

        try (Smq smq = this.open(database, Durability.POWER_LOSS)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(2, Duration.ZERO);
            if (!chunk.isEmpty()) {
                result.recoveryObservation = "successfully settled POWER_LOSS message was redelivered";
                result.status = "FAILED";
                throw new IllegalStateException(result.recoveryObservation);
            }
            result.recoveryObservation = "no redelivery after successful POWER_LOSS settlement";
            result.status = "PASSED";
        }
    }

    private void simulateLossOfUnsyncedWrites(final List<StorageLossResult> results) throws Exception {
        final String baselineId = "synced-baseline";
        final String durableWriteId = "synced-write";
        final String unsyncedWriteId = "unsynced-write";
        final Path database = this.database("simulated-storage-loss.db");
        final Path syncSnapshot = this.stores.resolve("simulated-storage-loss-latest-sync.db");

        this.initialize(database, Durability.POWER_LOSS, List.of(baselineId));

        final StorageLossResult durable = this.result(
                results,
                "simulated-storage-loss-synced-write",
                StorageLossResult.SIMULATED_STORAGE_LOSS,
                Durability.POWER_LOSS,
                durableWriteId);
        this.runConfirmedWorker(database, Durability.POWER_LOSS, "publish", durableWriteId, durable);
        this.snapshot(database, syncSnapshot);

        final StorageLossResult volatileWrite = this.result(
                results,
                "simulated-storage-loss-unsynced-write",
                StorageLossResult.SIMULATED_STORAGE_LOSS,
                Durability.PROCESS_CRASH,
                unsyncedWriteId);
        this.runConfirmedWorker(database, Durability.PROCESS_CRASH, "publish", unsyncedWriteId, volatileWrite);
        this.restoreSnapshot(syncSnapshot, database);

        try (Smq smq = this.open(database, Durability.PROCESS_CRASH)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(10, Duration.ZERO);
            final List<String> correlations = chunk.messages().stream()
                    .map(message -> message.headers().get(CORRELATION_HEADER))
                    .toList();
            if (!correlations.contains(baselineId) || !correlations.contains(durableWriteId)) {
                durable.recoveryObservation =
                        "a synchronized message was missing after restoring sync snapshot: " + correlations;
                durable.status = "FAILED";
                throw new IllegalStateException(durable.recoveryObservation);
            }
            if (correlations.contains(unsyncedWriteId)) {
                volatileWrite.recoveryObservation = "unsynced PROCESS_CRASH write remained after snapshot restore";
                volatileWrite.status = "FAILED";
                throw new IllegalStateException(volatileWrite.recoveryObservation);
            }
            durable.recoveryObservation = "baseline and POWER_LOSS write preserved in last synchronized snapshot";
            durable.status = "PASSED";
            volatileWrite.recoveryObservation = "PROCESS_CRASH write discarded by restoring last synchronized snapshot";
            volatileWrite.status = "PASSED";
        }
    }

    private void simulateLossAfterAppliedSettlement(final List<StorageLossResult> results) throws Exception {
        final String settledId = "settled-before-storage-loss";
        final String unsyncedWriteId = "unsynced-after-settlement";
        final Path database = this.database("simulated-storage-loss-settlement.db");
        final Path syncSnapshot = this.stores.resolve("simulated-storage-loss-settlement-sync.db");
        this.initialize(database, Durability.POWER_LOSS, List.of(settledId));

        final StorageLossResult settlement = this.result(
                results,
                "simulated-storage-loss-synced-settlement",
                StorageLossResult.SIMULATED_STORAGE_LOSS,
                Durability.POWER_LOSS,
                "power-loss-settlement-simulation");
        this.runConfirmedWorker(database, Durability.POWER_LOSS, "settle", settledId, settlement);
        this.snapshot(database, syncSnapshot);

        final StorageLossResult unsynced = this.result(
                results,
                "simulated-storage-loss-after-settlement",
                StorageLossResult.SIMULATED_STORAGE_LOSS,
                Durability.PROCESS_CRASH,
                unsyncedWriteId);
        this.runConfirmedWorker(database, Durability.PROCESS_CRASH, "publish", unsyncedWriteId, unsynced);
        this.restoreSnapshot(syncSnapshot, database);

        try (Smq smq = this.open(database, Durability.PROCESS_CRASH)) {
            final Chunk chunk = smq.declare(QUEUE, QueueOptions.defaults()).pull(2, Duration.ZERO);
            if (!chunk.isEmpty()) {
                final String observed = correlation(chunk);
                settlement.recoveryObservation =
                        "settled message or later unsynced write reappeared after snapshot restore: " + observed;
                settlement.status = "FAILED";
                throw new IllegalStateException(settlement.recoveryObservation);
            }
            settlement.recoveryObservation = "POWER_LOSS settlement remained applied in last synchronized snapshot";
            settlement.status = "PASSED";
            unsynced.recoveryObservation = "later PROCESS_CRASH write discarded; settled message stayed deleted";
            unsynced.status = "PASSED";
        }
    }

    private StorageLossResult result(
            final List<StorageLossResult> results,
            final String scenario,
            final String faultType,
            final Durability durability,
            final String requestId) {
        final StorageLossResult result = new StorageLossResult(scenario, faultType, durability, requestId);
        results.add(result);
        return result;
    }

    private void runConfirmedWorker(
            final Path database,
            final Durability durability,
            final String operation,
            final String correlationId,
            final StorageLossResult result)
            throws Exception {
        final Process process = new ProcessBuilder(
                        this.workerCommand(database, durability, operation, result.requestId, correlationId))
                .redirectErrorStream(true)
                .start();
        try {
            final String response;
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                response = reader.readLine();
            }
            final String responseKind = switch (operation) {
                case "publish" -> "PUBLISH";
                case "settle" -> "SETTLE";
                default -> throw new IllegalArgumentException("unsupported storage-loss operation: " + operation);
            };
            final String expectedPrefix =
                    "RESPONSE|" + result.requestId + "|" + responseKind + "|" + correlationId + "|";
            if (response == null || !response.startsWith(expectedPrefix)) {
                result.completionRecord = "UNKNOWN: no completion response observed";
                throw new IllegalStateException("worker did not confirm " + operation + ": " + response);
            }
            result.completionRecord = response;
            this.appendCompletionRecord(response);
        } finally {
            result.shutdownAt = Instant.now();
            process.destroyForcibly();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("worker JVM did not exit after forced termination");
            }
        }
    }

    private List<String> workerCommand(
            final Path database,
            final Durability durability,
            final String operation,
            final String requestId,
            final String correlationId) {
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
                Long.toString(this.options.leaseMillis()),
                "1");
    }

    private void appendCompletionRecord(final String response) throws IOException {
        final byte[] line = (response + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(
                this.ledger, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(line);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private void initialize(final Path database, final Durability durability, final List<String> messages)
            throws IOException {
        Files.createDirectories(database.getParent());
        this.resetStore(database);
        try (Smq smq = this.open(database, durability)) {
            final Queue queue = smq.declare(QUEUE, QueueOptions.defaults());
            if (!messages.isEmpty()) {
                queue.publish(messages.stream().map(this::message).toList());
            }
        }
    }

    private void snapshot(final Path database, final Path snapshot) throws IOException {
        Files.deleteIfExists(snapshot);
        try (Smq smq = this.open(database, Durability.POWER_LOSS)) {
            smq.declare(QUEUE, QueueOptions.defaults());
            smq.snapshot(snapshot);
        }
        try (FileChannel channel = FileChannel.open(snapshot, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private void restoreSnapshot(final Path snapshot, final Path database) throws IOException {
        this.resetStore(database);
        Files.copy(snapshot, database);
    }

    private void resetStore(final Path database) throws IOException {
        Files.deleteIfExists(database);
        Files.deleteIfExists(database.resolveSibling(database.getFileName() + "-wal"));
        Files.deleteIfExists(database.resolveSibling(database.getFileName() + "-shm"));
        Files.deleteIfExists(database.resolveSibling(database.getFileName() + ".lock"));
    }

    private Path database(final String name) {
        return this.stores.resolve(name);
    }

    private Smq open(final Path database, final Durability durability) {
        return Smq.sqlite(database)
                .durability(durability)
                .leaseDuration(Duration.ofMillis(this.options.leaseMillis()))
                .open();
    }

    private OutgoingMessage message(final String correlationId) {
        return new OutgoingMessage(
                correlationId.getBytes(StandardCharsets.UTF_8), Map.of(CORRELATION_HEADER, correlationId));
    }

    private static String correlation(final Chunk chunk) {
        return chunk.messages().getFirst().headers().get(CORRELATION_HEADER);
    }
}
