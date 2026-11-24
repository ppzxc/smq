package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Runs a bounded overload and recovery against a fresh SQLite queue. */
public final class BacklogBench {
    private static final int BATCH = 100;
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    record Rates(long initial, long ack, long end, long rampSeconds) {
        Rates {
            if (end <= 0 || end >= ack || ack >= initial || rampSeconds <= 0) {
                throw new IllegalArgumentException("Expected 0 < final IN < ACK < initial IN and a positive ramp");
            }
        }

        double publishAt(double elapsedSeconds) {
            return initial - (initial - end) * Math.min(1, elapsedSeconds / rampSeconds);
        }
    }

    private BacklogBench() {}

    public static void main(String[] args) throws Exception {
        ModuleGuard.requireSmqModule("backlog benchmark");
        if (args.length != 7) {
            throw new IllegalArgumentException("Expected work directory and six workload arguments");
        }
        long target = Long.parseLong(args[1]);
        if (target < BATCH) throw new IllegalArgumentException("Depth must be at least one batch");
        Rates rates = new Rates(
                Long.parseLong(args[2]), Long.parseLong(args[3]), Long.parseLong(args[4]), Long.parseLong(args[5]));
        Path dir = Files.createTempDirectory(Files.createDirectories(Path.of(args[0])), "backlog-");
        System.out.println("workDir=" + dir.toAbsolutePath() + " target=" + target + " rates=" + rates);
        Duration timeout = Duration.ofSeconds(Long.parseLong(args[6]));
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Timeout must be positive");
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db"))
                .durability(Durability.PROCESS_CRASH)
                .open()) {
            run(smq.declare("backlog", QueueOptions.defaults()), rates, target, timeout);
        }
    }

    private static void run(Queue queue, Rates rates, long target, Duration timeout) throws Exception {
        AtomicLong acked = new AtomicLong();
        AtomicBoolean finish = new AtomicBoolean();
        long started = System.nanoTime();
        long deadline = started + timeout.toNanos();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var consumer = executor.submit(() -> {
                consume(queue, rates.ack(), acked, finish, deadline);
                return null;
            });
            long published = 0;
            long ramp = 0;
            long recoveredSince = 0;
            long lastReport = started;
            long peak = 0;
            try {
                while (true) {
                    if (consumer.isDone()) consumer.get();
                    long now = System.nanoTime();
                    if (now >= deadline) throw new IllegalStateException("Overload or recovery timed out");
                    long acknowledged = acked.get();
                    long depth = published - acknowledged;
                    peak = Math.max(peak, depth);
                    if (ramp == 0 && depth >= target) {
                        ramp = now;
                        System.out.println("ramp started at depth=" + depth);
                    }
                    double rampElapsed = ramp == 0 ? 0 : (now - ramp) / (double) SECOND;
                    if (rampElapsed >= rates.rampSeconds() && depth <= 2 * BATCH) {
                        if (recoveredSince == 0) recoveredSince = now;
                        if (now - recoveredSince >= 5 * SECOND) break;
                    } else {
                        recoveredSince = 0;
                    }
                    if (now - lastReport >= SECOND) {
                        System.out.printf(
                                "t=%.1fs in=%d ack=%d depth=%d targetIn=%.0f%n",
                                (now - started) / (double) SECOND,
                                published,
                                acknowledged,
                                depth,
                                rates.publishAt(rampElapsed));
                        lastReport = now;
                    }
                    var batch = new ArrayList<OutgoingMessage>(BATCH);
                    for (int i = 0; i < BATCH; i++) {
                        batch.add(OutgoingMessage.of(
                                ByteBuffer.allocate(256).putLong(published + i).array()));
                    }
                    if (queue.publish(batch).size() != BATCH) throw new IllegalStateException("Incomplete publish");
                    published += BATCH;
                    pace(now, BATCH, rates.publishAt(rampElapsed));
                }
                finish.set(true);
                consumer.get();
                var counts = queue.counts();
                if (acked.get() != published || counts.ready() != 0 || counts.leased() != 0 || counts.delayed() != 0) {
                    throw new IllegalStateException("Final counts do not match: " + counts);
                }
                System.out.printf("PASS peak=%d published=%d acked=%d%n", peak, published, acked.get());
            } finally {
                consumer.cancel(true);
            }
        }
    }

    private static void consume(Queue queue, long rate, AtomicLong acked, AtomicBoolean finish, long deadline)
            throws InterruptedException {
        while (System.nanoTime() < deadline) {
            long now = System.nanoTime();
            boolean draining = finish.get();
            var chunk = queue.pull(BATCH, Duration.ofMillis(20));
            if (chunk.isEmpty()) {
                if (draining) return;
                continue;
            }
            long expected = acked.get();
            for (var message : chunk.messages()) {
                if (ByteBuffer.wrap(message.payload()).getLong() != expected++) {
                    throw new IllegalStateException("Unexpected message sequence");
                }
            }
            var outcomes = queue.settle(chunk.ackAll());
            if (!outcomes.allOk()
                    || outcomes.byMessage().size() != chunk.messages().size()) {
                throw new IllegalStateException("Incomplete ACK");
            }
            acked.set(expected);
            pace(now, chunk.messages().size(), rate);
        }
        throw new IllegalStateException("Consumer timed out");
    }

    private static void pace(long started, int size, double rate) throws InterruptedException {
        long remaining = (long) (size * (double) SECOND / rate) - (System.nanoTime() - started);
        if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
    }
}
