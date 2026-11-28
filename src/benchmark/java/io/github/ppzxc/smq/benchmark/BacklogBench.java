package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
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

        double publishAt(final double elapsedSeconds) {
            return initial - (initial - end) * Math.min(1, elapsedSeconds / rampSeconds);
        }
    }

    private BacklogBench() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(final String[] args) throws Exception {
        final long target;
        final Rates rates;
        final Path dir;
        final Duration timeout;
        ModuleGuard.requireSmqModule("backlog benchmark");
        if (args.length != 7) {
            throw new IllegalArgumentException("Expected work directory and six workload arguments");
        }
        target = Long.parseLong(args[1]);
        if (target < BATCH) throw new IllegalArgumentException("Depth must be at least one batch");
        rates = new Rates(
                Long.parseLong(args[2]), Long.parseLong(args[3]), Long.parseLong(args[4]), Long.parseLong(args[5]));
        dir = Files.createTempDirectory(Files.createDirectories(Path.of(args[0])), "backlog-");
        System.out.println("workDir=" + dir.toAbsolutePath() + " target=" + target + " rates=" + rates);
        timeout = Duration.ofSeconds(Long.parseLong(args[6]));
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Timeout must be positive");
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db"))
                .durability(Durability.PROCESS_CRASH)
                .open()) {
            run(smq.declare("backlog", QueueOptions.defaults()), rates, target, timeout);
        }
    }

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The load, ramp, recovery, and timeout decisions share the same concurrent run state.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.CyclomaticComplexity",
            reason = "The load, ramp, recovery, and timeout decisions share the same concurrent run state.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.SystemPrintln", "PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
    private static void run(final Queue queue, final Rates rates, final long target, final Duration timeout)
            throws ExecutionException, InterruptedException {
        final AtomicLong acked;
        final AtomicBoolean finish;
        final long started;
        final long deadline;
        acked = new AtomicLong();
        finish = new AtomicBoolean();
        started = System.nanoTime();
        deadline = started + timeout.toNanos();
        try (var executor = Executors.newSingleThreadExecutor()) {
            long published;
            long ramp;
            long recoveredSince;
            long lastReport;
            long peak;
            final var consumer = executor.submit(() -> {
                consume(queue, rates.ack(), acked, finish, deadline);
                return null;
            });
            published = 0;
            ramp = 0;
            recoveredSince = 0;
            lastReport = started;
            peak = 0;
            try {
                while (true) {
                    final long now;
                    final long acknowledged;
                    final long depth;
                    final double rampElapsed;
                    if (consumer.isDone()) consumer.get();
                    now = System.nanoTime();
                    if (now >= deadline) throw new IllegalStateException("Overload or recovery timed out");
                    acknowledged = acked.get();
                    depth = published - acknowledged;
                    peak = Math.max(peak, depth);
                    if (ramp == 0 && depth >= target) {
                        ramp = now;
                        System.out.println("ramp started at depth=" + depth);
                    }
                    rampElapsed = ramp == 0 ? 0 : (now - ramp) / (double) SECOND;
                    if (rampElapsed >= rates.rampSeconds() && depth <= 2 * BATCH) {
                        if (recoveredSince == 0) recoveredSince = now;
                        if (now - recoveredSince >= 5 * SECOND) break;
                    } else {
                        recoveredSince = 0;
                    }
                    if (now - lastReport >= SECOND) {
                        System.out.printf(
                                Locale.getDefault(Locale.Category.FORMAT),
                                "t=%.1fs in=%d ack=%d depth=%d targetIn=%.0f%n",
                                (now - started) / (double) SECOND,
                                published,
                                acknowledged,
                                depth,
                                rates.publishAt(rampElapsed));
                        lastReport = now;
                    }
                    final var batch = new ArrayList<OutgoingMessage>(BATCH);
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
                final var counts = queue.counts();
                if (acked.get() != published || counts.ready() != 0 || counts.leased() != 0 || counts.delayed() != 0) {
                    throw new IllegalStateException("Final counts do not match: " + counts);
                }
                System.out.printf(
                        Locale.getDefault(Locale.Category.FORMAT),
                        "PASS peak=%d published=%d acked=%d%n",
                        peak,
                        published,
                        acked.get());
            } finally {
                consumer.cancel(true);
            }
        }
    }

    @PolicyException(
            rule = "PMD.CyclomaticComplexity",
            reason = "The consumer checks sequence, settlement, draining, and deadline within one paced loop.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CyclomaticComplexity")
    private static void consume(
            final Queue queue, final long rate, final AtomicLong acked, final AtomicBoolean finish, final long deadline)
            throws InterruptedException {
        while (System.nanoTime() < deadline) {
            final long now;
            final boolean draining;
            long expected;
            now = System.nanoTime();
            draining = finish.get();
            final var chunk = queue.pull(BATCH, Duration.ofMillis(20));
            if (chunk.isEmpty()) {
                if (draining) return;
                continue;
            }
            expected = acked.get();
            for (final var message : chunk.messages()) {
                final long actual;
                final long sequence;
                actual = ByteBuffer.wrap(message.payload()).getLong();
                sequence = expected;
                expected++;
                if (actual != sequence) {
                    throw new IllegalStateException("Unexpected message sequence");
                }
            }
            final var outcomes = queue.settle(chunk.ackAll());
            if (!outcomes.allOk()
                    || outcomes.byMessage().size() != chunk.messages().size()) {
                throw new IllegalStateException("Incomplete ACK");
            }
            acked.set(expected);
            pace(now, chunk.messages().size(), rate);
        }
        throw new IllegalStateException("Consumer timed out");
    }

    private static void pace(final long started, final int size, final double rate) throws InterruptedException {
        final long remaining;
        remaining = (long) (size * (double) SECOND / rate) - (System.nanoTime() - started);
        if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
    }
}
