package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 자식 JVM 안에서 시나리오 하나를 도는 쪽. 공개 API({@code Smq}/{@code Queue})만 쓴다.
 *
 * <p>자식은 {@code startAtEpochMs}까지 같은 작업을 돌려 JIT를 데운 뒤(집계하지 않음), 그 시각부터
 * {@code durationMs} 동안 잰다.
 */
final class Workload {

    static final String QUEUE = "bench";
    /** S3 프로듀서가 발행 배치를 내는 간격. 큐가 쌓이지 않도록 느슨하게 둔다. */
    static final long S3_INTERVAL_MS = 50;
    /** S3 컨슈머가 측정 구간이 끝난 뒤 남은 메시지를 비우도록 더 기다리는 시간. */
    static final long S3_DRAIN_MS = 1_000;

    private static final int PREFILL_BATCH = 1_000;

    private Workload() {}

    static Smq open(final RunSpec spec, final Path db) {
        return switch (spec.storage()) {
            case SQLITE -> Smq.sqlite(db).durability(spec.durability()).open();
            case MEMORY -> Smq.inMemory().open();
        };
    }

    /** 큐를 선언하고 깊이만큼 미리 채운다. SQLite는 부모가, 인메모리는 자식이 부른다. */
    static void prepare(final Smq smq, final RunSpec spec) {
        final Queue q;
        final List<OutgoingMessage> batch;
        q = smq.declare(QUEUE, options(smq, spec));
        if (spec.depth() == 0) return;
        batch = batch(spec.profile(), PREFILL_BATCH);
        for (int done = 0; done < spec.depth(); done += PREFILL_BATCH) {
            final int n;
            n = Math.min(PREFILL_BATCH, spec.depth() - done);
            q.publish(n == PREFILL_BATCH ? batch : batch.subList(0, n));
        }
        if (spec.queueKind() == RunSpec.QueueKind.LEASED_HEAD) leaseHead(q, spec.depth());
    }

    private static QueueOptions options(final Smq smq, final RunSpec spec) {
        final QueueOptions options;
        options = QueueOptions.defaults();
        if (spec.queueKind() == RunSpec.QueueKind.MAX_DELIVERIES) {
            smq.declare("bench-dlq", QueueOptions.defaults());
            return options.maxDeliveries(5, "bench-dlq");
        }
        return spec.queueKind() == RunSpec.QueueKind.SINGLE_ACTIVE_CONSUMER
                ? options.singleActiveConsumer(true)
                : options;
    }

    private static void leaseHead(final Queue q, final int depth) {
        try {
            for (int done = 0; done < depth; done += 10_000)
                q.pull(Math.min(10_000, depth - done), Duration.ZERO, Duration.ofHours(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("리스 준비가 중단되었다", e);
        }
    }

    static ChildResult run(final Smq smq, final RunSpec spec, final Role role, final long startAtEpochMs)
            throws InterruptedException {
        final Queue q;
        q = smq.queue(QUEUE);
        return switch (spec.scenario()) {
            case S1 -> throughput(q, spec, role, startAtEpochMs, true);
            case S2 -> throughput(q, spec, role, startAtEpochMs, false);
            case S3 -> latency(q, spec, role, startAtEpochMs);
        };
    }

    private static List<OutgoingMessage> batch(final Profile profile, final int size) {
        final byte[] payload;
        final List<OutgoingMessage> batch;
        payload = new byte[profile.payloadBytes()];
        batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) batch.add(OutgoingMessage.of(payload));
        return List.copyOf(batch);
    }

    private static ChildResult throughput(
            final Queue q, final RunSpec spec, final Role role, final long startAtEpochMs, final boolean cycle)
            throws InterruptedException {
        final List<OutgoingMessage> batch;
        final long t0;
        final long end;
        long messages;
        batch = batch(spec.profile(), spec.profile().chunk());
        while (System.currentTimeMillis() < startAtEpochMs) cycle(q, batch, cycle);
        t0 = System.nanoTime();
        end = t0 + spec.durationMs() * 1_000_000L;
        messages = 0;
        while (System.nanoTime() < end) messages += cycle(q, batch, cycle);
        return new ChildResult(role.name(), messages, System.nanoTime() - t0, new long[0]);
    }

    /** 한 주기. S1은 발행, 꺼내기, 전체 ACK이고 S2는 발행만이다. 끝낸 메시지 수를 돌려준다. */
    private static int cycle(final Queue q, final List<OutgoingMessage> batch, final boolean full)
            throws InterruptedException {
        final Chunk chunk;
        q.publish(batch);
        if (!full) return batch.size();
        chunk = q.pull(batch.size(), Duration.ZERO);
        if (chunk.isEmpty()) return 0;
        q.settle(chunk.ackAll());
        return chunk.messages().size();
    }

    private static ChildResult latency(final Queue q, final RunSpec spec, final Role role, final long startAtEpochMs)
            throws InterruptedException {
        final long measureStart;
        final long measureEnd;
        final long consumeEnd;
        final AtomicReference<ChildResult> produced;
        final AtomicReference<ChildResult> consumed;
        final AtomicReference<Throwable> failure;
        final Thread producer;
        final Thread consumer;
        final ChildResult c;
        final ChildResult p;
        measureStart = startAtEpochMs * 1_000_000L;
        measureEnd = measureStart + spec.durationMs() * 1_000_000L;
        consumeEnd = measureEnd + S3_DRAIN_MS * 1_000_000L;
        produced = new AtomicReference<>();
        consumed = new AtomicReference<>();
        failure = new AtomicReference<>();
        producer = new Thread(() -> guard(failure, () -> produced.set(produce(q, spec, measureEnd))), "s3-producer");
        consumer = new Thread(
                () -> guard(failure, () -> consumed.set(consume(q, spec, role, measureStart, consumeEnd))),
                "s3-consumer");
        consumer.start();
        producer.start();
        producer.join();
        consumer.join();
        if (failure.get() != null) throw new IllegalStateException("S3 스레드 실패", failure.get());
        c = consumed.get();
        p = produced.get();
        return new ChildResult(
                role.name(), p.messages(), Math.max(p.elapsedNanos(), c.elapsedNanos()), c.latenciesMicros());
    }

    private interface Body {
        void run() throws Exception;
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker records any failure, including errors, for propagation to the controlling thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private static void guard(final AtomicReference<Throwable> failure, final Body body) {
        try {
            body.run();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    /** 간격마다 발행 배치를 내면서 각 메시지 payload 앞 8바이트에 발행 직전 시각(epoch ns)을 적는다. */
    private static ChildResult produce(final Queue q, final RunSpec spec, final long endEpochNanos)
            throws InterruptedException {
        final int size;
        final long t0;
        long sent;
        long next;
        size = spec.profile().chunk();
        t0 = System.nanoTime();
        sent = 0;
        next = System.nanoTime();
        while (epochNanos() < endEpochNanos) {
            final long stamp;
            final List<OutgoingMessage> batch;
            final long sleep;
            stamp = epochNanos();
            batch = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final ByteBuffer b;
                b = ByteBuffer.allocate(Math.max(8, spec.profile().payloadBytes()));
                b.putLong(stamp);
                batch.add(OutgoingMessage.of(b.array()));
            }
            q.publish(batch);
            sent += size;
            next += S3_INTERVAL_MS * 1_000_000L;
            sleep = next - System.nanoTime();
            if (sleep > 0) Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
        }
        return new ChildResult("PRODUCER", sent, System.nanoTime() - t0, new long[0]);
    }

    /** 꺼낸 직후 시각과 payload의 발행 시각 차이를 잰다. 측정 구간 앞에서 발행한 메시지는 버린다. */
    private static ChildResult consume(
            final Queue q, final RunSpec spec, final Role role, final long measureStart, final long endEpochNanos)
            throws InterruptedException {
        final long t0;
        final List<Long> samples;
        long received;
        final long[] lat;
        t0 = System.nanoTime();
        samples = new ArrayList<>();
        received = 0;
        while (epochNanos() < endEpochNanos) {
            final Chunk chunk;
            final long now;
            chunk = q.pull(spec.profile().chunk(), Duration.ofMillis(200));
            now = epochNanos();
            if (chunk.isEmpty()) continue;
            chunk.messages().forEach(m -> {
                final long stamp;
                stamp = ByteBuffer.wrap(m.payload()).getLong();
                if (stamp >= measureStart) samples.add(Math.max(0, (now - stamp) / 1000));
            });
            received += chunk.messages().size();
            q.settle(chunk.ackAll());
        }
        lat = samples.stream().mapToLong(Long::longValue).toArray();
        return new ChildResult(role.name(), received, System.nanoTime() - t0, lat);
    }

    static long epochNanos() {
        final Instant n;
        n = Instant.now();
        return n.getEpochSecond() * 1_000_000_000L + n.getNano();
    }
}
