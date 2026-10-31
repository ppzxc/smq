package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.OutgoingMessage;
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
 * <p>모든 자식은 {@code startAtEpochMs}까지 같은 작업을 돌려 JIT를 데운 뒤(집계하지 않음), 그 시각부터
 * {@code durationMs} 동안 잰다. 그래서 프로세스 여러 개가 같은 구간을 겹쳐 잰다.
 */
final class Workload {

    static final String QUEUE = "bench";
    /** S3 프로듀서가 발행 배치를 내는 간격. 큐가 쌓이지 않도록 느슨하게 둔다. */
    static final long S3_INTERVAL_MS = 50;
    /** S3 컨슈머가 측정 구간이 끝난 뒤 남은 메시지를 비우도록 더 기다리는 시간. */
    static final long S3_DRAIN_MS = 1_000;

    private static final int PREFILL_BATCH = 1_000;

    private Workload() {}

    static Smq open(RunSpec spec, Path db) {
        return switch (spec.storage()) {
            case SQLITE -> Smq.sqlite(db).durability(spec.durability()).open();
            case MEMORY -> Smq.inMemory().open();
        };
    }

    /** 큐를 선언하고 깊이만큼 미리 채운다. SQLite는 부모가, 인메모리는 자식이 부른다. */
    static void prepare(Smq smq, RunSpec spec) {
        Queue q = smq.declare(QUEUE, QueueOptions.defaults());
        if (spec.depth() == 0) return;
        List<OutgoingMessage> batch = batch(spec.profile(), PREFILL_BATCH);
        for (int done = 0; done < spec.depth(); done += PREFILL_BATCH) {
            int n = Math.min(PREFILL_BATCH, spec.depth() - done);
            q.publish(n == PREFILL_BATCH ? batch : batch.subList(0, n));
        }
    }

    static ChildResult run(Smq smq, RunSpec spec, Role role, long startAtEpochMs) throws Exception {
        Queue q = smq.queue(QUEUE);
        return switch (spec.scenario()) {
            case S1 -> throughput(q, spec, role, startAtEpochMs, true);
            case S2 -> throughput(q, spec, role, startAtEpochMs, false);
            case S3 -> latency(q, spec, role, startAtEpochMs);
        };
    }

    private static List<OutgoingMessage> batch(Profile profile, int size) {
        byte[] payload = new byte[profile.payloadBytes()];
        List<OutgoingMessage> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) batch.add(OutgoingMessage.of(payload));
        return List.copyOf(batch);
    }

    private static ChildResult throughput(Queue q, RunSpec spec, Role role, long startAtEpochMs, boolean cycle)
            throws InterruptedException {
        List<OutgoingMessage> batch = batch(spec.profile(), spec.profile().chunk());
        while (System.currentTimeMillis() < startAtEpochMs) cycle(q, batch, cycle);
        long t0 = System.nanoTime();
        long end = t0 + spec.durationMs() * 1_000_000L;
        long messages = 0;
        while (System.nanoTime() < end) messages += cycle(q, batch, cycle);
        return new ChildResult(role.name(), messages, System.nanoTime() - t0, new long[0]);
    }

    /** 한 주기. S1은 발행, 꺼내기, 전체 ACK이고 S2는 발행만이다. 끝낸 메시지 수를 돌려준다. */
    private static int cycle(Queue q, List<OutgoingMessage> batch, boolean full) throws InterruptedException {
        q.publish(batch);
        if (!full) return batch.size();
        Chunk chunk = q.pull(batch.size(), Duration.ZERO);
        if (chunk.isEmpty()) return 0;
        q.settle(chunk.ackAll());
        return chunk.messages().size();
    }

    private static ChildResult latency(Queue q, RunSpec spec, Role role, long startAtEpochMs) throws Exception {
        long measureStart = startAtEpochMs * 1_000_000L;
        long measureEnd = measureStart + spec.durationMs() * 1_000_000L;
        long consumeEnd = measureEnd + S3_DRAIN_MS * 1_000_000L;
        AtomicReference<ChildResult> produced = new AtomicReference<>();
        AtomicReference<ChildResult> consumed = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = role == Role.CONSUMER
                ? null
                : new Thread(() -> guard(failure, () -> produced.set(produce(q, spec, measureEnd))), "s3-producer");
        Thread consumer = role == Role.PRODUCER
                ? null
                : new Thread(
                        () -> guard(failure, () -> consumed.set(consume(q, spec, role, measureStart, consumeEnd))),
                        "s3-consumer");
        if (consumer != null) consumer.start();
        if (producer != null) producer.start();
        if (producer != null) producer.join();
        if (consumer != null) consumer.join();
        if (failure.get() != null) throw new IllegalStateException("S3 스레드 실패", failure.get());
        ChildResult c = consumed.get();
        ChildResult p = produced.get();
        return new ChildResult(
                role.name(),
                p != null ? p.messages() : c.messages(),
                Math.max(p != null ? p.elapsedNanos() : 0, c != null ? c.elapsedNanos() : 0),
                c != null ? c.latenciesMicros() : new long[0]);
    }

    private interface Body {
        void run() throws Exception;
    }

    private static void guard(AtomicReference<Throwable> failure, Body body) {
        try {
            body.run();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    /** 간격마다 발행 배치를 내면서 각 메시지 payload 앞 8바이트에 발행 직전 시각(epoch ns)을 적는다. */
    private static ChildResult produce(Queue q, RunSpec spec, long endEpochNanos) throws InterruptedException {
        int size = spec.profile().chunk();
        long t0 = System.nanoTime();
        long sent = 0;
        long next = System.nanoTime();
        while (epochNanos() < endEpochNanos) {
            long stamp = epochNanos();
            List<OutgoingMessage> batch = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ByteBuffer b = ByteBuffer.allocate(Math.max(8, spec.profile().payloadBytes()));
                b.putLong(stamp);
                batch.add(OutgoingMessage.of(b.array()));
            }
            q.publish(batch);
            sent += size;
            next += S3_INTERVAL_MS * 1_000_000L;
            long sleep = next - System.nanoTime();
            if (sleep > 0) Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
        }
        return new ChildResult("PRODUCER", sent, System.nanoTime() - t0, new long[0]);
    }

    /** 꺼낸 직후 시각과 payload의 발행 시각 차이를 잰다. 측정 구간 앞에서 발행한 메시지는 버린다. */
    private static ChildResult consume(Queue q, RunSpec spec, Role role, long measureStart, long endEpochNanos)
            throws InterruptedException {
        long t0 = System.nanoTime();
        List<Long> samples = new ArrayList<>();
        long received = 0;
        while (epochNanos() < endEpochNanos) {
            Chunk chunk = q.pull(spec.profile().chunk(), Duration.ofMillis(200));
            long now = epochNanos();
            if (chunk.isEmpty()) continue;
            chunk.messages().forEach(m -> {
                long stamp = ByteBuffer.wrap(m.payload()).getLong();
                if (stamp >= measureStart) samples.add(Math.max(0, (now - stamp) / 1000));
            });
            received += chunk.messages().size();
            q.settle(chunk.ackAll());
        }
        long[] lat = samples.stream().mapToLong(Long::longValue).toArray();
        return new ChildResult(role.name(), received, System.nanoTime() - t0, lat);
    }

    static long epochNanos() {
        Instant n = Instant.now();
        return n.getEpochSecond() * 1_000_000_000L + n.getNano();
    }
}
