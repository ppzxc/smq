package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Extension;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Settlement;
import io.github.ppzxc.smq.Smq;
import io.github.ppzxc.smq.benchmark.Aggregator;
import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 시드 하나의 실행: 미리 채우기, 부하 구간, 소진, 검증. 프로세스 수가 1이면 이 JVM의 스레드 여럿이 돈다. 1보다 크면 이 JVM(부모)은
 * 미리 채우기와 표본과 소진과 판정을 맡고, 프로듀서와 컨슈머는 자식 JVM이 같은 SQLite 파일에서 돈다. 공개 API({@code Smq}, {@code Queue})만 쓰고
 * 실제 시계로 돈다.
 *
 * <p>시각 규약: 발행과 정산은 호출 직전 벽시계를, 꺼내기는 호출이 돌아온 직후 벽시계를 기록한다. 검사기의 부등식이 모두 거짓 위반을 내지
 * 않는 쪽으로 기울도록 하는 것이다(발행·정산 시각은 실제보다 이르고, 꺼낸 시각은 실제보다 늦다).
 */
final class RandomRun {

    static final String TARGET = "random-target";
    static final String DEAD_LETTER = "random-dead-letter";

    private static final int PREFILL_BATCH = 1_000;
    private static final int DRAIN_CHUNK = 500;
    /** 부하 중 쌓여 있어도 되는 메시지 수와 바이트(미리 채운 몫은 따로 더한다). 이 선을 넘으면 프로듀서가 쉰다. */
    private static final long BACKLOG_MESSAGES = 50_000;

    private static final long BACKLOG_BYTES = 128L * 1024 * 1024;
    private static final long SAMPLE_INTERVAL_MS = 500;
    /** 소진 상한의 기본 몫. 리스 기한과 NACK 지연(최대 2초)은 따로 더한다. */
    private static final long DRAIN_BASE_MS = 30_000;
    /** 부모가 자식을 띄우고 자식이 SQLite를 열 때까지 주는 여유. 부하 구간은 이만큼 뒤에 시작한다. */
    private static final long CHILD_START_MARGIN_MS = 4_000;

    private static final long CHILD_KILL_GRACE_MS = 60_000;
    private static final long DEPTH_REFRESH_NANOS = 100_000_000L;

    interface Checker {
        List<Violation> check(
                InvariantChecker.Config config,
                List<Published> published,
                List<Delivery> deliveries,
                List<CountsSample> samples);
    }

    private final RunParams params;
    private final Script script;
    private final Checker checker;
    private final long drainLimitMillis;
    /** 이 JVM의 프로세스 번호. 단일 프로세스는 0, 다중 프로세스의 부모는 자식 번호(0..n-1) 다음인 n이다. */
    private final int process;

    private final ChildSpawner spawner;
    private volatile Path runDir;

    private final ConcurrentLinkedQueue<Published> published = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Delivery> deliveries = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<CountsSample> samples = new ConcurrentLinkedQueue<>();
    private final AtomicLong nextSeq = new AtomicLong();
    private final AtomicLong nextChunk = new AtomicLong();
    private final AtomicLong publishedMessages = new AtomicLong();
    private final AtomicLong publishedBytes = new AtomicLong();
    private final AtomicLong ackedMessages = new AtomicLong();
    private final AtomicLong ackedBytes = new AtomicLong();
    private final AtomicLong loadAckedMessages = new AtomicLong();
    private final AtomicLong loadAckedBytes = new AtomicLong();
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private long prefillMessages;
    private long prefillBytes;
    private volatile long loadStartMillis;
    private volatile long loadEndMillis;
    private volatile long depthAtNanos;
    private volatile long depth;
    private volatile Path walFile;
    private volatile long walMax = -1;

    RandomRun(RunParams params, Checker checker, long drainLimitMillis, int process, ChildSpawner spawner) {
        this.params = params;
        this.script = new Script(params);
        this.checker = checker;
        this.drainLimitMillis = drainLimitMillis;
        this.process = process;
        this.spawner = spawner;
    }

    private record Queues(Queue target, Queue deadLetter) {}

    static long drainLimit(RunParams p) {
        return DRAIN_BASE_MS + p.leaseMillis() + 2_000;
    }

    /** {@code dir}에 SQLite 파일을 만들고 돈다. 예외는 {@link Verdict#ERROR}로 돌려준다. */
    static RunResult execute(RunParams params, Path dir, Checker checker) {
        return execute(params, dir, checker, ChildSpawner.jvm());
    }

    static RunResult execute(RunParams params, Path dir, Checker checker, ChildSpawner spawner) {
        RandomRun run = new RandomRun(
                params, checker, drainLimit(params), params.processes() > 1 ? params.processes() : 0, spawner);
        try {
            return run.runIn(dir);
        } catch (Throwable t) {
            return run.errorResult(t);
        }
    }

    /** 예외를 발생 위치가 붙은 {@link Verdict#ERROR} 결과로 바꾼다. 자식 JVM의 실패는 그 자식이 남긴 보고를 우선한다. */
    private RunResult errorResult(Throwable t) {
        ErrorDetail detail;
        if (t instanceof ChildFailure f) {
            ErrorDetail reported = ChildReport.read(runDir, f.process()).orElse(null);
            String location = ErrorDetail.child(f.process());
            detail = reported != null
                    ? new ErrorDetail(
                            location,
                            reported.exceptionClass(),
                            reported.message(),
                            reported.stackTrace(),
                            f.exitCode(),
                            f.stderrTail())
                    : ErrorDetail.of(location, f.getCause() != null ? f.getCause() : f, f.exitCode(), f.stderrTail());
        } else {
            detail = ErrorDetail.of(ErrorDetail.HARNESS, t, null, null);
        }
        return RunResult.error(params, detail, String.valueOf(t), walMax < 0 ? null : walMax);
    }

    private RunResult runIn(Path dir) throws Exception {
        boolean drained;
        Path db = dir.resolve("smq.db");
        runDir = dir;
        walFile = dir.resolve("smq.db-wal");
        try (Smq smq = open(db)) {
            Queues q = declare(smq);
            prefill(q.target());
            if (params.processes() > 1) {
                runChildren(q, db, dir);
            } else {
                runLoad(
                        q,
                        System.currentTimeMillis(),
                        params.producers().getFirst(),
                        params.consumers().getFirst(),
                        true);
            }
            drained = drain(q.target(), q.deadLetter());
        }
        return judge(drained);
    }

    private Smq open(Path db) {
        return Smq.sqlite(db)
                .durability(params.durability())
                .leaseDuration(Duration.ofMillis(params.leaseMillis()))
                .open();
    }

    /** 대상 큐와 dead letter 큐를 선언한다. 여러 프로세스가 같은 옵션으로 불러도 같은 큐를 얻는다. */
    private Queues declare(Smq smq) {
        Queue dlq = smq.declare(DEAD_LETTER, QueueOptions.defaults());
        QueueOptions options = QueueOptions.defaults()
                .deadLetterQueue(DEAD_LETTER)
                .singleActiveConsumer(params.singleActiveConsumer());
        if (params.maxDeliveries() > 0) {
            options = options.maxDeliveries(params.maxDeliveries(), DEAD_LETTER);
        }
        return new Queues(smq.declare(TARGET, options), dlq);
    }

    /** 자식 프로세스를 띄워 부하 구간 동안 {@code counts()}만 표본하고, 끝나면 모든 자식의 기록을 합친다. */
    private void runChildren(Queues q, Path db, Path dir) throws Exception {
        long startAt = System.currentTimeMillis() + CHILD_START_MARGIN_MS;
        List<ChildSpawner.Running> running = new ArrayList<>();
        try {
            for (int i = 0; i < params.processes(); i++) {
                running.add(spawner.spawn(new ChildSpawner.Spec(params, i, db, dir, startAt)));
            }
            runLoad(q, startAt, 0, 0, true);
            long timeout = loadEndMillis + CHILD_KILL_GRACE_MS - System.currentTimeMillis();
            for (int i = 0; i < running.size(); i++) {
                awaitChild(running.get(i), i, timeout);
            }
        } finally {
            running.forEach(ChildSpawner.Running::destroy);
        }
        for (int i = 0; i < params.processes(); i++) {
            ChildOutput o = ChildOutput.read(dir, i);
            published.addAll(o.published());
            deliveries.addAll(o.deliveries());
            loadAckedMessages.addAndGet(o.loadAckedMessages());
            loadAckedBytes.addAndGet(o.loadAckedBytes());
        }
    }

    /** 자식 하나를 기다린다. 실패는 어느 자식인지 붙여 {@link ChildFailure}로 올린다. */
    private static void awaitChild(ChildSpawner.Running r, int process, long timeoutMs) throws Exception {
        try {
            r.await(timeoutMs);
        } catch (ChildFailure f) {
            throw f;
        } catch (Exception e) {
            throw new ChildFailure(process, null, "", e);
        }
    }

    /**
     * 다중 프로세스의 자식 쪽. 부모가 선언해 둔 큐에 붙어 {@code startAt}부터 부하 구간 동안 이 프로세스 몫의 스레드를 돌리고,
     * 끝나면 기록을 {@code outDir}에 쓴다.
     */
    static void runChild(RunParams params, int process, Path db, Path outDir, long startAt) throws Exception {
        ChildReport.guard(outDir, process, () -> {
            RandomRun run = new RandomRun(params, null, 0, process, null);
            try (Smq smq = run.open(db)) {
                run.runLoad(
                        run.declare(smq),
                        startAt,
                        params.producers().get(process),
                        params.consumers().get(process),
                        false);
            }
            ChildOutput.write(
                    outDir,
                    process,
                    new ChildOutput(
                            run.loadAckedMessages.get(),
                            run.loadAckedBytes.get(),
                            List.copyOf(run.published),
                            List.copyOf(run.deliveries)));
        });
    }

    // ---- 미리 채우기

    private void prefill(Queue target) {
        for (int done = 0; done < params.prefillDepth(); done += PREFILL_BATCH) {
            publishBatch(target, Math.min(PREFILL_BATCH, params.prefillDepth() - done));
        }
        prefillMessages = publishedMessages.get();
        prefillBytes = publishedBytes.get();
    }

    private void publishBatch(Queue target, int n) {
        long first = nextSeq.getAndAdd(n);
        List<OutgoingMessage> batch = new ArrayList<>(n);
        List<Published> records = new ArrayList<>(n);
        long bytes = 0;
        for (int i = 0; i < n; i++) {
            Tag tag = new Tag(params.seed(), process, first + i);
            Script.Planned p = script.message(tag);
            batch.add(p.message());
            records.add(new Published(tag, p.content(), 0, p.delayMillis()));
            bytes += p.message().payload().length;
        }
        // 시각은 위에서 만든 뒤 호출 직전에 다시 읽는다. 메시지를 만드는 시간이 지연 판정을 늦추지 않게 한다.
        long before = System.currentTimeMillis();
        target.publish(batch);
        for (Published p : records) {
            published.add(new Published(p.tag(), p.content(), before, p.delayMillis()));
        }
        publishedMessages.addAndGet(n);
        publishedBytes.addAndGet(bytes);
    }

    // ---- 부하 구간

    /**
     * {@code startAt}부터 {@code loadMillis} 동안 부하를 건다. 스레드는 {@code startAt}까지 기다렸다가 함께 시작한다.
     *
     * @param sampling 부하 중 {@code counts()}를 표본한다(단일 프로세스와 다중 프로세스의 부모)
     */
    private void runLoad(Queues q, long startAt, int producers, int consumers, boolean sampling)
            throws InterruptedException {
        Queue target = q.target();
        Queue dlq = q.deadLetter();
        loadStartMillis = startAt;
        loadEndMillis = startAt + params.loadMillis();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            threads.add(thread("producer-" + i, () -> producerLoop(target)));
        }
        for (int i = 0; i < consumers; i++) {
            threads.add(thread("consumer-" + i, () -> consumerLoop(target, dlq)));
        }
        if (!threads.isEmpty()) {
            for (long wait = startAt - System.currentTimeMillis();
                    wait > 0;
                    wait = startAt - System.currentTimeMillis()) {
                sleepMillis(Math.min(wait, 50));
            }
        }
        threads.forEach(Thread::start);
        try {
            while (System.currentTimeMillis() < loadEndMillis && failure.get() == null) {
                if (sampling) {
                    sample(target, dlq, false);
                }
                sleepMillis(Math.min(SAMPLE_INTERVAL_MS, Math.max(1, loadEndMillis - System.currentTimeMillis())));
            }
        } finally {
            stop.set(true);
            for (Thread t : threads) {
                t.join();
            }
        }
        rethrowFailure();
    }

    private Thread thread(String name, Body body) {
        return new Thread(
                () -> {
                    try {
                        body.run();
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                        stop.set(true);
                    }
                },
                name);
    }

    private interface Body {
        void run() throws Exception;
    }

    private void rethrowFailure() {
        Throwable t = failure.get();
        if (t != null) {
            throw new IllegalStateException("하네스 스레드가 실패했다", t);
        }
    }

    private void producerLoop(Queue target) {
        while (!stop.get()) {
            if (backlogFull(target)) {
                sleepMillis(5);
                continue;
            }
            publishBatch(target, params.publishBatch());
        }
    }

    /**
     * 쌓인 양이 선을 넘었는지. 단일 프로세스는 자기가 센 발행과 ACK의 차이로 보고, 다중 프로세스는 ACK가 다른 프로세스에서 일어나
     * 이 프로세스의 수로는 알 수 없으므로 큐의 {@code counts()}를 잠깐씩 캐시해 보며 메시지 수로만 제한한다.
     */
    private boolean backlogFull(Queue target) {
        if (params.processes() == 1) {
            return publishedMessages.get() - ackedMessages.get() >= prefillMessages + BACKLOG_MESSAGES
                    || publishedBytes.get() - ackedBytes.get() >= prefillBytes + BACKLOG_BYTES;
        }
        long now = System.nanoTime();
        if (now - depthAtNanos > DEPTH_REFRESH_NANOS) {
            QueueCounts c = target.counts();
            depth = c.ready() + c.leased() + c.delayed();
            depthAtNanos = now;
        }
        long avgBytes = params.maxMessageBytes() / 4 + 64;
        return depth >= params.prefillDepth() + Math.min(BACKLOG_MESSAGES, BACKLOG_BYTES / avgBytes);
    }

    private void consumerLoop(Queue target, Queue dlq) throws InterruptedException {
        for (long round = 0; !stop.get(); round++) {
            if (round % 4 == 3) {
                // 대상 큐만 보다가 dead letter 큐가 소진 때까지 쌓이지 않게 가끔 비운다.
                ackAll(dlq, QueueKind.DEAD_LETTER, params.chunkSize(), Duration.ZERO);
            } else {
                consumeTarget(target);
            }
        }
    }

    private void consumeTarget(Queue target) throws InterruptedException {
        Chunk chunk = target.pull(
                params.chunkSize(),
                Duration.ofMillis(params.pullWaitMillis()),
                Duration.ofMillis(params.leaseMillis()));
        long pulledAt = System.currentTimeMillis();
        if (chunk.isEmpty()) {
            return;
        }
        List<Message> messages = chunk.messages();
        List<Tag> tags = new ArrayList<>(messages.size());
        for (Message m : messages) {
            tags.add(tagOf(m));
        }
        long[] deadline = new long[messages.size()];
        Arrays.fill(deadline, chunk.leaseDeadline().toEpochMilli());

        Script.Processing p =
                script.processing(tags.getFirst(), messages.getFirst().deliveryCount());
        boolean finished = process(target, chunk, p, deadline);

        Settle[] settles = new Settle[messages.size()];
        Arrays.fill(settles, Settle.NONE);
        if (finished) {
            settleDecisions(target, chunk, tags, settles);
        }
        long chunkId = nextChunkId();
        recordChunk(QueueKind.TARGET, chunkId, messages, tags, pulledAt, deadline, settles);
    }

    /** 처리 시간만큼 기다린다. 길면 리스 기한의 절반마다 연장한다. 부하 구간이 끝나 멈추면 false(정산하지 않는다). */
    private boolean process(Queue target, Chunk chunk, Script.Processing p, long[] deadline)
            throws InterruptedException {
        long remaining = p.millis();
        long half = Math.max(1, params.leaseMillis() / 2);
        while (remaining > 0) {
            long step = p.extend() ? Math.min(remaining, half) : remaining;
            if (!sleepUnlessStopped(step)) {
                return false;
            }
            remaining -= step;
            if (p.extend() && remaining > 0) {
                Extension ext = target.extend(chunk, Duration.ofMillis(params.leaseMillis()));
                List<Message> ms = chunk.messages();
                for (int i = 0; i < ms.size(); i++) {
                    if (ext.outcomes().byMessage().get(ms.get(i).id()) == Outcome.OK) {
                        deadline[i] = ext.leaseDeadline().toEpochMilli();
                    }
                }
            }
        }
        return true;
    }

    private void settleDecisions(Queue target, Chunk chunk, List<Tag> tags, Settle[] settles) {
        List<Message> messages = chunk.messages();
        Settlement.Builder b = chunk.settle();
        Script.Decision[] decisions = new Script.Decision[messages.size()];
        boolean any = false;
        for (int i = 0; i < messages.size(); i++) {
            Message m = messages.get(i);
            Script.Decision d = script.settle(tags.get(i), m.deliveryCount());
            decisions[i] = d;
            switch (d.action()) {
                case ACK -> b.ack(m.id());
                case NACK -> {
                    if (d.nackDelayMillis() > 0) {
                        b.nack(m.id(), Duration.ofMillis(d.nackDelayMillis()));
                    } else {
                        b.nack(m.id());
                    }
                }
                case REJECT -> b.reject(m.id());
                case NONE -> {
                    continue;
                }
            }
            any = true;
        }
        if (!any) {
            return;
        }
        long at = System.currentTimeMillis();
        Outcomes outcomes = target.settle(b.build());
        for (int i = 0; i < messages.size(); i++) {
            Script.Decision d = decisions[i];
            if (d.action() == Action.NONE) {
                continue;
            }
            boolean ok = outcomes.byMessage().get(messages.get(i).id()) == Outcome.OK;
            settles[i] = new Settle(d.action(), ok, at, d.action() == Action.NACK ? d.nackDelayMillis() : 0);
            if (ok && d.action() == Action.ACK) {
                countAck(messages.get(i).payload().length);
            }
        }
    }

    /** 꺼낸 청크를 그대로 모두 ACK하고 기록한다. 부하 중 dead letter 큐와 소진 구간이 쓴다. 꺼냈으면 true. */
    private boolean ackAll(Queue q, QueueKind kind, int max, Duration wait) throws InterruptedException {
        Chunk chunk = q.pull(max, wait, Duration.ofMillis(params.leaseMillis()));
        long pulledAt = System.currentTimeMillis();
        if (chunk.isEmpty()) {
            return false;
        }
        List<Message> messages = chunk.messages();
        List<Tag> tags = new ArrayList<>(messages.size());
        for (Message m : messages) {
            tags.add(tagOf(m));
        }
        long[] deadline = new long[messages.size()];
        Arrays.fill(deadline, chunk.leaseDeadline().toEpochMilli());
        long at = System.currentTimeMillis();
        Outcomes outcomes = q.settle(chunk.ackAll());
        Settle[] settles = new Settle[messages.size()];
        for (int i = 0; i < settles.length; i++) {
            boolean ok = outcomes.byMessage().get(messages.get(i).id()) == Outcome.OK;
            settles[i] = new Settle(Action.ACK, ok, at, 0);
            if (ok) {
                countAck(messages.get(i).payload().length);
            }
        }
        recordChunk(kind, nextChunkId(), messages, tags, pulledAt, deadline, settles);
        return true;
    }

    /** 모든 프로세스에서 유일한 청크 번호. 위쪽 비트가 프로세스 번호다. */
    private long nextChunkId() {
        return ((long) process << 40) | nextChunk.incrementAndGet();
    }

    private void countAck(int bytes) {
        ackedMessages.incrementAndGet();
        ackedBytes.addAndGet(bytes);
        long now = System.currentTimeMillis();
        if (now >= loadStartMillis && now < loadEndMillis) {
            loadAckedMessages.incrementAndGet();
            loadAckedBytes.addAndGet(bytes);
        }
    }

    private void recordChunk(
            QueueKind kind,
            long chunkId,
            List<Message> messages,
            List<Tag> tags,
            long pulledAt,
            long[] deadline,
            Settle[] settles) {
        for (int i = 0; i < messages.size(); i++) {
            Message m = messages.get(i);
            Content content = new Content(Script.checksum(m.payload()), m.headers());
            deliveries.add(new Delivery(
                    kind,
                    tags.get(i),
                    m.id().value(),
                    m.deliveryCount(),
                    content,
                    new Pull(chunkId, i, pulledAt, deadline[i]),
                    settles[i]));
        }
    }

    /** 헤더의 태그. 읽을 수 없으면 어떤 발행 기록과도 맞지 않는 태그를 돌려줘 검사기가 유령으로 보고하게 한다. */
    static Tag tagOf(Message m) {
        try {
            return Tag.parse(m.headers().get(Script.TAG_HEADER));
        } catch (RuntimeException e) {
            return new Tag(0, -1, m.id().value());
        }
    }

    // ---- 소진

    private boolean drain(Queue target, Queue dlq) throws InterruptedException {
        long deadline = System.currentTimeMillis() + drainLimitMillis;
        boolean drained = false;
        while (System.currentTimeMillis() < deadline) {
            boolean got = ackAll(target, QueueKind.TARGET, DRAIN_CHUNK, Duration.ofMillis(20));
            got |= ackAll(dlq, QueueKind.DEAD_LETTER, DRAIN_CHUNK, Duration.ZERO);
            if (!got && isEmpty(target) && isEmpty(dlq)) {
                drained = true;
                break;
            }
        }
        sample(target, dlq, true);
        return drained;
    }

    private static boolean isEmpty(Queue q) {
        QueueCounts c = q.counts();
        return c.ready() == 0 && c.leased() == 0 && c.delayed() == 0;
    }

    private void sample(Queue target, Queue dlq, boolean afterDrain) {
        sampleWal();
        samples.add(sampleOf(QueueKind.TARGET, target.counts(), afterDrain));
        samples.add(sampleOf(QueueKind.DEAD_LETTER, dlq.counts(), afterDrain));
    }

    /** {@code smq.db-wal}의 지금 크기로 최댓값을 갱신한다. 파일이 아직 없거나 사라졌으면 건너뛴다. */
    private void sampleWal() {
        Path wal = walFile;
        if (wal == null) {
            return;
        }
        try {
            walMax = Math.max(walMax, Files.size(wal));
        } catch (java.io.IOException e) {
            // 아직 없거나 닫히며 지워졌다.
        }
    }

    private static CountsSample sampleOf(QueueKind kind, QueueCounts c, boolean afterDrain) {
        return new CountsSample(kind, afterDrain, c.ready(), c.leased(), c.delayed());
    }

    // ---- 검증과 수치

    private RunResult judge(boolean drained) {
        List<Published> pub = List.copyOf(published);
        List<Delivery> del = List.copyOf(deliveries);
        List<CountsSample> smp = List.copyOf(samples);
        InvariantChecker.Config config =
                new InvariantChecker.Config(params.singleActiveConsumer(), params.maxDeliveries());
        List<Violation> violations = checker.check(config, pub, del, smp);
        Verdict verdict = !drained ? Verdict.DRAIN_TIMEOUT : violations.isEmpty() ? Verdict.PASS : Verdict.VIOLATION;
        double seconds = params.loadMillis() / 1000.0;
        long[] latencies = latenciesMillis(pub, del);
        Double p50 = latencies.length == 0 ? null : (double) Aggregator.percentile(latencies, 50);
        Double p99 = latencies.length == 0 ? null : (double) Aggregator.percentile(latencies, 99);
        return new RunResult(
                params,
                verdict,
                loadAckedMessages.get() / seconds,
                loadAckedBytes.get() / 1_000_000.0 / seconds,
                p50,
                p99,
                violations,
                drained ? null : "소진이 " + drainLimitMillis + "ms 안에 끝나지 않았다",
                new RunResult.Records(pub, del, smp),
                null,
                walMax < 0 ? null : walMax);
    }

    /** 부하 구간에 발행해 지연 없이 처음 꺼낸 대상 큐 전달의 발행에서 꺼내기까지 걸린 시간(ms). 정렬해서 돌려준다. */
    private long[] latenciesMillis(List<Published> pub, List<Delivery> del) {
        Map<Tag, Published> byTag = new HashMap<>();
        pub.forEach(p -> byTag.put(p.tag(), p));
        List<Long> out = new ArrayList<>();
        for (Delivery d : del) {
            if (d.queue() != QueueKind.TARGET
                    || d.deliveryCount() != 1
                    || d.pull().atMillis() < loadStartMillis
                    || d.pull().atMillis() >= loadEndMillis) {
                continue;
            }
            Published p = byTag.get(d.tag());
            if (p != null && p.delayMillis() == 0 && p.atMillis() >= loadStartMillis) {
                out.add(Math.max(0, d.pull().atMillis() - p.atMillis()));
            }
        }
        return out.stream().mapToLong(Long::longValue).sorted().toArray();
    }

    // ---- 잠들기

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** 멈춤 신호를 조각마다 보며 잔다. 끝까지 잤으면 true. */
    private boolean sleepUnlessStopped(long millis) throws InterruptedException {
        long end = System.nanoTime() + millis * 1_000_000L;
        while (true) {
            if (stop.get()) {
                return false;
            }
            long left = end - System.nanoTime();
            if (left <= 0) {
                return true;
            }
            Thread.sleep(Math.min(20, Math.max(1, left / 1_000_000L)));
        }
    }
}
