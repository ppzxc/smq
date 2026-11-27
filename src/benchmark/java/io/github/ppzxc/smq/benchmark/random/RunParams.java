package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.Durability;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

/**
 * 실행 하나의 매개변수. 시드 하나에서 정해진 순서로 뽑으므로 같은 시드는 같은 매개변수다. 범위는 이슈 #86의 표를 따른다.
 *
 * @param producers 프로듀서 스레드 수. 1~4이다
 * @param consumers 컨슈머 스레드 수. 같은 규칙이다
 * @param maxMessageBytes 메시지 크기의 상한. 메시지마다 0부터 이 값까지 로그 균등으로 뽑는다
 * @param ackRatio 정산 결정 비율. 합이 1이고 ACK가 과반이다
 * @param slowChunkRatio 처리 시간이 0이 아닌 청크의 비율. 길이는 0부터 리스 기한의 1.5배까지 균등
 * @param extendRatio 처리 시간이 리스 기한의 절반을 넘는 청크가 리스를 연장할 확률
 * @param maxDeliveries 최대 전달 횟수. 없으면 0
 * @param targetRate 프로듀서 전체의 목표 초당 메시지 수. 0이면 목표 없이 최대한 보낸다(무작위 모드)
 * @param minMessageBytes 메시지 크기의 하한. 0이면 0부터 로그 균등(무작위 모드), 양수면 하한부터 상한까지 균등(고정 속도 모드)
 * @param minAchieved 달성률 하한. 0이면 판정하지 않는다. 시드와 무관하게 {@link Limits}에서 그대로 온다
 * @param consumerPaced 깊은 큐 모드. 깊이가 미리 채운 깊이 이하면 컨슈머가 쉬어서, 소비가 유입과 같아지며 깊이가 유지된다
 */
record RunParams(
        long seed,
        int maxMessageBytes,
        int publishBatch,
        int chunkSize,
        int producers,
        int consumers,
        Durability durability,
        int prefillDepth,
        double ackRatio,
        double nackRatio,
        double rejectRatio,
        double noneRatio,
        double slowChunkRatio,
        double extendRatio,
        long leaseMillis,
        double publishDelayRatio,
        long publishDelayMaxMillis,
        double nackDelayRatio,
        long nackDelayMaxMillis,
        int maxDeliveries,
        boolean singleActiveConsumer,
        long pullWaitMillis,
        long loadMillis,
        long targetRate,
        int minMessageBytes,
        boolean consumerPaced,
        double minAchieved) {

    static final int MAX_MESSAGE_BYTES = 64 * 1024;
    /** 미리 채우기가 만드는 데이터의 상한(바이트 추정). 큰 메시지로 수 GB를 만들지 않으려는 것이다. */
    private static final long PREFILL_BYTES_CAP = 256L * 1024 * 1024;
    /**
     * 고정 속도 모드의 청크 크기 하한. 컨슈머 스레드 하나가 꺼내기와 정산(커밋 둘)을 초당 십수 번 하므로, 청크가 작으면 한 스레드가 500/s를
     * 받지 못한다. 단일 활성 큐도 쓰지 않는다: 정산하지 않은 메시지 하나가 큐 전체를 리스 만료까지 막아 초당 수십 건이 한계다.
     */
    static final int SOAK_MIN_CHUNK_SIZE = 100;
    /** 고정 속도 모드의 발행 배치는 스레드 몫의 (1 / 이 값)초 분량을 넘지 않게 한다. 배치가 너무 크면 발행이 덩어리로 몰린다. */
    static final long SOAK_BATCH_WINDOW_DIVISOR = 4;
    /**
     * 고정 속도 모드의 발행 배치 하한. 건당에 가까운 작은 배치는 커밋 횟수를 늘리고, POWER_LOSS는 커밋마다 fsync를 하므로 fsync 상한(ns0270 S1
     * P0 약 722 msg/s)이 목표 속도를 막는다. 그래서 운영에서 쓰는 수준으로 묶는다.
     */
    static final long SOAK_MIN_PUBLISH_BATCH = 20;

    static RunParams fromSeed(final long seed, final Limits limits) {
        final SplittableRandom r;
        int maxBytes;
        int publishBatch;
        int chunkSize;
        final int producers;
        final int consumers;
        final Durability drawn;
        final Durability durability;
        int depth;
        final long avgBytes;
        final double ack;
        final double wn;
        final double wr;
        final double wz;
        final double rest;
        final long lease;
        double slow;
        final double extend;
        double publishDelay;
        final long publishDelayMax;
        double nackDelay;
        final long nackDelayMax;
        final int maxDeliveries;
        boolean single;
        final long pullWait;
        final long rate;
        int minBytes;
        boolean consumerPaced;
        r = new SplittableRandom(seed);
        maxBytes = logUniform(r, 0, MAX_MESSAGE_BYTES);
        publishBatch = logUniform(r, 1, 500);
        chunkSize = logUniform(r, 1, 500);
        producers = 1 + r.nextInt(4);
        consumers = 1 + r.nextInt(4);
        // 고정해도 난수는 뽑아서, 나머지 매개변수가 같은 시드의 것과 같게 한다.
        drawn = r.nextBoolean() ? Durability.POWER_LOSS : Durability.PROCESS_CRASH;
        durability = limits.durability().orElse(drawn);
        depth = r.nextBoolean() ? 0 : logUniform(r, 1_000, 100_000);
        avgBytes = avgMessageBytes(0, maxBytes);
        depth = (int) Math.min(Math.min(depth, limits.maxDepth()), PREFILL_BYTES_CAP / avgBytes);

        ack = 0.55 + 0.40 * r.nextDouble();
        wn = r.nextDouble() + 0.01;
        wr = r.nextDouble() + 0.01;
        wz = r.nextDouble() + 0.01;
        rest = (1 - ack) / (wn + wr + wz);
        lease = Math.min(logUniform(r, 200, 10_000), limits.maxLeaseMillis());
        slow = optionalRatio(r);
        extend = r.nextDouble();
        publishDelay = optionalRatio(r);
        publishDelayMax = r.nextLong(2_001);
        nackDelay = optionalRatio(r);
        nackDelayMax = r.nextLong(2_001);
        maxDeliveries = r.nextBoolean() ? 0 : 1 + r.nextInt(5);
        single = r.nextDouble() < 0.2;
        pullWait = r.nextLong(501);
        rate = limits.ratePerSecond();
        minBytes = 0;
        consumerPaced = false;
        if (rate > 0) {
            // 고정 속도 모드: 목표 속도를 못 맞추는 원인이 매개변수의 우연이 되지 않게, 지연과 느린 처리를 빼고 작은 메시지로 맞춘다. 미리 채운 깊이는 상한까지 둬서, 비어 있지 않은 큐에서
            // 시작한다.
            final long perThread;
            final long lowest;
            final long highest;
            maxBytes = limits.soakMessageBytes() > 0 ? limits.soakMessageBytes() : Limits.DEFAULT_SOAK_MESSAGE_BYTES;
            minBytes = Math.max(1, maxBytes / 2); // 0은 '하한 없음'(무작위 모드) 표식이라 쓰지 않는다
            consumerPaced = limits.standingQueue();
            depth = (int) Math.min(limits.maxDepth(), PREFILL_BYTES_CAP / avgMessageBytes(minBytes, maxBytes));
            slow = 0;
            single = false;
            chunkSize = Math.max(chunkSize, SOAK_MIN_CHUNK_SIZE);
            publishDelay = 0;
            nackDelay = 0;
            perThread = perThreadRate(rate, producers);
            lowest = Math.min(SOAK_MIN_PUBLISH_BATCH, perThread);
            highest = Math.max(lowest, perThread / SOAK_BATCH_WINDOW_DIVISOR);
            publishBatch = (int) Math.max(lowest, Math.min(publishBatch, highest));
        }
        return new RunParams(
                seed,
                maxBytes,
                publishBatch,
                chunkSize,
                producers,
                consumers,
                durability,
                depth,
                ack,
                wn * rest,
                wr * rest,
                wz * rest,
                slow,
                extend,
                lease,
                publishDelay,
                publishDelayMax,
                nackDelay,
                nackDelayMax,
                maxDeliveries,
                single,
                pullWait,
                limits.loadMillis(),
                rate,
                minBytes,
                consumerPaced,
                limits.minAchieved());
    }

    /** 메시지 크기 범위에서 어림한 평균 크기(바이트). 하한이 없으면 로그 균등으로 보고 상한의 4분의 1로 어림한다. 미리 채우기와 쌓임 상한을 바이트로 환산할 때 쓴다. */
    static long avgMessageBytes(final int minMessageBytes, final int maxMessageBytes) {
        return (minMessageBytes > 0 ? ((long) minMessageBytes + maxMessageBytes) / 2 : maxMessageBytes / 4) + 64;
    }

    /** 고정 속도 모드인가. */
    boolean isSoak() {
        return targetRate > 0;
    }

    /**
     * 프로듀서 스레드 하나가 맡는 초당 메시지 수. 스레드 수로 올림해 나눠서, 스레드가 보내는 합이 목표보다 모자라지 않게 한다(넘는 양은
     * 스레드 수 미만이다).
     */
    long perThreadRate() {
        return perThreadRate(targetRate, producers);
    }

    private static long perThreadRate(final long rate, final int producers) {
        return Math.max(1, (rate + producers - 1) / producers);
    }

    /** 30%는 0, 나머지는 0~30% 균등. 지연이나 느린 처리가 아예 없는 실행도 탐색하려는 것이다. */
    private static double optionalRatio(final SplittableRandom r) {
        final double u;
        final double v;
        u = r.nextDouble();
        v = r.nextDouble();
        return u < 0.3 ? 0 : 0.3 * v;
    }

    /** [lo, hi]에서 로그 균등. lo가 0이면 0도 나오도록 1을 더해 뽑고 뺀다. */
    static int logUniform(final SplittableRandom r, final int lo, final int hi) {
        final double a;
        final double b;
        final int v;
        a = Math.log(lo + 1.0);
        b = Math.log(hi + 1.0);
        v = (int) Math.round(Math.exp(a + r.nextDouble() * (b - a))) - 1;
        return Math.max(lo, Math.min(hi, v));
    }

    Map<String, Object> toMap() {
        return fields(d -> Math.round(d * 10_000) / 10_000.0);
    }

    private Map<String, Object> fields(final DoubleUnaryOperator round) {
        final Map<String, Object> m;
        m = new LinkedHashMap<>();
        m.put("seed", seed);
        m.put("maxMessageBytes", maxMessageBytes);
        m.put("publishBatch", publishBatch);
        m.put("chunkSize", chunkSize);
        m.put("producers", producers);
        m.put("consumers", consumers);
        m.put("durability", durability.name());
        m.put("prefillDepth", prefillDepth);
        m.put("ackRatio", round.applyAsDouble(ackRatio));
        m.put("nackRatio", round.applyAsDouble(nackRatio));
        m.put("rejectRatio", round.applyAsDouble(rejectRatio));
        m.put("noneRatio", round.applyAsDouble(noneRatio));
        m.put("slowChunkRatio", round.applyAsDouble(slowChunkRatio));
        m.put("extendRatio", round.applyAsDouble(extendRatio));
        m.put("leaseMillis", leaseMillis);
        m.put("publishDelayRatio", round.applyAsDouble(publishDelayRatio));
        m.put("publishDelayMaxMillis", publishDelayMaxMillis);
        m.put("nackDelayRatio", round.applyAsDouble(nackDelayRatio));
        m.put("nackDelayMaxMillis", nackDelayMaxMillis);
        m.put("maxDeliveries", maxDeliveries);
        m.put("singleActiveConsumer", singleActiveConsumer);
        m.put("pullWaitMillis", pullWaitMillis);
        m.put("loadMillis", loadMillis);
        m.put("targetRate", targetRate);
        m.put("minMessageBytes", minMessageBytes);
        m.put("consumerPaced", consumerPaced);
        return m;
    }

    /** 실패 출력용으로 풀어 쓴 매개변수. 한 줄에 하나. */
    String describe() {
        return toMap().entrySet().stream()
                .map(e -> "  " + e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining("\n"));
    }
}
