package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.Durability;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

/**
 * 실행 하나의 매개변수. 시드 하나에서 정해진 순서로 뽑으므로 같은 시드는 같은 매개변수다. 범위는 이슈 #86의 표를 따른다(프로세스 수는
 * 이 하네스가 단일 프로세스라 1이다).
 *
 * @param maxMessageBytes 메시지 크기의 상한. 메시지마다 0부터 이 값까지 로그 균등으로 뽑는다
 * @param ackRatio 정산 결정 비율. 합이 1이고 ACK가 과반이다
 * @param slowChunkRatio 처리 시간이 0이 아닌 청크의 비율. 길이는 0부터 리스 기한의 1.5배까지 균등
 * @param extendRatio 처리 시간이 리스 기한의 절반을 넘는 청크가 리스를 연장할 확률
 * @param maxDeliveries 최대 전달 횟수. 없으면 0
 */
record RunParams(long seed, int processes, int maxMessageBytes, int publishBatch, int chunkSize, int producers,
        int consumers, Durability durability, int prefillDepth, double ackRatio, double nackRatio,
        double rejectRatio, double noneRatio, double slowChunkRatio, double extendRatio, long leaseMillis,
        double publishDelayRatio, long publishDelayMaxMillis, double nackDelayRatio, long nackDelayMaxMillis,
        int maxDeliveries, boolean singleActiveConsumer, long pullWaitMillis, long loadMillis) {

    static final int MAX_MESSAGE_BYTES = 64 * 1024;
    /** 미리 채우기가 만드는 데이터의 상한(바이트 추정). 큰 메시지로 수 GB를 만들지 않으려는 것이다. */
    private static final long PREFILL_BYTES_CAP = 256L * 1024 * 1024;

    static RunParams fromSeed(long seed, Limits limits) {
        SplittableRandom r = new SplittableRandom(seed);
        int maxBytes = logUniform(r, 0, MAX_MESSAGE_BYTES);
        int publishBatch = logUniform(r, 1, 500);
        int chunkSize = logUniform(r, 1, 500);
        int producers = 1 + r.nextInt(4);
        int consumers = 1 + r.nextInt(4);
        Durability durability = r.nextBoolean() ? Durability.POWER_LOSS : Durability.PROCESS_CRASH;
        int depth = r.nextBoolean() ? 0 : logUniform(r, 1_000, 100_000);
        long avgBytes = maxBytes / 4 + 64;
        depth = (int) Math.min(Math.min(depth, limits.maxDepth()), PREFILL_BYTES_CAP / avgBytes);

        double ack = 0.55 + 0.40 * r.nextDouble();
        double wn = r.nextDouble() + 0.01;
        double wr = r.nextDouble() + 0.01;
        double wz = r.nextDouble() + 0.01;
        double rest = (1 - ack) / (wn + wr + wz);
        long lease = Math.min(logUniform(r, 200, 10_000), limits.maxLeaseMillis());
        double slow = optionalRatio(r);
        double extend = r.nextDouble();
        double publishDelay = optionalRatio(r);
        long publishDelayMax = r.nextLong(2_001);
        double nackDelay = optionalRatio(r);
        long nackDelayMax = r.nextLong(2_001);
        int maxDeliveries = r.nextBoolean() ? 0 : 1 + r.nextInt(5);
        boolean single = r.nextDouble() < 0.2;
        long pullWait = r.nextLong(501);
        return new RunParams(seed, 1, maxBytes, publishBatch, chunkSize, producers, consumers, durability, depth,
                ack, wn * rest, wr * rest, wz * rest, slow, extend, lease, publishDelay, publishDelayMax, nackDelay,
                nackDelayMax, maxDeliveries, single, pullWait, limits.loadMillis());
    }

    /** 30%는 0, 나머지는 0~30% 균등. 지연이나 느린 처리가 아예 없는 실행도 탐색하려는 것이다. */
    private static double optionalRatio(SplittableRandom r) {
        double u = r.nextDouble();
        double v = r.nextDouble();
        return u < 0.3 ? 0 : 0.3 * v;
    }

    /** [lo, hi]에서 로그 균등. lo가 0이면 0도 나오도록 1을 더해 뽑고 뺀다. */
    static int logUniform(SplittableRandom r, int lo, int hi) {
        double a = Math.log(lo + 1.0);
        double b = Math.log(hi + 1.0);
        int v = (int) Math.round(Math.exp(a + r.nextDouble() * (b - a))) - 1;
        return Math.max(lo, Math.min(hi, v));
    }

    Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seed", seed);
        m.put("processes", processes);
        m.put("maxMessageBytes", maxMessageBytes);
        m.put("publishBatch", publishBatch);
        m.put("chunkSize", chunkSize);
        m.put("producers", producers);
        m.put("consumers", consumers);
        m.put("durability", durability.name());
        m.put("prefillDepth", prefillDepth);
        m.put("ackRatio", round(ackRatio));
        m.put("nackRatio", round(nackRatio));
        m.put("rejectRatio", round(rejectRatio));
        m.put("noneRatio", round(noneRatio));
        m.put("slowChunkRatio", round(slowChunkRatio));
        m.put("extendRatio", round(extendRatio));
        m.put("leaseMillis", leaseMillis);
        m.put("publishDelayRatio", round(publishDelayRatio));
        m.put("publishDelayMaxMillis", publishDelayMaxMillis);
        m.put("nackDelayRatio", round(nackDelayRatio));
        m.put("nackDelayMaxMillis", nackDelayMaxMillis);
        m.put("maxDeliveries", maxDeliveries);
        m.put("singleActiveConsumer", singleActiveConsumer);
        m.put("pullWaitMillis", pullWaitMillis);
        m.put("loadMillis", loadMillis);
        return m;
    }

    private static double round(double d) {
        return Math.round(d * 10_000) / 10_000.0;
    }

    /** 실패 출력용으로 풀어 쓴 매개변수. 한 줄에 하나. */
    String describe() {
        return toMap().entrySet().stream().map(e -> "  " + e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining("\n"));
    }
}
