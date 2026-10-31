package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.benchmark.Json;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

/**
 * 실행 하나의 매개변수. 시드 하나에서 정해진 순서로 뽑으므로 같은 시드는 같은 매개변수다. 범위는 이슈 #86의 표를 따른다.
 *
 * @param processes 프로세스 수(1~4). 1이면 이 JVM 하나에서 돈다. 1보다 크면 프로듀서와 컨슈머는 자식 JVM에 나뉘고 부모는 미리 채우기,
 *     {@code counts()} 표본, 소진, 판정만 맡는다
 * @param producers 프로세스마다의 프로듀서 스레드 수(길이가 {@code processes}). 프로세스당 0~4이고 전체는 1 이상이다
 * @param consumers 프로세스마다의 컨슈머 스레드 수. 같은 규칙이다
 * @param maxMessageBytes 메시지 크기의 상한. 메시지마다 0부터 이 값까지 로그 균등으로 뽑는다
 * @param ackRatio 정산 결정 비율. 합이 1이고 ACK가 과반이다
 * @param slowChunkRatio 처리 시간이 0이 아닌 청크의 비율. 길이는 0부터 리스 기한의 1.5배까지 균등
 * @param extendRatio 처리 시간이 리스 기한의 절반을 넘는 청크가 리스를 연장할 확률
 * @param maxDeliveries 최대 전달 횟수. 없으면 0
 * @param targetRate 프로듀서 전체의 목표 초당 메시지 수. 0이면 목표 없이 최대한 보낸다(무작위 모드)
 */
record RunParams(
        long seed,
        int processes,
        int maxMessageBytes,
        int publishBatch,
        int chunkSize,
        List<Integer> producers,
        List<Integer> consumers,
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
        long targetRate) {

    RunParams {
        producers = List.copyOf(producers);
        consumers = List.copyOf(consumers);
    }

    static final int MAX_MESSAGE_BYTES = 64 * 1024;
    /** 미리 채우기가 만드는 데이터의 상한(바이트 추정). 큰 메시지로 수 GB를 만들지 않으려는 것이다. */
    private static final long PREFILL_BYTES_CAP = 256L * 1024 * 1024;
    /** 고정 속도 모드의 메시지 크기 상한. 운영의 작은 메시지에 가깝게 맞춘다. */
    private static final int SOAK_MAX_MESSAGE_BYTES = 1024;
    /** 고정 속도 모드에서 프로듀서 스레드는 이 시간 분량씩 묶어 보낸다(배치가 너무 크면 발행이 덩어리로 몰린다). */
    private static final long SOAK_BATCH_WINDOW_DIVISOR = 10;

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
        // 프로세스 수는 맨 끝에서 뽑는다. 앞의 축은 프로세스 수를 넣기 전과 같은 값을 유지한다.
        int processes = 1 + r.nextInt(4);
        List<Integer> producerList = List.of(producers);
        List<Integer> consumerList = List.of(consumers);
        if (processes > 1) {
            producerList = perProcessThreads(r, processes);
            consumerList = perProcessThreads(r, processes);
        }
        long rate = limits.ratePerSecond();
        if (rate > 0) {
            // 고정 속도 모드: 목표 속도를 못 맞추는 원인이 매개변수의 우연이 되지 않게, 지연과 느린 처리를 빼고 작은 메시지로 맞춘다.
            maxBytes = Math.min(maxBytes, SOAK_MAX_MESSAGE_BYTES);
            depth = 0;
            slow = 0;
            publishDelay = 0;
            nackDelay = 0;
            int totalProducers =
                    producerList.stream().mapToInt(Integer::intValue).sum();
            long perThread = Math.max(1, rate / totalProducers);
            publishBatch = (int) Math.max(1, Math.min(publishBatch, perThread / SOAK_BATCH_WINDOW_DIVISOR));
        }
        return new RunParams(
                seed,
                processes,
                maxBytes,
                publishBatch,
                chunkSize,
                producerList,
                consumerList,
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
                rate);
    }

    /** 프로세스마다 0~4. 전체가 0이면 첫 프로세스에 1을 준다(전체에서 1 이상). */
    private static List<Integer> perProcessThreads(SplittableRandom r, int processes) {
        List<Integer> out = new ArrayList<>(processes);
        for (int i = 0; i < processes; i++) {
            out.add(r.nextInt(5));
        }
        if (out.stream().mapToInt(Integer::intValue).sum() == 0) {
            out.set(0, 1);
        }
        return out;
    }

    int totalProducers() {
        return producers.stream().mapToInt(Integer::intValue).sum();
    }

    int totalConsumers() {
        return consumers.stream().mapToInt(Integer::intValue).sum();
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
        return fields(d -> Math.round(d * 10_000) / 10_000.0);
    }

    /** 자식 JVM에 넘기는 인자. 비율을 반올림하지 않으므로 자식이 부모와 같은 워크로드를 만든다. */
    String toArg() {
        Map<String, Object> m = fields(DoubleUnaryOperator.identity());
        // JSON 수는 double로 읽혀 2^53을 넘는 시드가 깨진다. 시드는 문자열로 보낸다.
        m.put("seed", Long.toString(seed));
        String json = Json.write(m);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    static RunParams fromArg(String arg) {
        Map<?, ?> m = (Map<?, ?>) Json.parse(new String(Base64.getUrlDecoder().decode(arg), StandardCharsets.UTF_8));
        return new RunParams(
                Long.parseLong((String) m.get("seed")),
                i(m, "processes"),
                i(m, "maxMessageBytes"),
                i(m, "publishBatch"),
                i(m, "chunkSize"),
                ints(m, "producers"),
                ints(m, "consumers"),
                Durability.valueOf((String) m.get("durability")),
                i(m, "prefillDepth"),
                d(m, "ackRatio"),
                d(m, "nackRatio"),
                d(m, "rejectRatio"),
                d(m, "noneRatio"),
                d(m, "slowChunkRatio"),
                d(m, "extendRatio"),
                l(m, "leaseMillis"),
                d(m, "publishDelayRatio"),
                l(m, "publishDelayMaxMillis"),
                d(m, "nackDelayRatio"),
                l(m, "nackDelayMaxMillis"),
                i(m, "maxDeliveries"),
                (Boolean) m.get("singleActiveConsumer"),
                l(m, "pullWaitMillis"),
                l(m, "loadMillis"),
                l(m, "targetRate"));
    }

    private static double d(Map<?, ?> m, String k) {
        return ((Double) m.get(k));
    }

    private static long l(Map<?, ?> m, String k) {
        return ((Double) m.get(k)).longValue();
    }

    private static int i(Map<?, ?> m, String k) {
        return ((Double) m.get(k)).intValue();
    }

    private static List<Integer> ints(Map<?, ?> m, String k) {
        return ((List<?>) m.get(k)).stream().map(o -> ((Double) o).intValue()).toList();
    }

    private Map<String, Object> fields(DoubleUnaryOperator round) {
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
        return m;
    }

    /** 실패 출력용으로 풀어 쓴 매개변수. 한 줄에 하나. */
    String describe() {
        return toMap().entrySet().stream()
                .map(e -> "  " + e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining("\n"));
    }
}
