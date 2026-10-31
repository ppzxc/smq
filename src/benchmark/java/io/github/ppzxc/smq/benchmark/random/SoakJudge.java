package io.github.ppzxc.smq.benchmark.random;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 고정 속도 실행의 판정. 달성률은 기록만 하고(환경에 따라 목표를 못 따라갈 수 있다), 실패로 보는 것은 큐 깊이가 계속 늘어나는
 * 백로그 증가뿐이다. 표본 목록만 받는 순수 로직이라 시계도 파일도 읽지 않는다.
 */
final class SoakJudge {

    /** 큐 깊이의 기울기가 목표 속도의 이 비율을 넘고(그리고 아래 양도 넘어야) 백로그 증가로 본다. */
    static final double GROWTH_SLOPE_FRACTION = 0.05;
    /** 표본 구간 동안 늘어난 양이 목표 속도 기준 이 시간 분량보다 많아야 한다. */
    static final double GROWTH_SECONDS_OF_BACKLOG = 2.0;

    /** 깊이가 프로듀서가 쉬는 상한의 이 비율에 닿으면 백로그 증가로 본다. 상한에서 깊이가 멈추면 추세가 평평해 보이기 때문이다. */
    static final double SATURATION_FRACTION = 0.9;

    private static final int MIN_SAMPLES = 3;

    /**
     * @param targetRate 목표 초당 메시지 수
     * @param achievedRatio 부하 구간에 실제 발행한 수 / (목표 * 구간 길이)
     * @param ackRate 부하 구간에 ACK된 초당 메시지 수
     * @param backlogSlopePerSec 큐 깊이(ready+leased+delayed) 표본의 최소제곱 기울기(개/초). 표본이 3개 미만이면 0
     * @param maxDepth 부하 중 잰 큐 깊이 표본의 최댓값
     * @param backlogGrowing 백로그가 계속 늘었는가. 실행의 실패 기준이다
     */
    record Report(
            long targetRate,
            double achievedRatio,
            double ackRate,
            double backlogSlopePerSec,
            long maxDepth,
            boolean backlogGrowing) {

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("targetRate", targetRate);
            m.put("achievedRatio", achievedRatio);
            m.put("ackRate", ackRate);
            m.put("backlogSlopePerSec", backlogSlopePerSec);
            m.put("maxDepth", maxDepth);
            m.put("backlogGrowing", backlogGrowing);
            return m;
        }
    }

    private SoakJudge() {}

    /**
     * @param publishedInLoad 부하 구간에 발행한 메시지 수(미리 채운 몫은 뺀다)
     * @param ackedInLoad 부하 구간에 ACK된 메시지 수
     * @param depths 부하 중 같은 간격으로 잰 대상 큐 깊이
     * @param sampleIntervalMillis 표본 간격
     * @param saturationDepth 프로듀서가 쉬기 시작하는 큐 깊이. 소비가 못 따라가면 깊이가 여기서 멈춘다
     */
    static Report judge(
            long targetRate,
            double loadSeconds,
            long publishedInLoad,
            long ackedInLoad,
            List<Long> depths,
            long sampleIntervalMillis,
            long saturationDepth) {
        double achieved = publishedInLoad / (targetRate * loadSeconds);
        double ackRate = ackedInLoad / loadSeconds;
        long max = depths.stream().mapToLong(Long::longValue).max().orElse(0);
        double slope = depths.size() < MIN_SAMPLES ? 0 : slope(depths, sampleIntervalMillis / 1000.0);
        double span = (depths.size() - 1) * (sampleIntervalMillis / 1000.0);
        boolean trending = depths.size() >= MIN_SAMPLES
                && slope > GROWTH_SLOPE_FRACTION * targetRate
                && slope * span > GROWTH_SECONDS_OF_BACKLOG * targetRate;
        boolean saturated = max >= SATURATION_FRACTION * saturationDepth;
        boolean growing = trending || saturated;
        return new Report(targetRate, achieved, ackRate, slope, max, growing);
    }

    /** 최소제곱 기울기. x는 표본 번호 * 간격(초). */
    private static double slope(List<Long> ys, double intervalSeconds) {
        int n = ys.size();
        double meanX = (n - 1) / 2.0 * intervalSeconds;
        double meanY = ys.stream().mapToLong(Long::longValue).average().orElse(0);
        double num = 0;
        double den = 0;
        for (int i = 0; i < n; i++) {
            double dx = i * intervalSeconds - meanX;
            num += dx * (ys.get(i) - meanY);
            den += dx * dx;
        }
        return den == 0 ? 0 : num / den;
    }
}
