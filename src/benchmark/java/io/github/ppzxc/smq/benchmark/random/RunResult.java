package io.github.ppzxc.smq.benchmark.random;

import java.util.List;

/**
 * 실행 하나의 결과.
 *
 * @param msgPerSec 부하 구간에서 ACK된 메시지 수를 구간 길이로 나눈 값
 * @param p50Ms 부하 구간에 발행해 지연 없이 처음 꺼낸 메시지의 발행에서 꺼내기까지 걸린 시간. 표본이 없으면 null
 * @param error {@link Verdict#ERROR}일 때 원인
 */
record RunResult(RunParams params, Verdict verdict, double msgPerSec, double mbPerSec, Double p50Ms, Double p99Ms,
        List<Violation> violations, String error, Records records) {

    /** 검사기에 넘긴 기록. 실패한 실행에서 파일로 보존한다. */
    record Records(List<Published> published, List<Delivery> deliveries, List<CountsSample> samples) {
        static final Records EMPTY = new Records(List.of(), List.of(), List.of());
    }

    boolean passed() {
        return verdict == Verdict.PASS;
    }

    static RunResult error(RunParams params, Throwable t) {
        return new RunResult(params, Verdict.ERROR, 0, 0, null, null, List.of(), String.valueOf(t), Records.EMPTY);
    }
}
