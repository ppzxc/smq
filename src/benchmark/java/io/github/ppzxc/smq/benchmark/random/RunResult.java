package io.github.ppzxc.smq.benchmark.random;

import java.util.List;

/**
 * 실행 하나의 결과.
 *
 * @param msgPerSec 부하 구간에서 ACK된 메시지 수를 구간 길이로 나눈 값
 * @param p50Ms 부하 구간에 발행해 지연 없이 처음 꺼낸 메시지의 발행에서 꺼내기까지 걸린 시간. 표본이 없으면 null
 * @param error {@link Verdict#ERROR}일 때 원인 한 줄(기존 필드)
 * @param errorDetail {@link Verdict#ERROR}일 때 발생 위치와 스택 트레이스 전체. 아니면 null
 * @param walMaxBytes 실행 중 측정한 {@code smq.db-wal} 크기의 최댓값(바이트). 한 번도 측정하지 못했으면 null
 * @param jvmMetrics 부모 JVM의 JVM 메트릭 요약. 표본이 없으면 null
 * @param soak 고정 속도 모드의 달성률과 백로그 보고. 고정 속도가 아니면 null
 * @param childMetrics 자식 JVM마다의 메트릭 요약(프로세스 번호 순). 단일 프로세스면 빈 목록
 */
record RunResult(
        RunParams params,
        Verdict verdict,
        double msgPerSec,
        double mbPerSec,
        Double p50Ms,
        Double p99Ms,
        List<Violation> violations,
        String error,
        Records records,
        ErrorDetail errorDetail,
        Long walMaxBytes,
        JvmMetricsSampler.Summary jvmMetrics,
        SoakJudge.Report soak,
        List<JvmMetricsSampler.Summary> childMetrics) {

    /** 검사기에 넘긴 기록. 실패한 실행에서 파일로 보존한다. */
    record Records(List<Published> published, List<Delivery> deliveries, List<CountsSample> samples) {
        static final Records EMPTY = new Records(List.of(), List.of(), List.of());
    }

    /** 기록만 비운 사본. 통과한 실행의 기록은 더 쓸 데가 없어서, 세션이 결과를 쌓아 둘 때 힙을 차지하지 않게 한다. */
    RunResult withoutRecords() {
        return new RunResult(
                params,
                verdict,
                msgPerSec,
                mbPerSec,
                p50Ms,
                p99Ms,
                violations,
                error,
                Records.EMPTY,
                errorDetail,
                walMaxBytes,
                jvmMetrics,
                soak,
                childMetrics);
    }

    boolean passed() {
        return verdict == Verdict.PASS;
    }

    /** 하네스 JVM에서 난 예외. */
    static RunResult error(RunParams params, Throwable t) {
        return error(params, ErrorDetail.of(ErrorDetail.HARNESS, t, null, null), String.valueOf(t), null);
    }

    static RunResult error(RunParams params, ErrorDetail detail, String summary, Long walMaxBytes) {
        return new RunResult(
                params,
                Verdict.ERROR,
                0,
                0,
                null,
                null,
                List.of(),
                summary,
                Records.EMPTY,
                detail,
                walMaxBytes,
                null,
                null,
                List.of());
    }
}
