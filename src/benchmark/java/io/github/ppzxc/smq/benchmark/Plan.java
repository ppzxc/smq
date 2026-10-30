package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 어떤 조건을 돌릴지. 스펙의 측정 조건 표에 4KB(P4)를 더한 것이다. */
final class Plan {

    static final long FULL_DURATION_MS = 10_000;
    static final long FULL_WARMUP_MS = 3_000;
    static final long SMOKE_DURATION_MS = 1_500;
    static final long SMOKE_WARMUP_MS = 300;
    static final int SMOKE_DEPTH = 5_000;

    private Plan() {}

    /**
     * 전체 측정. SQLite는 스펙 표 전체이고, 인메모리는 한 JVM에서 도는 조건(프로세스 1, 같은 프로세스)만이다. 인메모리는 JVM끼리
     * 공유되지 않으므로 4프로세스와 다른 프로세스 S3는 의미가 없다.
     */
    static List<RunSpec> full(Storage storage) {
        return build(storage, FULL_DURATION_MS, FULL_WARMUP_MS, RunSpec.DEEP);
    }

    /** 비율 검사(scaling, depth)를 반복 측정하는 기본 횟수. */
    static final int DEFAULT_REPEATS = 3;

    /**
     * 교차 순서 계획. 첫 바퀴는 전체 계획이고, 나머지 바퀴는 비율 조건(scaling과 depth의 분자와 분모가 되는 SQLite S1)만 돈다.
     * 같은 조건을 연속으로 재면 그 몇 분의 장비 부하가 모든 표본에 같이 실리기 때문이다. 비율 조건이 없으면 한 바퀴뿐이다.
     */
    static List<List<RunSpec>> rounds(List<RunSpec> plan, int repeats) {
        if (repeats < 1) throw new IllegalArgumentException("반복 횟수는 1 이상: " + repeats);
        List<List<RunSpec>> rounds = new ArrayList<>();
        rounds.add(plan);
        List<RunSpec> ratio = plan.stream().filter(Plan::isRatioCondition).toList();
        if (!ratio.isEmpty()) for (int i = 1; i < repeats; i++) rounds.add(ratio);
        return rounds;
    }

    /** scaling(P1~P4 빈 큐 1·4프로세스)과 depth(P1 100만 건 깊이와 빈 큐)의 분자 또는 분모인 SQLite S1 조건. */
    static boolean isRatioCondition(RunSpec s) {
        if (s.storage() != Storage.SQLITE || s.scenario() != Scenario.S1) return false;
        boolean scalingProfile = s.profile() == Profile.P1
                || s.profile() == Profile.P2
                || s.profile() == Profile.P3
                || s.profile() == Profile.P4;
        return scalingProfile && (s.depth() == 0 || (s.profile() == Profile.P1 && s.depth() == RunSpec.DEEP));
    }

    /** 짧은 스모크. 임계값 없이 모든 경로(다중 JVM, 다른 프로세스 S3, 미리 채운 깊이)를 한 번씩 지나가게 한다. */
    static List<RunSpec> smoke(Storage storage) {
        List<RunSpec> plan = new ArrayList<>();
        long d = SMOKE_DURATION_MS;
        long w = SMOKE_WARMUP_MS;
        if (storage == Storage.SQLITE) {
            plan.add(RunSpec.s1(storage, Profile.P1, 1, Durability.POWER_LOSS, 0, d, w));
            plan.add(RunSpec.s1(storage, Profile.P1, 2, Durability.PROCESS_CRASH, 0, d, w));
            plan.add(RunSpec.s1(storage, Profile.P1, 1, Durability.PROCESS_CRASH, SMOKE_DEPTH, d, w));
            plan.add(RunSpec.s2(storage, Profile.P1, d, w));
            plan.add(RunSpec.s3(storage, Profile.P1, Topology.SAME_PROCESS, d, w));
            plan.add(RunSpec.s3(storage, Profile.P1, Topology.CROSS_PROCESS, d, w));
        } else {
            plan.add(RunSpec.s1(storage, Profile.P1, 1, null, 0, d, w));
            plan.add(RunSpec.s1(storage, Profile.P1, 1, null, SMOKE_DEPTH, d, w));
            plan.add(RunSpec.s2(storage, Profile.P1, d, w));
            plan.add(RunSpec.s3(storage, Profile.P1, Topology.SAME_PROCESS, d, w));
        }
        return plan;
    }

    private static List<RunSpec> build(Storage storage, long d, long w, int deep) {
        boolean sqlite = storage == Storage.SQLITE;
        List<Durability> durabilities =
                sqlite ? List.of(Durability.POWER_LOSS, Durability.PROCESS_CRASH) : Collections.singletonList(null);
        List<Integer> processCounts = sqlite ? List.of(1, 4) : List.of(1);
        List<RunSpec> plan = new ArrayList<>();
        // S1: P0은 1프로세스, 빈 큐만. P1~P4는 프로세스 {1,4} x 내구성 수준.
        for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, Profile.P0, 1, dur, 0, d, w));
        for (Profile p : List.of(Profile.P1, Profile.P2, Profile.P3, Profile.P4))
            for (int n : processCounts)
                for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, p, n, dur, 0, d, w));
        // 단건(청크 1): 데이터 크기마다 S1 1프로세스 빈 큐(두 내구성)와 S2. P0은 위에서 S1을 이미 더했다.
        for (Profile p : List.of(Profile.P2S, Profile.P3S, Profile.P4S))
            for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, p, 1, dur, 0, d, w));
        // 100만 건 깊이는 P1에서만.
        for (int n : processCounts)
            for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, Profile.P1, n, dur, deep, d, w));
        // S2: P1~P4와 단건 프로파일(P0, P2S, P3S, P4S), 1프로세스, 빈 큐.
        for (Profile p : List.of(
                Profile.P1, Profile.P2, Profile.P3, Profile.P4, Profile.P0, Profile.P2S, Profile.P3S, Profile.P4S))
            plan.add(RunSpec.s2(storage, p, d, w));
        // S3: P1만.
        plan.add(RunSpec.s3(storage, Profile.P1, Topology.SAME_PROCESS, d, w));
        if (sqlite) plan.add(RunSpec.s3(storage, Profile.P1, Topology.CROSS_PROCESS, d, w));
        return plan;
    }
}
