package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 어떤 조건을 돌릴지. 스펙의 측정 조건 표를 그대로 옮긴 것이다. */
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
        List<Durability> durabilities = sqlite ? List.of(Durability.POWER_LOSS, Durability.PROCESS_CRASH)
                : Collections.singletonList(null);
        List<Integer> processCounts = sqlite ? List.of(1, 4) : List.of(1);
        List<RunSpec> plan = new ArrayList<>();
        // S1: P0은 1프로세스, 빈 큐만. P1~P3은 프로세스 {1,4} x 내구성 수준.
        for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, Profile.P0, 1, dur, 0, d, w));
        for (Profile p : List.of(Profile.P1, Profile.P2, Profile.P3))
            for (int n : processCounts)
                for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, p, n, dur, 0, d, w));
        // 100만 건 깊이는 P1에서만.
        for (int n : processCounts)
            for (Durability dur : durabilities) plan.add(RunSpec.s1(storage, Profile.P1, n, dur, deep, d, w));
        // S2: P1~P3, 1프로세스, 빈 큐.
        for (Profile p : List.of(Profile.P1, Profile.P2, Profile.P3)) plan.add(RunSpec.s2(storage, p, d, w));
        // S3: P1만.
        plan.add(RunSpec.s3(storage, Profile.P1, Topology.SAME_PROCESS, d, w));
        if (sqlite) plan.add(RunSpec.s3(storage, Profile.P1, Topology.CROSS_PROCESS, d, w));
        return plan;
    }
}
