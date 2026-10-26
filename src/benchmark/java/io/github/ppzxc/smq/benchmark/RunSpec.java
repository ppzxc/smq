package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 측정 한 번의 조건. {@code processes}는 별도 JVM 수이고, S3는 {@code topology}가 JVM 배치를 정한다.
 * {@code depth}는 측정을 시작하기 전에 미리 채워 두는 메시지 수다. {@code durability}는 SQLite에만 있다.
 */
record RunSpec(Storage storage, Scenario scenario, Profile profile, int processes, Topology topology,
        Durability durability, int depth, long durationMs, long warmupMs) {

    static final int DEEP = 1_000_000;

    static RunSpec s1(Storage storage, Profile profile, int processes, Durability durability, int depth,
            long durationMs, long warmupMs) {
        return new RunSpec(storage, Scenario.S1, profile, processes, Topology.NONE, durability, depth, durationMs,
                warmupMs);
    }

    static RunSpec s2(Storage storage, Profile profile, long durationMs, long warmupMs) {
        return new RunSpec(storage, Scenario.S2, profile, 1, Topology.NONE, defaultDurability(storage), 0,
                durationMs, warmupMs);
    }

    static RunSpec s3(Storage storage, Profile profile, Topology topology, long durationMs, long warmupMs) {
        int processes = topology == Topology.CROSS_PROCESS ? 2 : 1;
        return new RunSpec(storage, Scenario.S3, profile, processes, topology, defaultDurability(storage), 0,
                durationMs, warmupMs);
    }

    private static Durability defaultDurability(Storage storage) {
        return storage == Storage.SQLITE ? Durability.POWER_LOSS : null;
    }

    /** 결과와 기준선에서 이 조건을 가리키는 키. 예: {@code sqlite/S1/P1/proc4/POWER_LOSS/1m}. */
    String key() {
        return Keys.of(storage, scenario, profile, topology == Topology.NONE ? "proc" + processes : topology.id(),
                durability == null ? "none" : durability.name(), depth);
    }

    String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("storage", storage.name());
        m.put("scenario", scenario.name());
        m.put("profile", profile.name());
        m.put("processes", processes);
        m.put("topology", topology.name());
        m.put("durability", durability == null ? null : durability.name());
        m.put("depth", depth);
        m.put("durationMs", durationMs);
        m.put("warmupMs", warmupMs);
        return Json.write(m);
    }

    static RunSpec fromJson(String json) {
        Map<?, ?> m = (Map<?, ?>) Json.parse(json);
        Object d = m.get("durability");
        return new RunSpec(Storage.valueOf((String) m.get("storage")), Scenario.valueOf((String) m.get("scenario")),
                Profile.valueOf((String) m.get("profile")), ((Double) m.get("processes")).intValue(),
                Topology.valueOf((String) m.get("topology")), d == null ? null : Durability.valueOf((String) d),
                ((Double) m.get("depth")).intValue(), ((Double) m.get("durationMs")).longValue(),
                ((Double) m.get("warmupMs")).longValue());
    }
}
