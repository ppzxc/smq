package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Durability;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 측정 한 번의 조건. smq는 한 JVM만 열 수 있으므로(ADR-0012) 측정은 항상 JVM 하나에서 돈다.
 * {@code depth}는 측정을 시작하기 전에 미리 채워 두는 메시지 수다. {@code durability}는 SQLite에만 있다.
 */
record RunSpec(
        Storage storage,
        Scenario scenario,
        Profile profile,
        Durability durability,
        int depth,
        QueueKind queueKind,
        long durationMs,
        long warmupMs) {

    enum QueueKind {
        DEFAULT,
        MAX_DELIVERIES,
        SINGLE_ACTIVE_CONSUMER,
        LEASED_HEAD
    }

    static final int DEEP = 1_000_000;

    static RunSpec s1(
            final Storage storage,
            final Profile profile,
            final Durability durability,
            final int depth,
            final long durationMs,
            final long warmupMs) {
        return new RunSpec(storage, Scenario.S1, profile, durability, depth, QueueKind.DEFAULT, durationMs, warmupMs);
    }

    static RunSpec s1(final QueueKind kind, final int depth, final long durationMs, final long warmupMs) {
        return new RunSpec(
                Storage.SQLITE, Scenario.S1, Profile.P1, Durability.PROCESS_CRASH, depth, kind, durationMs, warmupMs);
    }

    static RunSpec s2(final Storage storage, final Profile profile, final long durationMs, final long warmupMs) {
        return new RunSpec(
                storage, Scenario.S2, profile, defaultDurability(storage), 0, QueueKind.DEFAULT, durationMs, warmupMs);
    }

    static RunSpec s3(final Storage storage, final Profile profile, final long durationMs, final long warmupMs) {
        return new RunSpec(
                storage, Scenario.S3, profile, defaultDurability(storage), 0, QueueKind.DEFAULT, durationMs, warmupMs);
    }

    private static Durability defaultDurability(final Storage storage) {
        return storage == Storage.SQLITE ? Durability.POWER_LOSS : null;
    }

    /** 결과와 기준선에서 이 조건을 가리키는 키. 예: {@code sqlite/S1/P1/POWER_LOSS/1m}. */
    String key() {
        final String key;
        key = Keys.of(storage, scenario, profile, durability == null ? "none" : durability.name(), depth);
        return queueKind == QueueKind.DEFAULT ? key : key + "/" + queueKind;
    }

    String toJson() {
        final Map<String, Object> m;
        m = new LinkedHashMap<>();
        m.put("storage", storage.name());
        m.put("scenario", scenario.name());
        m.put("profile", profile.name());
        m.put("durability", durability == null ? null : durability.name());
        m.put("depth", depth);
        m.put("queueKind", queueKind.name());
        m.put("durationMs", durationMs);
        m.put("warmupMs", warmupMs);
        return Json.write(m);
    }

    /**
     * 자식 JVM에 넘기는 명령행 인자. Windows의 ProcessBuilder는 인자 안의 큰따옴표를 지우므로, JSON을 큰따옴표도 공백도 없는
     * Base64(URL 안전, 패딩 없음)로 감싼다.
     */
    String toArg() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(toJson().getBytes(StandardCharsets.UTF_8));
    }

    static RunSpec fromArg(final String arg) {
        return fromJson(new String(Base64.getUrlDecoder().decode(arg), StandardCharsets.UTF_8));
    }

    static RunSpec fromJson(final String json) {
        final Map<?, ?> m;
        final Object d;
        m = (Map<?, ?>) Json.parse(json);
        d = m.get("durability");
        return new RunSpec(
                Storage.valueOf((String) m.get("storage")),
                Scenario.valueOf((String) m.get("scenario")),
                Profile.valueOf((String) m.get("profile")),
                d == null ? null : Durability.valueOf((String) d),
                ((Double) m.get("depth")).intValue(),
                m.get("queueKind") == null ? QueueKind.DEFAULT : QueueKind.valueOf((String) m.get("queueKind")),
                ((Double) m.get("durationMs")).longValue(),
                ((Double) m.get("warmupMs")).longValue());
    }
}
