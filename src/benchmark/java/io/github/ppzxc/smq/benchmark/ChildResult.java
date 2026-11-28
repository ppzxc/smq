package io.github.ppzxc.smq.benchmark;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 자식 JVM 하나의 결과. 표준 출력에 JSON 한 줄로 낸다. {@code messages}는 처리량 시나리오에서 측정 구간에 끝낸 메시지 수이고,
 * {@code latenciesMicros}는 S3 컨슈머가 잰 지연 표본(마이크로초)이다.
 */
record ChildResult(String role, long messages, long elapsedNanos, long[] latenciesMicros) {

    String toJson() {
        final Map<String, Object> m;
        m = new LinkedHashMap<>();
        m.put("role", role);
        m.put("messages", messages);
        m.put("elapsedNanos", elapsedNanos);
        m.put("latenciesMicros", latenciesMicros);
        return Json.write(m);
    }

    static ChildResult fromJson(final String json) {
        final Map<?, ?> m;
        final List<?> l;
        final long[] lat;
        m = (Map<?, ?>) Json.parse(json);
        l = (List<?>) m.get("latenciesMicros");
        lat = new long[l.size()];
        for (int i = 0; i < lat.length; i++) lat[i] = ((Double) l.get(i)).longValue();
        return new ChildResult(
                (String) m.get("role"),
                ((Double) m.get("messages")).longValue(),
                ((Double) m.get("elapsedNanos")).longValue(),
                lat);
    }
}
