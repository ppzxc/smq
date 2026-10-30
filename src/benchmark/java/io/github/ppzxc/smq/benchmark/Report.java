package io.github.ppzxc.smq.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 결과 파일이자 기준선 파일의 형식. 같은 형식이라 측정 결과를 그대로 기준선으로 커밋할 수 있다.
 *
 * <pre>{"environment":{"os":"..."},"entries":{"sqlite/S1/P1/proc1/POWER_LOSS/empty:msgPerSec":12345.6}}</pre>
 *
 * 반복 측정한 항목만 {@code "samples":{"<항목 id>":[바퀴별 값...]}}을 덧붙인다. 항목 값은 그 표본의 중앙값이다. 표본 필드가 없으면 단일 표본이다.
 *
 * 항목 id는 {@code <저장소>/<시나리오>/<프로파일>/<형태>/<내구성>/<깊이>:<지표>} 이다. 지표는 {@code msgPerSec}(높을수록 좋음),
 * {@code p50Ms}와 {@code p99Ms}(낮을수록 좋음)다.
 */
record Report(Map<String, String> environment, Map<String, Double> entries, Map<String, List<Double>> samples) {

    /** 표본 필드가 없는 보고서(기존 결과 파일과 기준선). */
    Report(Map<String, String> environment, Map<String, Double> entries) {
        this(environment, entries, Map.of());
    }

    /** 항목 키 → 표본. 표본 필드가 없는 항목은 대표값 하나가 단일 표본이다. */
    Map<String, List<Double>> sampleMap() {
        Map<String, List<Double>> out = new LinkedHashMap<>();
        entries.forEach((k, v) -> out.put(k, samples.getOrDefault(k, List.of(v))));
        return out;
    }

    String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("environment", environment);
        m.put("entries", entries);
        if (!samples.isEmpty()) m.put("samples", samples);
        return Json.write(m);
    }

    static Report fromJson(String json) {
        Map<?, ?> m = (Map<?, ?>) Json.parse(json);
        Map<String, String> env = new LinkedHashMap<>();
        ((Map<?, ?>) m.get("environment")).forEach((k, v) -> env.put((String) k, String.valueOf(v)));
        Map<String, Double> entries = new LinkedHashMap<>();
        ((Map<?, ?>) m.get("entries")).forEach((k, v) -> entries.put((String) k, (Double) v));
        Map<String, List<Double>> samples = new LinkedHashMap<>();
        if (m.get("samples") instanceof Map<?, ?> sm) {
            sm.forEach((k, v) -> {
                List<Double> l = new ArrayList<>();
                for (Object o : (List<?>) v) l.add((Double) o);
                samples.put((String) k, l);
            });
        }
        return new Report(env, entries, samples);
    }

    /** 사람이 읽기 좋게 항목을 줄마다 나눈 JSON. 기준선을 커밋할 때 diff가 읽히게 한다. */
    String toPrettyJson() {
        StringBuilder sb = new StringBuilder("{\n  \"environment\": {\n");
        appendMap(
                sb,
                environment.entrySet().stream()
                        .map(e -> Map.entry(e.getKey(), (Object) e.getValue()))
                        .toList());
        sb.append("  },\n  \"entries\": {\n");
        appendMap(
                sb,
                entries.entrySet().stream()
                        .map(e -> Map.entry(e.getKey(), (Object) e.getValue()))
                        .toList());
        if (samples.isEmpty()) {
            sb.append("  }\n}\n");
        } else {
            sb.append("  },\n  \"samples\": {\n");
            appendMap(
                    sb,
                    samples.entrySet().stream()
                            .map(e -> Map.entry(e.getKey(), (Object) e.getValue()))
                            .toList());
            sb.append("  }\n}\n");
        }
        return sb.toString();
    }

    private static void appendMap(StringBuilder sb, List<Map.Entry<String, Object>> items) {
        for (int i = 0; i < items.size(); i++) {
            sb.append("    ")
                    .append(Json.write(items.get(i).getKey()))
                    .append(": ")
                    .append(Json.write(items.get(i).getValue()))
                    .append(i + 1 < items.size() ? ",\n" : "\n");
        }
    }
}
