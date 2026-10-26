package io.github.ppzxc.smq.benchmark;

import java.util.List;
import java.util.Map;

/** 같은 데이터 크기에서 청크 1(단건)과 묶음의 처리량을 견주는 표. 임계값 없이 보여 주기만 한다. */
final class ChunkTable {

    private record Pair(String size, Profile single, Profile batch) {}

    private record Row(String label, String scenario, String durability) {}

    private static final List<Pair> PAIRS = List.of(
            new Pair("256B", Profile.P0, Profile.P1),
            new Pair("2KB", Profile.P2S, Profile.P2),
            new Pair("4KB", Profile.P4S, Profile.P4),
            new Pair("6KB", Profile.P3S, Profile.P3));

    private static final List<Row> ROWS = List.of(
            new Row("S1 POWER_LOSS", "S1", "POWER_LOSS"),
            new Row("S1 PROCESS_CRASH", "S1", "PROCESS_CRASH"),
            new Row("S2 POWER_LOSS", "S2", "POWER_LOSS"));

    private ChunkTable() {}

    static String render(Map<String, Double> results) {
        StringBuilder sb = new StringBuilder("청크 1 대 묶음 (1프로세스, 빈 큐, msg/s)\n");
        sb.append(String.format("%-18s %-6s %10s %10s %8s%n", "조건", "크기", "청크 1", "묶음", "배수"));
        for (Row row : ROWS) {
            for (Pair p : PAIRS) {
                Double single = results.get(Keys.throughput(key(row, p.single())));
                Double batch = results.get(Keys.throughput(key(row, p.batch())));
                String ratio = single == null || batch == null || single <= 0 ? "-"
                        : String.format("%.1fx", batch / single);
                sb.append(String.format("%-18s %-6s %10s %10s %8s%n", row.label(), p.size(), fmt(single), fmt(batch),
                        ratio));
            }
        }
        return sb.toString();
    }

    private static String key(Row row, Profile profile) {
        return Storage.SQLITE.id() + "/" + row.scenario() + "/" + profile + "/proc1/" + row.durability() + "/empty";
    }

    private static String fmt(Double v) {
        return v == null ? "-" : String.format("%,.0f", v);
    }
}
