package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 자식 프로세스가 부하 구간이 끝나고 남기는 것. 프로세스마다 {@code p<n>-published.jsonl}, {@code p<n>-deliveries.jsonl},
 * {@code p<n>-result.json}을 실행 디렉터리에 쓰고, 부모가 읽어 모든 프로세스의 기록을 합친다. 실행이 실패하면 이 파일들이 실패
 * 디렉터리에 그대로 남는다.
 *
 * @param loadAckedMessages 부하 구간에 이 프로세스가 ACK한 메시지 수
 * @param loadAckedBytes 그 payload 바이트 수
 */
record ChildOutput(long loadAckedMessages, long loadAckedBytes, List<Published> published, List<Delivery> deliveries) {

    static void write(Path dir, int process, ChildOutput o) throws IOException {
        Files.write(
                file(dir, process, "published.jsonl"),
                o.published.stream().map(RecordsJson::line).toList());
        Files.write(
                file(dir, process, "deliveries.jsonl"),
                o.deliveries.stream().map(RecordsJson::line).toList());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("loadAckedMessages", o.loadAckedMessages);
        m.put("loadAckedBytes", o.loadAckedBytes);
        // 결과 파일을 마지막에 쓴다. 이 파일이 있으면 앞의 두 파일은 모두 쓰인 것이다.
        Files.writeString(file(dir, process, "result.json"), Json.write(m) + "\n");
    }

    static ChildOutput read(Path dir, int process) throws IOException {
        Map<?, ?> m = (Map<?, ?>) Json.parse(Files.readString(file(dir, process, "result.json")));
        List<Published> published = Files.readAllLines(file(dir, process, "published.jsonl")).stream()
                .map(RecordsJson::parsePublished)
                .toList();
        List<Delivery> deliveries = Files.readAllLines(file(dir, process, "deliveries.jsonl")).stream()
                .map(RecordsJson::parseDelivery)
                .toList();
        return new ChildOutput(
                ((Double) m.get("loadAckedMessages")).longValue(),
                ((Double) m.get("loadAckedBytes")).longValue(),
                published,
                deliveries);
    }

    private static Path file(Path dir, int process, String name) {
        return dir.resolve("p" + process + "-" + name);
    }
}
