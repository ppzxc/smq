package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 현재 스키마 버전의 견본을 만드는 수동 실행용 도구. 평소 테스트는 이것을 돌리지 않고 커밋된 파일만 연다.
 * 공개 API와 고정 시계만 쓰므로 어느 스키마 버전의 코드에서 돌려도 그 버전의 견본이 나온다.
 * 실행: {@code ./gradlew generateSchemaSample} (견본 폴더의 README 참고).
 *
 * <pre>
 * 시각 T0에서 고정한 상태
 *   v1-dead   (기본 옵션)                    준비 1: poison (v1-orders에서 reject됨)
 *   v1-orders (최대 3회, dead letter v1-dead)   준비 2: m3 m4 / 리스 중 2: m1(전달 2회) m2 / 지연됨 1: m5 (T0+2h)
 *   v1-serial (단일 활성 컨슈머)             리스 중 1: s1 / 준비 1: s2
 *   리스 기한은 모두 T0+1h
 * </pre>
 */
public final class SchemaSampleGenerator {

    private SchemaSampleGenerator() {}

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) throws Exception {
        Path out = Path.of(
                args.length > 0
                        ? args[0]
                        : "src/test/resources/schema-samples/schema-v" + SqliteStorage.SCHEMA_VERSION + ".db");
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.deleteIfExists(out);
        generate(out);
        for (String suffix : List.of("-wal", "-shm", "-journal")) {
            if (Files.exists(Path.of(out + suffix))) {
                throw new IllegalStateException("견본 옆에 " + suffix + " 파일이 남았다: " + out);
            }
        }
        System.out.println("wrote " + out.toAbsolutePath() + " (" + Files.size(out) + " bytes)");
    }

    static void generate(Path out) throws Exception {
        MutableClock clock = new MutableClock(SchemaV1SampleTest.T0);
        try (Smq smq =
                Smq.sqlite(out).clock(clock).leaseDuration(Duration.ofHours(1)).open()) {
            smq.declare("v1-dead", QueueOptions.defaults());
            Queue orders = smq.declare("v1-orders", QueueOptions.defaults().maxDeliveries(3, "v1-dead"));
            Queue serial = smq.declare("v1-serial", QueueOptions.defaults().singleActiveConsumer(true));

            orders.publish(msg("poison", Map.of("kind", "poison")));
            Chunk poison = orders.pull(1, Duration.ZERO);
            orders.settle(
                    poison.settle().reject(poison.messages().getFirst().id()).build());

            orders.publish(List.of(
                    msg("m1", Map.of("kind", "retried", "n", "1")),
                    msg("m2", Map.of("kind", "leased")),
                    msg("m3", Map.of("kind", "ready")),
                    msg("m4", Map.of("kind", "ready")),
                    msg("m5", Map.of("kind", "delayed")).withDelay(Duration.ofHours(2))));
            Chunk first = orders.pull(1, Duration.ZERO);
            orders.settle(first.settle().nack(first.messages().getFirst().id()).build());
            orders.pull(2, Duration.ZERO);

            serial.publish(List.of(msg("s1", Map.of()), msg("s2", Map.of())));
            serial.pull(1, Duration.ZERO);
        }
    }

    private static OutgoingMessage msg(String text, Map<String, String> headers) {
        return new OutgoingMessage(text.getBytes(StandardCharsets.UTF_8), headers);
    }
}
