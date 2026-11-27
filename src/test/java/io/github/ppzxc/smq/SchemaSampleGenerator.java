package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public final class SchemaSampleGenerator {

    private SchemaSampleGenerator() {}

    @PolicyException(
            rule = "PMD.SystemPrintln",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(final String[] args) throws Exception {
        final Path out;
        out = Path.of(
                args.length > 0
                        ? args[0]
                        : "src/test/resources/schema-samples/schema-v" + SqliteStorage.SCHEMA_VERSION + ".db");
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.deleteIfExists(out);
        generate(out);
        for (final String suffix : List.of("-wal", "-shm", "-journal")) {
            if (Files.exists(Path.of(out + suffix))) {
                throw new IllegalStateException("견본 옆에 " + suffix + " 파일이 남았다: " + out);
            }
        }
        System.out.println("wrote " + out.toAbsolutePath() + " (" + Files.size(out) + " bytes)");
    }

    static void generate(final Path out) throws Exception {
        final MutableClock clock;
        clock = new MutableClock(SchemaV1SampleTest.T0);
        try (Smq smq =
                Smq.sqlite(out).clock(clock).leaseDuration(Duration.ofHours(1)).open()) {
            final Queue orders;
            final Queue serial;
            final Chunk poison;
            final Chunk first;
            smq.declare("v1-dead", QueueOptions.defaults());
            orders = smq.declare("v1-orders", QueueOptions.defaults().maxDeliveries(3, "v1-dead"));
            serial = smq.declare("v1-serial", QueueOptions.defaults().singleActiveConsumer(true));

            orders.publish(msg("poison", Map.of("kind", "poison")));
            poison = orders.pull(1, Duration.ZERO);
            orders.settle(
                    poison.settle().reject(poison.messages().getFirst().id()).build());

            orders.publish(List.of(
                    msg("m1", Map.of("kind", "retried", "n", "1")),
                    msg("m2", Map.of("kind", "leased")),
                    msg("m3", Map.of("kind", "ready")),
                    msg("m4", Map.of("kind", "ready")),
                    msg("m5", Map.of("kind", "delayed")).withDelay(Duration.ofHours(2))));
            first = orders.pull(1, Duration.ZERO);
            orders.settle(first.settle().nack(first.messages().getFirst().id()).build());
            orders.pull(2, Duration.ZERO);

            serial.publish(List.of(msg("s1", Map.of()), msg("s2", Map.of())));
            serial.pull(1, Duration.ZERO);
        }
    }

    private static OutgoingMessage msg(final String text, final Map<String, String> headers) {
        return new OutgoingMessage(text.getBytes(StandardCharsets.UTF_8), headers);
    }
}
