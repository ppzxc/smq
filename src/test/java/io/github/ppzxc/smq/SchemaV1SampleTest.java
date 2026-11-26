package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SchemaV1SampleTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    private static String text(Message m) {
        return new String(m.payload(), StandardCharsets.UTF_8);
    }

    private static List<String> texts(Chunk chunk) {
        return chunk.messages().stream().map(SchemaV1SampleTest::text).toList();
    }

    @Test
    void 견본을_열면_큐_선언과_상태별_개수가_보존되어_있다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            assertEquals(QueueOptions.defaults(), smq.queue("v1-dead").options());
            assertEquals(
                    QueueOptions.defaults().maxDeliveries(3, "v1-dead"),
                    smq.queue("v1-orders").options());
            assertEquals(
                    QueueOptions.defaults().singleActiveConsumer(true),
                    smq.queue("v1-serial").options());

            assertEquals(new QueueCounts(1, 0, 0), smq.queue("v1-dead").counts());
            assertEquals(new QueueCounts(2, 2, 1), smq.queue("v1-orders").counts());
            assertEquals(new QueueCounts(1, 1, 0), smq.queue("v1-serial").counts());
        }
    }

    @Test
    void 견본을_열면_메시지_id_전달_횟수_헤더가_보존되고_리스_기한에_재전달된다() throws Exception {
        Path copy = StorageFactory.copyV1Sample(dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement("""
                        UPDATE message SET published_at = published_at - 2, available_at = published_at - 1
                        WHERE id = ?
                        """)) {
            ps.setLong(1, IDS_M1);
            ps.executeUpdate();
        }

        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            Queue orders = smq.queue("v1-orders");

            Chunk ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1));
            assertEquals(List.of("m3", "m4"), texts(ready));
            assertEquals(
                    List.of(1, 1),
                    ready.messages().stream().map(Message::deliveryCount).toList());
            assertEquals(Map.of("kind", "ready"), ready.messages().getFirst().headers());
            assertEquals(
                    List.of(new MessageId(IDS_M3), new MessageId(IDS_M4)),
                    ready.messages().stream().map(Message::id).toList());
            assertTrue(orders.settle(ready.ackAll()).allOk());

            clock.advance(Duration.ofHours(1).minusSeconds(1));
            assertTrue(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1));
            Chunk redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10));
            assertEquals(List.of("m1", "m2"), texts(redelivered));
            assertEquals(
                    List.of(3, 2),
                    redelivered.messages().stream().map(Message::deliveryCount).toList());
            assertEquals(
                    Map.of("kind", "retried", "n", "1"),
                    redelivered.messages().getFirst().headers());
            assertEquals(
                    new MessageId(IDS_M1), redelivered.messages().getFirst().id());
            assertEquals(T0.minusNanos(2), redelivered.messages().getFirst().publishedAt());

            assertEquals(new QueueCounts(0, 2, 1), orders.counts());
            clock.advance(Duration.ofHours(1));

            assertEquals(new QueueCounts(0, 2, 1), orders.counts());
            Message delayed = orders.pull(10, Duration.ZERO).messages().getFirst();
            assertEquals("m5", text(delayed));
            assertEquals(1, delayed.deliveryCount());
        }
    }

    @Test
    void 견본의_dead_letter_큐와_단일_활성_컨슈머_큐가_그대로_동작한다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            Chunk dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            assertEquals(List.of("poison"), texts(dead));
            Map<String, String> h = dead.messages().getFirst().headers();
            assertEquals("poison", h.get("kind"));
            assertEquals("v1-orders", h.get("smq-original-queue"));
            assertEquals("rejected", h.get("smq-reason"));

            Queue serial = smq.queue("v1-serial");
            assertTrue(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1));
            assertEquals(List.of("s1", "s2"), texts(serial.pull(10, Duration.ZERO)));
            assertThrows(QueueInUseException.class, () -> smq.delete("v1-dead"));
        }
    }

    @Test
    void user_version을_높인_복사본을_열면_IncompatibleStorageException이다() throws Exception {
        Path copy = StorageFactory.copyV1Sample(dir.resolve("future.db"));
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA user_version = 99");
        }
        assertThrows(IncompatibleStorageException.class, () -> Smq.sqlite(copy).open());
    }

    static final long IDS_M1 = 3, IDS_M3 = 5, IDS_M4 = 6;
}
