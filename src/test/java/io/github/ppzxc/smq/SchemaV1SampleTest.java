package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 커밋된 {@code schema-v1.db} 견본(재생성 방법은 같은 폴더의 README)을 복사해 연다. 공개 API로만 보존을 확인하고,
 * 파일 조작은 {@code user_version}을 올린 복사본을 만들 때만 JDBC를 쓴다.
 */
class SchemaV1SampleTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    /** 견본 복사는 저장소 factory 한 곳에만 있다. */
    static Path copySample(Path target) {
        return StorageFactory.copyV1Sample(target);
    }

    private static String text(Message m) {
        return new String(m.payload(), StandardCharsets.UTF_8);
    }

    private static List<String> texts(Chunk chunk) {
        return chunk.messages().stream().map(SchemaV1SampleTest::text).toList();
    }

    @Test
    void 견본을_열면_큐_선언과_상태별_개수가_보존되어_있다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(copySample(dir.resolve("smq.db"))).clock(clock).open()) {
            assertEquals(QueueOptions.defaults(), smq.queue("v1-dead").options());
            assertEquals(QueueOptions.defaults().maxDeliveries(3, "v1-dead"), smq.queue("v1-orders").options());
            assertEquals(QueueOptions.defaults().singleActiveConsumer(true), smq.queue("v1-serial").options());

            assertEquals(new QueueCounts(1, 0, 0), smq.queue("v1-dead").counts());
            assertEquals(new QueueCounts(2, 2, 1), smq.queue("v1-orders").counts());
            assertEquals(new QueueCounts(1, 1, 0), smq.queue("v1-serial").counts());
        }
    }

    @Test
    void 견본을_열면_메시지_id_전달_횟수_헤더가_보존되고_리스_기한에_재전달된다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(copySample(dir.resolve("smq.db"))).clock(clock).open()) {
            Queue orders = smq.queue("v1-orders");

            // 준비 상태 메시지
            Chunk ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1));
            assertEquals(List.of("m3", "m4"), texts(ready));
            assertEquals(List.of(1, 1), ready.messages().stream().map(Message::deliveryCount).toList());
            assertEquals(Map.of("kind", "ready"), ready.messages().getFirst().headers());
            assertEquals(List.of(new MessageId(IDS_M3), new MessageId(IDS_M4)),
                    ready.messages().stream().map(Message::id).toList());
            assertTrue(orders.settle(ready.ackAll()).allOk());

            // 리스 기한 직전까지는 리스 중이고, 기한이 되면 다시 꺼낼 수 있다
            clock.advance(Duration.ofHours(1).minusSeconds(1));
            assertTrue(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1));
            Chunk redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10));
            assertEquals(List.of("m1", "m2"), texts(redelivered));
            assertEquals(List.of(3, 2), redelivered.messages().stream().map(Message::deliveryCount).toList());
            assertEquals(Map.of("kind", "retried", "n", "1"), redelivered.messages().getFirst().headers());
            assertEquals(new MessageId(IDS_M1), redelivered.messages().getFirst().id());

            // 지연됨 메시지는 지연 시간이 지나야 나온다
            assertEquals(new QueueCounts(0, 2, 1), orders.counts());
            clock.advance(Duration.ofHours(1));
            assertEquals(new QueueCounts(1, 2, 0), orders.counts());
            Message delayed = orders.pull(10, Duration.ZERO).messages().getFirst();
            assertEquals("m5", text(delayed));
            assertEquals(1, delayed.deliveryCount());
        }
    }

    @Test
    void 견본의_dead_letter_큐와_단일_활성_컨슈머_큐가_그대로_동작한다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(copySample(dir.resolve("smq.db"))).clock(clock).open()) {
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
        Path copy = copySample(dir.resolve("future.db"));
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy); Statement s = c.createStatement()) {
            s.execute("PRAGMA user_version = 99");
        }
        assertThrows(IncompatibleStorageException.class, () -> Smq.sqlite(copy).open());
    }

    // 견본을 만들 때 큐가 붙인 id(poison이 2)
    static final long IDS_M1 = 3, IDS_M3 = 5, IDS_M4 = 6;
}
