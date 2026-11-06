package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 메모리 개수가 DB의 저장된 상태를 세어 얻은 값과 같은지 본다(ADR-0018). 시각 기준의 정확한 개수와는 다를 수 있다. */
class SqliteStorageCountersTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    private static NewMessage message(int i, Duration delay) {
        return new NewMessage(new byte[] {(byte) i}, Map.of(), delay);
    }

    private static void assertMatchesStored(SqliteStorage s, String queue) {
        assertEquals(ExactCounts.stored(s, queue), s.counts(queue), queue);
    }

    @Test
    void 무작위_연산_뒤에도_메모리_개수가_저장된_상태_집계와_같고_다시_열어도_같다() {
        Path db = dir.resolve("smq.db");
        Random random = new Random(20261106);
        List<QueueCounts> beforeClose = new ArrayList<>();
        try (SqliteStorage s = SqliteStorage.open(db, Durability.PROCESS_CRASH)) {
            s.declare("dead", QueueOptions.defaults());
            s.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
            Instant now = T0;
            List<PullResult> held = new ArrayList<>();
            for (int step = 0; step < 400; step++) {
                switch (random.nextInt(6)) {
                    case 0 -> {
                        List<NewMessage> batch = new ArrayList<>();
                        for (int i = 0; i <= random.nextInt(4); i++) {
                            batch.add(message(i, random.nextInt(3) == 0 ? Duration.ofSeconds(5) : Duration.ZERO));
                        }
                        s.publish("q", batch, now);
                    }
                    case 1 -> {
                        PullResult r = s.pull("q", 1 + random.nextInt(4), LEASE, now);
                        if (!r.messages().isEmpty()) {
                            held.add(r);
                        }
                    }
                    case 2, 3 -> {
                        if (!held.isEmpty()) {
                            PullResult r = held.remove(random.nextInt(held.size()));
                            List<Disposition> items = new ArrayList<>();
                            r.messages()
                                    .forEach(m -> items.add(
                                            switch (random.nextInt(4)) {
                                                case 0 -> new Disposition(m.id(), Disposition.Kind.ACK);
                                                case 1 -> new Disposition(m.id(), Disposition.Kind.NACK);
                                                case 2 ->
                                                    new Disposition(
                                                            m.id(), Disposition.Kind.NACK, Duration.ofSeconds(7));
                                                default -> new Disposition(m.id(), Disposition.Kind.REJECT);
                                            }));
                            s.settle(r.token(), items, now);
                        }
                    }
                    case 4 -> {
                        if (!held.isEmpty()) {
                            PullResult r = held.get(random.nextInt(held.size()));
                            Instant at = now;
                            s.extend(
                                    r.token(),
                                    r.messages().stream().map(m -> m.id()).toList(),
                                    LEASE,
                                    InstantSource.fixed(at));
                        }
                    }
                    default -> now = now.plusSeconds(1 + random.nextInt(12));
                }
                assertMatchesStored(s, "q");
                assertMatchesStored(s, "dead");
            }
            beforeClose.add(s.counts("q"));
            beforeClose.add(s.counts("dead"));
        }
        try (SqliteStorage s = SqliteStorage.open(db, Durability.PROCESS_CRASH)) {
            assertEquals(beforeClose.get(0), s.counts("q"));
            assertEquals(beforeClose.get(1), s.counts("dead"));
            assertMatchesStored(s, "q");
        }
    }

    @Test
    void 시각이_흐르지_않으면_시각_기준_집계와_같고_흐르면_저장된_상태를_따른다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            s.declare("q", QueueOptions.defaults());
            s.publish(
                    "q",
                    List.of(message(1, Duration.ZERO), message(2, Duration.ZERO), message(3, Duration.ofSeconds(5))),
                    T0);
            s.pull("q", 1, LEASE, T0);
            assertEquals(ExactCounts.at(s, "q", T0), s.counts("q"));

            Instant later = T0.plusSeconds(60);
            assertEquals(new QueueCounts(3, 0, 0), ExactCounts.at(s, "q", later));
            assertEquals(new QueueCounts(1, 1, 1), s.counts("q"));
        }
    }

    @Test
    void 없는_큐는_QueueNotFoundException이고_지운_큐도_그렇다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            assertThrows(QueueNotFoundException.class, () -> s.counts("none"));
            s.declare("q", QueueOptions.defaults());
            assertEquals(new QueueCounts(0, 0, 0), s.counts("q"));
            assertTrue(s.delete("q"));
            assertThrows(QueueNotFoundException.class, () -> s.counts("q"));
            s.declare("q", QueueOptions.defaults());
            assertEquals(new QueueCounts(0, 0, 0), s.counts("q"));
        }
    }
}
