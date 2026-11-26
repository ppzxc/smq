package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BuilderSmokeTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(7);

    @TempDir
    Path dir;

    @Test
    void 메모리_builder로_한_바퀴_돌고_시각과_리스_기간이_반영된다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.inMemory().clock(clock).leaseDuration(LEASE).open()) {
            roundTrip(smq, clock);
        }
    }

    @Test
    void SQLite_builder로_한_바퀴_돌고_시각과_리스_기간이_반영된다() throws Exception {
        MutableClock clock = new MutableClock(T0);
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db"))
                .clock(clock)
                .leaseDuration(LEASE)
                .open()) {
            roundTrip(smq, clock);
        }
    }

    @Test
    void 공개_builder_둘_다_양수가_아닌_리스_기간을_IllegalArgumentException으로_거부한다() {
        for (Duration bad : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1), null}) {
            assertThrows(IllegalArgumentException.class, () -> Smq.inMemory().leaseDuration(bad), "inMemory " + bad);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Smq.sqlite(dir.resolve("smq.db")).leaseDuration(bad),
                    "sqlite " + bad);
        }
    }

    private static void roundTrip(Smq smq, MutableClock clock) throws Exception {
        Queue q = smq.declare("q", QueueOptions.defaults());
        MessageId id = q.publish(OutgoingMessage.of("a".getBytes(StandardCharsets.UTF_8)));

        Chunk chunk = q.pull(1, Duration.ZERO);
        Message m = chunk.messages().getFirst();
        assertEquals(id, m.id());
        assertEquals(T0, m.publishedAt(), "주입한 시각이 발행 시각에 반영된다");
        assertEquals(T0.plus(LEASE), chunk.leaseDeadline(), "설정한 리스 기간이 기한에 반영된다");

        assertTrue(q.settle(chunk.ackAll()).allOk());
        assertEquals(new QueueCounts(0, 0, 0), q.counts());

        q.publish(OutgoingMessage.of("b".getBytes(StandardCharsets.UTF_8)));
        q.pull(1, Duration.ZERO);
        clock.advance(LEASE.minusNanos(1));
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofNanos(1));
        assertEquals(2, q.pull(1, Duration.ZERO).messages().getFirst().deliveryCount());
    }
}
