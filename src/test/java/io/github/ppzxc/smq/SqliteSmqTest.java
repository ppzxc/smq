package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SqliteSmqTest extends SmqConformanceTest {

    @Override
    Smq.Builder builder(Path dir) {
        return Smq.sqlite(dir.resolve("smq.db"));
    }

    @Test
    void 다시_열어도_선언한_옵션과_참조가_남아_있고_시작할_때마다_안심하고_선언할_수_있다() {
        QueueOptions options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = builder(dir).open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = builder(dir).open()) {
            second.declare("dead", QueueOptions.defaults());
            assertEquals(options, second.declare("orders", options).options());
            assertEquals(options, second.queue("orders").options());
            assertThrows(QueueDeclarationConflictException.class,
                    () -> second.declare("orders", QueueOptions.defaults()));
            assertThrows(QueueInUseException.class, () -> second.delete("dead"));
        }
    }

    @Test
    void 닫아도_정산하지_않은_리스는_남고_다시_연_뒤_만료되면_재전달된다() throws Exception {
        MessageId id;
        Chunk lost;
        try (Smq first = builder(dir).clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            id = q.publish(msg("a"));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10));
        }
        try (Smq second = builder(dir).clock(clock).open()) {
            Queue q = second.queue("q");
            assertEquals(new QueueCounts(0, 1, 0), q.counts());
            assertTrue(q.pull(1, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(10));
            assertEquals(new QueueCounts(1, 0, 0), q.counts());
            Chunk again = q.pull(1, Duration.ZERO);
            assertEquals(id, again.messages().getFirst().id());
            assertEquals(2, again.messages().getFirst().deliveryCount());
            assertEquals(Outcome.STALE, q.settle(lost.ackAll()).byMessage().get(id));
            assertTrue(q.settle(again.ackAll()).allOk());
        }
    }

    @Test
    void 닫기_전에_꺼낸_리스는_다시_연_뒤_기한_전이면_그대로_정산된다() throws Exception {
        Chunk held;
        try (Smq first = builder(dir).clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = builder(dir).clock(clock).open()) {
            assertTrue(second.queue("q").settle(held.ackAll()).allOk());
        }
    }
}
