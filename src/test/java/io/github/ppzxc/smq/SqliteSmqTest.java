package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SqliteSmqTest extends SmqConformanceTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.SQLITE_NEW;
    }

    private final List<Smq> direct = new ArrayList<>();

    @AfterEach
    void closeDirect() {
        direct.forEach(Smq::close);
    }

    private Smq.SqliteBuilder builder() {
        return Smq.sqlite(dir.resolve("smq.db"));
    }

    private Smq openDirect(Duration lease) {
        Smq smq = builder().clock(clock).leaseDuration(lease).open();
        direct.add(smq);
        return smq;
    }

    @Test
    void 다시_열어도_선언한_옵션과_참조가_남아_있고_시작할_때마다_안심하고_선언할_수_있다() {
        QueueOptions options = QueueOptions.defaults().maxDeliveries(3, "dead").singleActiveConsumer(true);
        try (Smq first = builder().open()) {
            first.declare("dead", QueueOptions.defaults());
            first.declare("orders", options);
        }
        try (Smq second = builder().open()) {
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
        try (Smq first = builder().clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            id = q.publish(msg("a"));
            lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10));
        }
        try (Smq second = builder().clock(clock).open()) {
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
        try (Smq first = builder().clock(clock).open()) {
            Queue q = first.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));
            held = q.pull(1, Duration.ZERO);
        }
        try (Smq second = builder().clock(clock).open()) {
            assertTrue(second.queue("q").settle(held.ackAll()).allOk());
        }
    }

    @Test
    void 호출_순서와_상관없이_durability와_공용_설정을_체인할_수_있다() {
        try (Smq a = Smq.sqlite(dir.resolve("a.db")).clock(clock).durability(Durability.PROCESS_CRASH)
                .leaseDuration(Duration.ofSeconds(1)).open();
                Smq b = Smq.sqlite(dir.resolve("b.db")).durability(Durability.PROCESS_CRASH).clock(clock)
                        .leaseDuration(Duration.ofSeconds(1)).open()) {
            assertNotNull(a.declare("q", QueueOptions.defaults()));
            assertNotNull(b.declare("q", QueueOptions.defaults()));
        }
    }

    @Test
    void 두_내구성_수준_모두_발행과_꺼내기가_동작한다() throws Exception {
        for (Durability d : Durability.values()) {
            try (Smq smq = Smq.sqlite(dir.resolve(d + ".db")).durability(d).open()) {
                Queue q = smq.declare("q", QueueOptions.defaults());
                q.publish(msg("a"));
                assertEquals(1, q.pull(1, Duration.ZERO).messages().size());
            }
        }
    }

    @Test
    void 같은_파일을_연_두_인스턴스가_동시에_꺼내도_단일_활성_컨슈머_큐의_리스_중인_청크는_하나다() throws Exception {
        Smq first = openDirect(Duration.ofMinutes(10));
        Smq second = openDirect(Duration.ofMinutes(10));
        // 두 인스턴스는 연결과 락이 따로라, 검사와 리스 부여가 한 쓰기 트랜잭션이 아니면 둘 다 통과할 수 있다.
        for (int round = 0; round < 300; round++) {
            QueueOptions options = QueueOptions.defaults().singleActiveConsumer(true);
            Queue a = first.declare("sac" + round, options);
            Queue b = second.declare("sac" + round, options);
            a.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));

            var got = new java.util.concurrent.atomic.AtomicInteger();
            runConcurrently(List.of(
                    () -> got.addAndGet(a.pull(2, Duration.ZERO).isEmpty() ? 0 : 1),
                    () -> got.addAndGet(b.pull(2, Duration.ZERO).isEmpty() ? 0 : 1)));

            assertEquals(1, got.get(), "round " + round);
            assertEquals(new QueueCounts(2, 2, 0), a.counts(), "round " + round);
        }
    }

    @Test
    void 같은_파일을_연_두_인스턴스가_동시에_꺼내도_같은_메시지가_두_리스에_묶이지_않는다() throws Exception {
        Smq first = openDirect(Duration.ofMinutes(10));
        Smq second = openDirect(Duration.ofMinutes(10));
        for (int round = 0; round < 300; round++) {
            Queue a = first.declare("two" + round, QueueOptions.defaults());
            Queue b = second.declare("two" + round, QueueOptions.defaults());
            a.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));

            List<Chunk> chunks = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            runConcurrently(List.of(() -> chunks.add(a.pull(4, Duration.ZERO)), () -> chunks.add(b.pull(4, Duration.ZERO))));

            List<MessageId> delivered = chunks.stream().flatMap(c -> c.messages().stream()).map(Message::id).toList();
            assertEquals(4, delivered.size(), "round " + round + " " + delivered);
            assertEquals(4, java.util.Set.copyOf(delivered).size(), "round " + round + " " + delivered);
        }
    }
}
