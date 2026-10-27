package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 저장소 인터페이스의 계약 스위트. 코어가 가리거나 흡수해 공개 API로는 보이지 않는 약속과 경계 사례만 다룬다.
 * 하위 클래스는 {@link #factory()}만 정한다. 시각은 각 연산의 {@code now}로 명시해 넘긴다.
 */
abstract class StorageContractTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration LEASE = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    Storage storage;

    abstract StorageFactory factory();

    private static NewMessage message(int b) {
        return new NewMessage(new byte[] {(byte) b}, Map.of());
    }

    @BeforeEach
    void openStorage() {
        storage = factory().open(dir);
    }

    @AfterEach
    void closeStorage() {
        storage.close();
    }

    @Test
    void 꺼내기가_정확히_상한만큼_옮기면_모두_옮기고_남았다고_하지_않는다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult exact = pullAfterExhausting("exact", cap);
        assertTrue(exact.messages().isEmpty());
        assertFalse(exact.deadLettersRemain());
        assertEquals(cap, storage.counts("dead-exact", T0.plus(LEASE)).ready());
        assertEquals(0, storage.counts("exact", T0.plus(LEASE)).ready());
    }

    @Test
    void 상한보다_하나_많으면_상한만큼_옮기고_남았다고_알린다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult over = pullAfterExhausting("over", cap + 1);
        assertTrue(over.messages().isEmpty());
        assertTrue(over.deadLettersRemain());
        assertEquals(cap, storage.counts("dead-over", T0.plus(LEASE)).ready());
    }

    @Test
    void 남았다는_표시_뒤의_다음_꺼내기가_나머지를_옮긴다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        pullAfterExhausting("over", cap + 1);
        PullResult next = storage.pull("over", 1, LEASE, T0.plus(LEASE));
        assertTrue(next.messages().isEmpty());
        assertFalse(next.deadLettersRemain());
        assertEquals(cap + 1, storage.counts("dead-over", T0.plus(LEASE)).ready());
    }

    @Test
    void 옮겨진_메시지는_새_메시지라_전달_횟수를_처음부터_센다() {
        pullAfterExhausting("over", DeadLetters.MAX_MOVES_PER_PULL + 1);
        Message moved = storage.pull("dead-over", 1, LEASE, T0.plus(LEASE)).messages().getFirst();
        assertEquals(1, moved.deliveryCount());
    }

    @Test
    void 발행_배치_중간에_실패하면_배치_전체가_반영되지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        var ok = message(1);
        var bad = new NewMessage(null, Map.of());
        assertThrows(RuntimeException.class, () -> storage.publish("q", List.of(ok, bad), T0));
        assertEquals(0, storage.counts("q", T0).ready());
    }

    @Test
    void 빈_꺼내기는_토큰과_기한이_비고_남았다는_표시가_거짓이다() {
        storage.declare("q", QueueOptions.defaults());
        PullResult empty = storage.pull("q", 1, LEASE, T0);
        assertTrue(empty.messages().isEmpty());
        assertNull(empty.token());
        assertNull(empty.leaseDeadline());
        assertFalse(empty.deadLettersRemain());
    }

    @Test
    void 꺼낼_때마다_새_리스_토큰이_나온다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1), message(2)), T0);
        LeaseToken first = storage.pull("q", 1, LEASE, T0).token();
        LeaseToken second = storage.pull("q", 1, LEASE, T0).token();
        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first, second);
    }

    @Test
    void 리스가_만료된_뒤_같은_큐에서_다시_꺼내도_새_리스_토큰이_나온다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), T0);
        LeaseToken first = storage.pull("q", 1, LEASE, T0).token();
        PullResult again = storage.pull("q", 1, LEASE, T0.plus(LEASE));
        assertEquals(1, again.messages().size());
        assertNotEquals(first, again.token());
    }

    @Test
    void 리스_기한_정확히에는_다시_꺼낼_수_있고_1ns_앞에는_꺼낼_수_없다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), T0);
        storage.pull("q", 1, LEASE, T0);
        Instant deadline = T0.plus(LEASE);
        assertTrue(storage.pull("q", 1, LEASE, deadline.minusNanos(1)).messages().isEmpty());
        assertEquals(1, storage.pull("q", 1, LEASE, deadline).messages().size());
    }

    @Test
    void 지연_시각_정확히에는_꺼낼_수_있고_1ns_앞에는_꺼낼_수_없다() {
        storage.declare("q", QueueOptions.defaults());
        Duration delay = Duration.ofSeconds(5);
        storage.publish("q", List.of(new NewMessage(new byte[] {1}, Map.of(), delay)), T0);
        Instant availableAt = T0.plus(delay);
        assertTrue(storage.pull("q", 1, LEASE, availableAt.minusNanos(1)).messages().isEmpty());
        assertEquals(1, storage.pull("q", 1, LEASE, availableAt).messages().size());
    }

    @Test
    void 메시지가_전부_낡은_연장도_기한은_now_더하기_d이고_메시지별_결과는_STALE이다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), T0);
        PullResult pulled = storage.pull("q", 1, LEASE, T0);
        MessageId id = pulled.messages().getFirst().id();
        storage.settle(pulled.token(), List.of(new Disposition(id, Disposition.Kind.ACK)), T0);

        Instant now = T0.plusSeconds(1);
        Duration d = Duration.ofSeconds(30);
        Extended extended = storage.extend(pulled.token(), List.of(id), d, now);

        assertEquals(now.plus(d), extended.leaseDeadline());
        assertEquals(List.of(new Result(id, Outcome.STALE)), extended.results());
    }

    @Test
    void 낡은_리스_토큰으로_정산하면_STALE이고_저장소_상태가_바뀌지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1)), T0);
        PullResult first = storage.pull("q", 1, LEASE, T0);
        MessageId id = first.messages().getFirst().id();
        Instant second = T0.plus(LEASE);
        PullResult current = storage.pull("q", 1, LEASE, second);
        assertEquals(1, current.messages().size());

        List<Result> results = storage.settle(first.token(), List.of(new Disposition(id, Disposition.Kind.ACK)), second);

        assertEquals(List.of(new Result(id, Outcome.STALE)), results);
        assertEquals(new QueueCounts(0, 1, 0), storage.counts("q", second));
        Instant expired = second.plus(LEASE);
        assertEquals(1, storage.pull("q", 1, LEASE, expired).messages().size());
    }

    @Test
    void 정산_안에_dead_letter_큐_없는_reject가_있으면_정산_전체가_반영되지_않는다() {
        storage.declare("q", QueueOptions.defaults());
        storage.publish("q", List.of(message(1), message(2)), T0);
        PullResult pulled = storage.pull("q", 2, LEASE, T0);
        MessageId acked = pulled.messages().get(0).id();
        MessageId rejected = pulled.messages().get(1).id();

        assertThrows(RuntimeException.class, () -> storage.settle(pulled.token(),
                List.of(new Disposition(acked, Disposition.Kind.ACK), new Disposition(rejected, Disposition.Kind.REJECT)),
                T0));

        assertEquals(new QueueCounts(0, 2, 0), storage.counts("q", T0));
        assertEquals(2, storage.pull("q", 2, LEASE, T0.plus(LEASE)).messages().size());
    }

    /** 최대 전달 횟수 1인 큐에 count개를 발행해 한 번씩 꺼내고 리스가 만료된 시각에 다시 꺼낸 결과를 돌려준다. */
    private PullResult pullAfterExhausting(String queue, int count) {
        storage.declare("dead-" + queue, QueueOptions.defaults());
        storage.declare(queue, QueueOptions.defaults().maxDeliveries(1, "dead-" + queue));
        List<NewMessage> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(message(i));
        }
        storage.publish(queue, batch, T0);
        assertEquals(count, storage.pull(queue, count, LEASE, T0).messages().size());
        return storage.pull(queue, 1, LEASE, T0.plus(LEASE));
    }
}
