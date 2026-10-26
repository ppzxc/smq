package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 꺼내기 한 번에 옮기는 dead letter 개수의 상한({@link DeadLetters#MAX_MOVES_PER_PULL})을 저장소 수준에서 못 박는다.
 * 코어는 남았다는 표시가 있으면 바로 다시 꺼내므로, 코어만 통과해서는 상한 경계가 드러나지 않는다.
 */
@Order(1)
class DeadLetterMoveCapTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    @Test
    void MemoryStorage는_상한만큼만_옮기고_남았다고_알린다() {
        MemoryStorage s = new MemoryStorage();
        verifyCap(s);
    }

    @Test
    void SqliteStorage는_상한만큼만_옮기고_남았다고_알린다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            verifyCap(s);
        }
    }

    private static void verifyCap(Storage s) {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        // 정확히 상한만큼: 모두 옮기고 남았다고 하지 않는다.
        PullResult exact = pullAfterExhausting(s, "exact", cap);
        assertTrue(exact.messages().isEmpty());
        assertFalse(exact.deadLettersRemain());
        assertEquals(cap, s.counts("dead-exact", T0.plus(LEASE)).ready());
        assertEquals(0, s.counts("exact", T0.plus(LEASE)).ready());

        // 상한보다 하나 많이: 상한만큼 옮기고 남았다고 알린다. 다음 꺼내기가 나머지를 옮긴다.
        PullResult over = pullAfterExhausting(s, "over", cap + 1);
        assertTrue(over.messages().isEmpty());
        assertTrue(over.deadLettersRemain());
        assertEquals(cap, s.counts("dead-over", T0.plus(LEASE)).ready());
        PullResult next = s.pull("over", 1, LEASE, T0.plus(LEASE));
        assertTrue(next.messages().isEmpty());
        assertFalse(next.deadLettersRemain());
        assertEquals(cap + 1, s.counts("dead-over", T0.plus(LEASE)).ready());

        // 옮겨진 메시지는 새 메시지라 전달 횟수를 처음부터 센다(첫 꺼내기가 1).
        Message moved = s.pull("dead-over", 1, LEASE, T0.plus(LEASE)).messages().getFirst();
        assertEquals(1, moved.deliveryCount());
    }

    /** 최대 전달 횟수 1인 큐에 count개를 발행해 한 번씩 꺼내고 리스가 만료된 시각에 다시 꺼낸 결과를 돌려준다. */
    private static PullResult pullAfterExhausting(Storage s, String queue, int count) {
        s.declare("dead-" + queue, QueueOptions.defaults());
        s.declare(queue, QueueOptions.defaults().maxDeliveries(1, "dead-" + queue));
        List<NewMessage> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(new NewMessage(new byte[] {(byte) i}, java.util.Map.of()));
        }
        s.publish(queue, batch, T0);
        assertEquals(count, s.pull(queue, count, LEASE, T0).messages().size());
        return s.pull(queue, 1, LEASE, T0.plus(LEASE));
    }
}
