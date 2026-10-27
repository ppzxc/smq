package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Message;
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

    Storage s;

    abstract StorageFactory factory();

    @BeforeEach
    void openStorage() {
        s = factory().open(dir);
    }

    @AfterEach
    void closeStorage() {
        s.close();
    }

    @Test
    void 꺼내기가_정확히_상한만큼_옮기면_모두_옮기고_남았다고_하지_않는다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult exact = pullAfterExhausting("exact", cap);
        assertTrue(exact.messages().isEmpty());
        assertFalse(exact.deadLettersRemain());
        assertEquals(cap, s.counts("dead-exact", T0.plus(LEASE)).ready());
        assertEquals(0, s.counts("exact", T0.plus(LEASE)).ready());
    }

    @Test
    void 상한보다_하나_많으면_상한만큼_옮기고_남았다고_알린다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        PullResult over = pullAfterExhausting("over", cap + 1);
        assertTrue(over.messages().isEmpty());
        assertTrue(over.deadLettersRemain());
        assertEquals(cap, s.counts("dead-over", T0.plus(LEASE)).ready());
    }

    @Test
    void 남았다는_표시_뒤의_다음_꺼내기가_나머지를_옮긴다() {
        int cap = DeadLetters.MAX_MOVES_PER_PULL;
        pullAfterExhausting("over", cap + 1);
        PullResult next = s.pull("over", 1, LEASE, T0.plus(LEASE));
        assertTrue(next.messages().isEmpty());
        assertFalse(next.deadLettersRemain());
        assertEquals(cap + 1, s.counts("dead-over", T0.plus(LEASE)).ready());
    }

    @Test
    void 옮겨진_메시지는_새_메시지라_전달_횟수를_처음부터_센다() {
        pullAfterExhausting("over", DeadLetters.MAX_MOVES_PER_PULL + 1);
        Message moved = s.pull("dead-over", 1, LEASE, T0.plus(LEASE)).messages().getFirst();
        assertEquals(1, moved.deliveryCount());
    }

    @Test
    void 발행_배치_중간에_실패하면_배치_전체가_반영되지_않는다() {
        s.declare("q", QueueOptions.defaults());
        var ok = new NewMessage(new byte[] {1}, Map.of());
        var bad = new NewMessage(null, Map.of());
        assertThrows(RuntimeException.class, () -> s.publish("q", List.of(ok, bad), T0));
        assertEquals(0, s.counts("q", T0).ready());
    }

    /** 최대 전달 횟수 1인 큐에 count개를 발행해 한 번씩 꺼내고 리스가 만료된 시각에 다시 꺼낸 결과를 돌려준다. */
    private PullResult pullAfterExhausting(String queue, int count) {
        s.declare("dead-" + queue, QueueOptions.defaults());
        s.declare(queue, QueueOptions.defaults().maxDeliveries(1, "dead-" + queue));
        List<NewMessage> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(new NewMessage(new byte[] {(byte) i}, Map.of()));
        }
        s.publish(queue, batch, T0);
        assertEquals(count, s.pull(queue, count, LEASE, T0).messages().size());
        return s.pull(queue, 1, LEASE, T0.plus(LEASE));
    }
}
