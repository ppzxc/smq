package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Extended;
import io.github.ppzxc.smq.storage.LeaseToken;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import io.github.ppzxc.smq.storage.Result;
import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * 꺼내기의 재시도 판단을 저장소 호출 횟수로 못 박는다. 재시도 판단이 틀리면 대기 루프가 끝나지 않으므로,
 * 호출 횟수가 상한을 넘으면 시간 제한을 기다리지 않고 바로 실패한다.
 */
@Order(1)
class DefaultQueueTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final int MAX_PULL_CALLS = 20;

    /** 꺼내기 결과를 차례로 돌려주고 호출 횟수를 센다. 준비한 결과가 떨어지면 빈 결과다. */
    private static final class ScriptedStorage implements Storage {
        final Queue<PullResult> script = new ArrayDeque<>();
        int pulls;

        ScriptedStorage(PullResult... results) {
            script.addAll(List.of(results));
        }

        @Override
        public PullResult pull(String queue, int max, Duration lease, Instant now) {
            if (++pulls > MAX_PULL_CALLS) {
                throw new AssertionError("꺼내기를 " + MAX_PULL_CALLS + "번 넘게 되풀이했다");
            }
            PullResult next = script.poll();
            return next != null ? next : PullResult.empty();
        }

        @Override
        public void declare(String queue, QueueOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(String queue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(String queue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<QueueOptions> options(String queue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueueCounts counts(String queue, Instant now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<MessageId> publish(String queue, List<NewMessage> batch, Instant now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Result> settle(LeaseToken token, List<Disposition> items, Instant now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, Instant now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static DefaultQueue queueOver(ScriptedStorage storage) {
        return new DefaultQueue(storage, () -> T0, Duration.ofSeconds(10), new PullWaiter(Optional.empty()), "q",
                QueueOptions.defaults());
    }

    private static PullResult delivered() {
        Message m = new Message(new MessageId(1), new byte[] {1}, Map.of(), 1, T0);
        return PullResult.of(new LeaseToken(1), T0.plusSeconds(10), List.of(m));
    }

    @Test
    void 비어_돌아오고_남은_dead_letter_표시가_없으면_한_번만_꺼내고_빈_청크다() throws Exception {
        ScriptedStorage storage = new ScriptedStorage(PullResult.empty());

        Chunk chunk = queueOver(storage).pull(1, Duration.ZERO);

        assertTrue(chunk.isEmpty());
        assertNull(chunk.leaseDeadline());
        assertEquals(1, storage.pulls);
    }

    @Test
    void 남은_dead_letter_표시가_있으면_기다리지_않고_바로_다시_꺼내_표시가_없어질_때까지_되풀이한다() throws Exception {
        ScriptedStorage storage = new ScriptedStorage(PullResult.emptyDeadLettersRemain(),
                PullResult.emptyDeadLettersRemain(), PullResult.empty());

        Chunk chunk = queueOver(storage).pull(1, Duration.ZERO);

        assertTrue(chunk.isEmpty());
        assertEquals(3, storage.pulls);
    }

    @Test
    void 남은_dead_letter_표시_뒤에_메시지가_나오면_그_청크를_돌려주고_더_꺼내지_않는다() throws Exception {
        ScriptedStorage storage = new ScriptedStorage(PullResult.emptyDeadLettersRemain(), delivered());

        Chunk chunk = queueOver(storage).pull(1, Duration.ZERO);

        assertEquals(List.of(new MessageId(1)), chunk.messages().stream().map(Message::id).toList());
        assertEquals(2, storage.pulls);
    }

    @Test
    void 표시가_없으면_wait_동안만_폴링하고_그_안의_호출_수는_한정된다() throws Exception {
        ScriptedStorage storage = new ScriptedStorage();

        Chunk chunk = queueOver(storage).pull(1, Duration.ofMillis(30));

        // 폴링 간격이 1ms에서 늘어나므로 30ms 동안의 호출은 한 자릿수에서 십여 번이다. 쉬지 않고 돌면 상한을 넘는다.
        assertTrue(chunk.isEmpty());
        assertTrue(storage.pulls >= 2, "pulls=" + storage.pulls);
    }
}
