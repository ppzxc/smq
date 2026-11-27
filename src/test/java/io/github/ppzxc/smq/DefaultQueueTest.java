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
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

@Order(1)
class DefaultQueueTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final int MAX_PULL_CALLS = 20;

    private static final class ScriptedStorage implements Storage {
        final Queue<PullResult> script = new ArrayDeque<>();
        int pulls;

        ScriptedStorage(final PullResult... results) {
            script.addAll(List.of(results));
        }

        @Override
        public PullResult pull(String queue, int max, Duration lease, InstantSource clock) {
            final PullResult next;
            pulls++;
            if (pulls > MAX_PULL_CALLS) {
                throw new AssertionError("꺼내기를 " + MAX_PULL_CALLS + "번 넘게 되풀이했다");
            }
            next = script.poll();
            return next != null ? next : PullResult.empty();
        }

        @Override
        public void snapshot(Path target) {
            throw new UnsupportedOperationException();
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
        public QueueCounts counts(String queue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<MessageId> publish(String queue, List<NewMessage> batch, InstantSource clock) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Result> settle(LeaseToken token, List<Disposition> items, InstantSource clock) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, InstantSource clock) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static DefaultQueue queueOver(final ScriptedStorage storage) {
        return new DefaultQueue(
                storage,
                () -> T0,
                Duration.ofSeconds(10),
                new PullWaiter(Optional.empty()),
                new Object(),
                "q",
                QueueOptions.defaults());
    }

    private static PullResult delivered() {
        final Message m;
        m = new Message(new MessageId(1), new byte[] {1}, Map.of(), 1, T0);
        return PullResult.of(new LeaseToken(1), T0.plusSeconds(10), List.of(m));
    }

    @Test
    @DisplayName("남은 dead letter가 없으면 빈 청크를 반환한다")
    void emptyWithoutDeadLettersReturnsEmptyChunk() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(PullResult.empty())) {
            final Chunk chunk;
            chunk = queueOver(storage).pull(1, Duration.ZERO);
            assertTrue(chunk.isEmpty(), "The returned chunk should be empty.");
        }
    }

    @Test
    @DisplayName("남은 dead letter가 없으면 빈 청크의 리스 기한이 없다")
    void emptyWithoutDeadLettersHasNoLeaseDeadline() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(PullResult.empty())) {
            final Chunk chunk;
            chunk = queueOver(storage).pull(1, Duration.ZERO);
            assertNull(chunk.leaseDeadline(), "The empty chunk should have no lease deadline.");
        }
    }

    @Test
    @DisplayName("남은 dead letter가 없으면 한 번만 꺼낸다")
    void emptyWithoutDeadLettersPullsOnce() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(PullResult.empty())) {
            queueOver(storage).pull(1, Duration.ZERO);
            assertEquals(1, storage.pulls, "Storage should be queried once.");
        }
    }

    @Test
    @DisplayName("남은 dead letter 처리가 끝나면 빈 청크를 반환한다")
    void deadLetterRetriesReturnEmptyChunk() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(
                PullResult.emptyDeadLettersRemain(), PullResult.emptyDeadLettersRemain(), PullResult.empty())) {
            final Chunk chunk;
            chunk = queueOver(storage).pull(1, Duration.ZERO);
            assertTrue(chunk.isEmpty(), "The returned chunk should be empty.");
        }
    }

    @Test
    @DisplayName("남은 dead letter가 있는 동안 즉시 다시 꺼낸다")
    void deadLetterRetriesContinueUntilComplete() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(
                PullResult.emptyDeadLettersRemain(), PullResult.emptyDeadLettersRemain(), PullResult.empty())) {
            queueOver(storage).pull(1, Duration.ZERO);
            assertEquals(3, storage.pulls, "Storage should be queried until no dead letters remain.");
        }
    }

    @Test
    @DisplayName("dead letter 처리 뒤 메시지가 나오면 그 메시지를 반환한다")
    void messageAfterDeadLettersIsReturned() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(PullResult.emptyDeadLettersRemain(), delivered())) {
            final Chunk chunk;
            chunk = queueOver(storage).pull(1, Duration.ZERO);
            assertEquals(
                    List.of(new MessageId(1)),
                    chunk.messages().stream().map(Message::id).toList(),
                    "The delivered message ID should be preserved.");
        }
    }

    @Test
    @DisplayName("dead letter 처리 뒤 메시지가 나오면 더 꺼내지 않는다")
    void messageAfterDeadLettersStopsFurtherPulls() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage(PullResult.emptyDeadLettersRemain(), delivered())) {
            queueOver(storage).pull(1, Duration.ZERO);
            assertEquals(2, storage.pulls, "Storage should not be queried after delivery.");
        }
    }

    @Test
    @DisplayName("빈 저장소를 기다리면 빈 청크를 반환한다")
    void emptyPollReturnsEmptyChunk() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage()) {
            final Chunk chunk;
            chunk = queueOver(storage).pull(1, Duration.ofMillis(30));
            assertTrue(chunk.isEmpty(), "The returned chunk should be empty.");
        }
    }

    @Test
    @DisplayName("빈 저장소를 기다리는 동안 다시 조회한다")
    void emptyPollRetriesWhileWaiting() throws Exception {
        try (ScriptedStorage storage = new ScriptedStorage()) {
            queueOver(storage).pull(1, Duration.ofMillis(30));
            assertTrue(storage.pulls >= 2, "Storage should be queried more than once.");
        }
    }
}
