package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.ppzxc.smq.storage.ChangeHint;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PullWaiterTest {

    /** 호출을 기록하고 기다리지 않고 바로 돌아오는 힌트. {@code beforeVersion}은 버전을 읽기 직전에 실행한다. */
    private static final class RecordingHint implements ChangeHint {
        long version;
        final List<Duration> awaits = new ArrayList<>();
        Runnable beforeVersion = () -> {};

        @Override
        public long version() {
            beforeVersion.run();
            return version;
        }

        @Override
        public void await(long seen, Duration timeout) {
            awaits.add(timeout);
        }

        @Override
        public void wake() {
            version++;
        }
    }

    @Test
    void 버전을_읽는_사이에_닫혀도_깨움을_놓치지_않고_기다리지_않는다() throws Exception {
        RecordingHint hint = new RecordingHint();
        PullWaiter waiter = new PullWaiter(hint, true);
        AtomicInteger reads = new AtomicInteger();
        hint.beforeVersion = () -> {
            if (reads.getAndIncrement() == 0) {
                waiter.close();
            }
        };
        String result = waiter.await(Duration.ofSeconds(30), () -> null, () -> "empty");
        assertEquals("empty", result);
        assertEquals(List.of(), hint.awaits);
    }

    @Test
    void 꺼내는_중에_저장소가_닫혀_예외가_나도_닫힌_상태면_빈_결과다() throws Exception {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        String result = waiter.await(Duration.ofSeconds(30), () -> {
            waiter.close();
            throw new IllegalStateException("storage closed");
        }, () -> "empty");
        assertEquals("empty", result);
    }

    @Test
    void 닫히지_않았을_때의_예외는_그대로_던진다() {
        PullWaiter waiter = new PullWaiter(new RecordingHint(), true);
        assertThrows(IllegalStateException.class, () -> waiter.await(Duration.ofSeconds(30), () -> {
            throw new IllegalStateException("boom");
        }, () -> "empty"));
    }

    private static List<Duration> awaitsUntilFifthAttempt(PullWaiter waiter, RecordingHint hint) throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        waiter.await(Duration.ofSeconds(30), () -> attempts.incrementAndGet() < 6 ? null : "got", () -> "empty");
        return hint.awaits;
    }

    @Test
    void 저장소_힌트가_있으면_폴링_간격을_늘리지_않고_상한만큼_힌트를_기다린다() throws Exception {
        RecordingHint hint = new RecordingHint();
        List<Duration> awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, true), hint);
        assertEquals(List.of(Duration.ofMillis(250), Duration.ofMillis(250), Duration.ofMillis(250),
                Duration.ofMillis(250), Duration.ofMillis(250)), awaits);
    }

    @Test
    void 저장소_힌트가_없으면_폴링_간격이_1ms에서_250ms까지_늘어난다() throws Exception {
        RecordingHint hint = new RecordingHint();
        List<Duration> awaits = awaitsUntilFifthAttempt(new PullWaiter(hint, false), hint);
        assertEquals(List.of(Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(4),
                Duration.ofMillis(8), Duration.ofMillis(16)), awaits);
    }
}
