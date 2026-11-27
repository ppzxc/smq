package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

@Order(1)
class StorageContractMemoryTest extends StorageContractTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.MEMORY;
    }

    @Test
    @DisplayName("변경 힌트는 wake하면 기다리던 스레드를 즉시 깨운다")
    void whenAChangeHintIsWokenItImmediatelyWakesTheWaitingThread() throws Exception {
        final ChangeHint hint;
        final long seen;
        hint = storage.changeHint().orElseThrow();
        seen = hint.version();
        final var woke = new CountDownLatch(1);
        Thread.ofPlatform().start(() -> {
            try {
                hint.await(seen, Duration.ofSeconds(30));
                woke.countDown();
            } catch (InterruptedException ignored) {
            }
        });
        Thread.sleep(100);
        hint.wake();
        assertTrue(woke.await(2, TimeUnit.SECONDS), "The operation should complete before the timeout.");
    }

    @Test
    @DisplayName("힌트는 이미 바뀐 버전을 보고 기다리면 바로 돌아온다")
    void theHintIsThatIfYouSeeTheAlreadyChangedVersionAndWaitItWillReturnImmediately() throws Exception {
        final ChangeHint hint;
        final long seen;
        final long start;
        hint = storage.changeHint().orElseThrow();
        seen = hint.version();
        hint.wake();
        start = System.nanoTime();
        hint.await(seen, Duration.ofSeconds(30));
        assertTrue(
                Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000, "To millis should be less than 1000.");
    }
}
