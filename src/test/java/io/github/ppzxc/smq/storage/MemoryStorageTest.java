package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MemoryStorageTest {

    @Test
    void 발행_배치_중간에_실패하면_저장소가_하나도_반영하지_않는다() {
        MemoryStorage s = new MemoryStorage();
        s.declare("q", QueueOptions.defaults());
        var ok = new NewMessage(new byte[] {1}, Map.of());
        var bad = new NewMessage(null, Map.of());
        assertThrows(RuntimeException.class, () -> s.publish("q", List.of(ok, bad), Instant.EPOCH));
        assertEquals(0, s.counts("q", Instant.EPOCH).ready());
    }

    @Test
    void 변경_힌트는_wake하면_기다리던_스레드를_즉시_깨운다() throws Exception {
        MemoryStorage s = new MemoryStorage();
        s.declare("q", QueueOptions.defaults());
        ChangeHint hint = s.changeHint().orElseThrow();
        long seen = hint.version();
        var woke = new java.util.concurrent.CountDownLatch(1);
        Thread.ofPlatform().start(() -> {
            try {
                hint.await(seen, java.time.Duration.ofSeconds(30));
                woke.countDown();
            } catch (InterruptedException ignored) {
            }
        });
        Thread.sleep(100);
        hint.wake();
        assertTrue(woke.await(2, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    void 힌트는_이미_바뀐_버전을_보고_기다리면_바로_돌아온다() throws Exception {
        MemoryStorage s = new MemoryStorage();
        s.declare("q", QueueOptions.defaults());
        ChangeHint hint = s.changeHint().orElseThrow();
        long seen = hint.version();
        hint.wake();
        long start = System.nanoTime();
        hint.await(seen, java.time.Duration.ofSeconds(30));
        assertTrue(java.time.Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000);
    }
}
