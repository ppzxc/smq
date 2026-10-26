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
}
