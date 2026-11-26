package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class InMemorySmqTest extends SmqConformanceTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.MEMORY;
    }

    @Test
    void 인메모리_빌더에는_durability가_없다() {

        assertFalse(Arrays.stream(Smq.Builder.class.getMethods())
                .anyMatch(m -> m.getName().equals("durability")));
    }

    @Test
    void 인메모리_저장소는_스냅샷을_지원하지_않는다() {
        try (Smq smq = Smq.inMemory().open()) {
            assertThrows(UnsupportedOperationException.class, () -> smq.snapshot(dir.resolve("snap.db")));
        }
    }
}
