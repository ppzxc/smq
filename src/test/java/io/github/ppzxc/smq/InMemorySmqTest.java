package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InMemorySmqTest extends SmqConformanceTest {

    @Override
    StorageFactory factory() {
        return StorageFactory.MEMORY;
    }

    @Test
    @DisplayName("인메모리 빌더에는 durability가 없다")
    void inMemoryBuildersLackDurability() {

        assertFalse(
                Arrays.stream(Smq.Builder.class.getMethods())
                        .anyMatch(m -> m.getName().equals("durability")),
                "The memory builder should not expose durability.");
    }

    @Test
    @DisplayName("인메모리 저장소는 스냅샷을 지원하지 않는다")
    void inMemoryStorageDoesNotSupportSnapshots() {
        try (Smq smq = Smq.inMemory().open()) {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> smq.snapshot(dir.resolve("snap.db")),
                    "Snapshot should throw UnsupportedOperationException.");
        }
    }
}
