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
        // 컴파일 타임에 `Smq.inMemory().durability(..)`는 막혀 있다. 이 테스트는 누가 메서드를 추가해
        // 그 약속(인메모리에는 내구성 옵션이 없다, ADR 0005)을 깨는 것을 알리는 용도로만 둔다.
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
