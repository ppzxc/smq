package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class InMemorySmqTest extends SmqConformanceTest {

    @Override
    TestBuilder builder(Path dir) {
        Smq.Builder b = Smq.inMemory();
        return new TestBuilder() {
            @Override
            public TestBuilder clock(InstantSource clock) {
                b.clock(clock);
                return this;
            }

            @Override
            public TestBuilder leaseDuration(Duration leaseDuration) {
                b.leaseDuration(leaseDuration);
                return this;
            }

            @Override
            public Smq open() {
                return b.open();
            }
        };
    }

    @Test
    void 인메모리_빌더에는_durability가_없다() {
        // 컴파일 타임에 `Smq.inMemory().durability(..)`는 막혀 있다. 이 테스트는 누가 메서드를 추가해
        // 그 약속(인메모리에는 내구성 옵션이 없다, ADR 0005)을 깨는 것을 알리는 용도로만 둔다.
        assertFalse(Arrays.stream(Smq.Builder.class.getMethods()).anyMatch(m -> m.getName().equals("durability")));
    }
}
