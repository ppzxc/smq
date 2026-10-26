package io.github.ppzxc.smq;

import java.nio.file.Path;

class InMemorySmqTest extends SmqConformanceTest {

    @Override
    Smq.Builder builder(Path dir) {
        return Smq.inMemory();
    }
}
