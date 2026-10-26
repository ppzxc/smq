package io.github.ppzxc.smq;

import java.nio.file.Path;

class SqliteSmqTest extends SmqConformanceTest {

    @Override
    Smq.Builder builder(Path dir) {
        return Smq.sqlite(dir.resolve("smq.db"));
    }
}
