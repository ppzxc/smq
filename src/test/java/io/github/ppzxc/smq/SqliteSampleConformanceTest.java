package io.github.ppzxc.smq;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;

/** 견본 복사본 위에서 적합성 시나리오 전체를 돌린다. 견본의 큐 이름과 겹치지 않으므로 시나리오는 그대로 통과해야 한다. */
class SqliteSampleConformanceTest extends SmqConformanceTest {

    @Override
    TestBuilder builder(Path dir) {
        Path db = dir.resolve("smq.db");
        try {
            if (Files.notExists(db)) {
                SchemaV1SampleTest.copySample(db);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        Smq.SqliteBuilder b = Smq.sqlite(db);
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
}
