package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** factory가 파일 저장소를 다시 여는 방식을 지킨다. 재오픈은 데이터를 보존해야 하고, 견본 복사는 덮어쓴다. */
class StorageFactoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    private static void publishOne(Storage s) {
        s.declare("extra", QueueOptions.defaults());
        s.publish("extra", List.of(new NewMessage(new byte[] {1}, Map.of())), T0);
    }

    @Test
    void v1_견본_factory로_같은_디렉터리를_다시_열어도_데이터가_유지된다() {
        try (Storage first = StorageFactory.SQLITE_V1_SAMPLE.open(dir)) {
            publishOne(first);
        }
        try (Storage second = StorageFactory.SQLITE_V1_SAMPLE.open(dir)) {
            assertEquals(1, second.counts("extra", T0).ready());
        }
    }

    @Test
    void 새_SQLite_factory로_같은_디렉터리를_다시_열어도_데이터가_유지된다() {
        try (Storage first = StorageFactory.SQLITE_NEW.open(dir)) {
            publishOne(first);
        }
        try (Storage second = StorageFactory.SQLITE_NEW.open(dir)) {
            assertEquals(1, second.counts("extra", T0).ready());
        }
    }

    @Test
    void copyV1Sample은_이미_있는_파일을_견본으로_덮어쓴다() throws Exception {
        Path target = dir.resolve("smq.db");
        Files.writeString(target, "not a database");
        StorageFactory.copyV1Sample(target);
        long sampleSize = Files.size(target);
        assertNotEquals("not a database".length(), sampleSize);
        StorageFactory.copyV1Sample(target);
        assertEquals(sampleSize, Files.size(target));
    }
}
