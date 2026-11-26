package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageFactoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    private static void publishOne(Storage s) {
        s.declare("extra", QueueOptions.defaults());
        s.publish("extra", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
    }

    @Test
    void v1_견본_factory로_같은_디렉터리를_다시_열어도_데이터가_유지된다() {
        try (Storage first =
                StorageFactory.sqliteV1Sample(Durability.PROCESS_CRASH).open(dir)) {
            publishOne(first);
        }
        try (Storage second =
                StorageFactory.sqliteV1Sample(Durability.PROCESS_CRASH).open(dir)) {
            assertEquals(1, second.counts("extra").ready());
        }
    }

    @Test
    void 새_SQLite_factory로_같은_디렉터리를_다시_열어도_데이터가_유지된다() {
        try (Storage first = StorageFactory.sqliteNew(Durability.PROCESS_CRASH).open(dir)) {
            publishOne(first);
        }
        try (Storage second = StorageFactory.sqliteNew(Durability.PROCESS_CRASH).open(dir)) {
            assertEquals(1, second.counts("extra").ready());
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
