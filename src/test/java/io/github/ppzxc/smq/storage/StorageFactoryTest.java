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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageFactoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    private static void publishOne(final Storage s) {
        s.declare("extra", QueueOptions.defaults());
        s.publish("extra", List.of(new NewMessage(new byte[] {1}, Map.of())), InstantSource.fixed(T0));
    }

    @Test
    @DisplayName("v1 견본 factory로 같은 디렉터리를 다시 열어도 데이터가 유지된다")
    void dataIsRetainedEvenIfTheSameDirectoryIsReopenedWithTheV1SampleFactory() {
        try (Storage first =
                StorageFactory.sqliteV1Sample(Durability.PROCESS_CRASH).open(dir)) {
            publishOne(first);
        }
        try (Storage second =
                StorageFactory.sqliteV1Sample(Durability.PROCESS_CRASH).open(dir)) {
            assertEquals(1, second.counts("extra").ready(), "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 SQLite factory로 같은 디렉터리를 다시 열어도 데이터가 유지된다")
    void evenIfYouReopenTheSameDirectoryWithANewSQLiteFactoryTheDataIsRetained() {
        try (Storage first = StorageFactory.sqliteNew(Durability.PROCESS_CRASH).open(dir)) {
            publishOne(first);
        }
        try (Storage second = StorageFactory.sqliteNew(Durability.PROCESS_CRASH).open(dir)) {
            assertEquals(1, second.counts("extra").ready(), "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("copyV1Sample은 이미 있는 파일을 견본으로 덮어쓴다")
    void copyv1sampleOverwritesAnExistingFileWithASample() throws Exception {
        final Path target;
        final long sampleSize;
        target = dir.resolve("smq.db");
        Files.writeString(target, "not a database");
        StorageFactory.copyV1Sample(target);
        sampleSize = Files.size(target);
        assertNotEquals("not a database".length(), sampleSize, "Sample size should differ from the unexpected value.");
    }

    @Test
    @DisplayName("copyV1Sample은 견본을 다시 복사해도 크기가 같다")
    void copyingV1SampleAgainPreservesItsSize() throws Exception {
        final Path target;
        final long sampleSize;
        target = dir.resolve("smq.db");
        StorageFactory.copyV1Sample(target);
        sampleSize = Files.size(target);
        StorageFactory.copyV1Sample(target);
        assertEquals(sampleSize, Files.size(target), "Size should match the expected value.");
    }
}
