package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@FunctionalInterface
public interface StorageFactory {

    String V1_SAMPLE_RESOURCE = "/schema-samples/schema-v1.db";

    Storage open(Path dir);

    StorageFactory MEMORY = dir -> new MemoryStorage();

    static StorageFactory sqliteNew(final Durability durability) {
        return dir -> SqliteStorage.open(dir.resolve("smq.db"), durability);
    }

    static StorageFactory sqliteV1Sample(final Durability durability) {
        return dir -> {
            final Path db;
            db = dir.resolve("smq.db");
            if (Files.notExists(db)) {
                copyV1Sample(db);
            }
            return SqliteStorage.open(db, durability);
        };
    }

    static Path copyV1Sample(final Path target) {
        try (InputStream in = StorageFactory.class.getResourceAsStream(V1_SAMPLE_RESOURCE)) {
            assertNotNull(in, "src/test/resources" + V1_SAMPLE_RESOURCE + " 가 없다");
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
