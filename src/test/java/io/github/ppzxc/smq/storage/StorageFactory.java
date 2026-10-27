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

/**
 * 테스트가 저장소를 여는 유일한 곳. 디렉터리를 받아 저장소를 돌려주며, 저장소 구성(메모리, 새 SQLite 파일,
 * v1 견본 복사본 SQLite 파일)을 여기서만 정의한다. SQLite는 기본 내구성 수준으로 연다.
 */
@FunctionalInterface
public interface StorageFactory {

    String V1_SAMPLE_RESOURCE = "/schema-samples/schema-v1.db";

    Storage open(Path dir);

    StorageFactory MEMORY = dir -> new MemoryStorage();

    StorageFactory SQLITE_NEW = dir -> SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS);

    /** 파일이 아직 없을 때만 견본을 복사한다. 이미 있으면 그대로 열어 재오픈이 데이터를 보존한다. */
    StorageFactory SQLITE_V1_SAMPLE = dir -> {
        Path db = dir.resolve("smq.db");
        if (Files.notExists(db)) {
            copyV1Sample(db);
        }
        return SqliteStorage.open(db, Durability.POWER_LOSS);
    };

    /** 커밋된 v1 견본 파일을 target으로 복사한다. 이미 있으면 덮어쓴다. */
    static Path copyV1Sample(Path target) {
        try (InputStream in = StorageFactory.class.getResourceAsStream(V1_SAMPLE_RESOURCE)) {
            assertNotNull(in, "src/test/resources" + V1_SAMPLE_RESOURCE + " 가 없다");
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
