package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.SmqStorageException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** autocommit 읽기. 큐가 없어 확실하지 않으면 "있다"고 답하고, SQL 오류는 BUSY도 예외다. */
class TransactionExecutorReadTest {

    private static final int SQLITE_BUSY_SNAPSHOT = 517;

    @TempDir
    Path dir;

    private Connection connection;
    private TransactionExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        connection = SqliteStorage.connect(dir.resolve("t.db"), Durability.POWER_LOSS);
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("CREATE TABLE t (v INTEGER)");
        }
        executor = new TransactionExecutor(connection);
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    @Test
    void 확실히_비어_있으면_없다고_답한다() {
        assertFalse(executor.anyOrUncertain(c -> false));
    }

    @Test
    void 읽기가_있다고_하면_있다고_답한다() {
        assertTrue(executor.anyOrUncertain(c -> true));
    }

    /** SQL 오류는 BUSY(5, 517)든 아니든 같은 규칙이다. 재시도 없이 한 번 실행하고 예외로 올린다. */
    @ParameterizedTest
    @ValueSource(ints = {5, SQLITE_BUSY_SNAPSHOT, 19})
    void SQL_오류는_코드와_상관없이_재시도_없이_SmqStorageException이다(int errorCode) {
        int[] calls = {0};
        assertThrows(
                SmqStorageException.class,
                () -> executor.anyOrUncertain(c -> {
                    calls[0]++;
                    throw new SQLException("boom", null, errorCode);
                }));
        assertEquals(1, calls[0]);
    }
}
