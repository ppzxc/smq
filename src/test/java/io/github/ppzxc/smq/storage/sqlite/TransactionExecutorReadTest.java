package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.SmqStorageException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** autocommit 읽기. 확실하지 않으면 "있다"고 답하고, 쓰기 락도 재시도도 읽기 트랜잭션도 남기지 않는다. */
class TransactionExecutorReadTest {

    private static final int SQLITE_BUSY_SNAPSHOT = 517;

    @TempDir
    Path dir;

    private Connection connection;
    private TransactionExecutor executor;
    private int waits;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("t.db"));
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("CREATE TABLE t (v INTEGER)");
        }
        executor = new TransactionExecutor(connection, new TransactionExecutor.RetryPolicy(5, a -> waits++));
        executor.executeUnverified(c -> 0); // 기준 버전 0
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    private void setUserVersion(int v) throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA user_version = " + v);
        }
    }

    @Test
    void 확실히_비어_있고_버전이_그대로면_없다고_답한다() {
        assertFalse(executor.anyOrUncertain(c -> false));
    }

    @Test
    void 읽기가_있다고_하면_있다고_답한다() {
        assertTrue(executor.anyOrUncertain(c -> true));
    }

    @Test
    void 비어_보여도_버전이_바뀌었으면_있다고_답한다() throws Exception {
        setUserVersion(7);
        assertTrue(executor.anyOrUncertain(c -> false));
    }

    @Test
    void 영구_실패_상태면_버전이_돌아와도_있다고_답한다() throws Exception {
        setUserVersion(7);
        assertThrows(RuntimeException.class, () -> executor.execute(c -> null));
        setUserVersion(0);
        assertTrue(executor.anyOrUncertain(c -> false));
    }

    @Test
    void BUSY가_나면_재시도_없이_있다고_답한다() {
        int[] calls = {0};
        assertTrue(executor.anyOrUncertain(c -> {
            calls[0]++;
            throw new SQLException("busy", null, SQLITE_BUSY_SNAPSHOT);
        }));
        assertEquals(1, calls[0]);
        assertEquals(0, waits);
    }

    @Test
    void BUSY가_아닌_오류는_SmqStorageException이다() {
        assertThrows(
                SmqStorageException.class,
                () -> executor.anyOrUncertain(c -> {
                    throw new SQLException("boom", null, 19);
                }));
    }

    @Test
    void 읽는_동안과_읽은_뒤에_다른_연결의_쓰기가_막히지_않는다() throws Exception {
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("t.db"))) {
            try (Statement s = other.createStatement()) {
                s.execute("PRAGMA busy_timeout=0");
            }
            executor.anyOrUncertain(c -> {
                insert(other); // 읽는 중에도 쓰기 락이 비어 있다.
                return false;
            });
            insert(other); // 읽기 트랜잭션도 남지 않았다.
        }
    }

    private static void insert(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("INSERT INTO t VALUES (1)");
        }
    }
}
