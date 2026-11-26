package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.SmqStorageException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @Test
    void 실패한_읽기_뒤_다른_스레드에서_연산할_수_있다() throws Exception {
        assertThrows(
                SmqStorageException.class,
                () -> executor.read(c -> {
                    throw new SQLException("injected");
                }));
        try (var threads = Executors.newSingleThreadExecutor()) {
            assertEquals(
                    42,
                    threads.submit(() -> executor.read(c -> 42, Deadline.after(Duration.ofSeconds(1), Duration.ZERO)))
                            .get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedLockAttemptDoesNotPreventLaterOperations() throws Exception {
        try (var threads = Executors.newSingleThreadExecutor()) {
            var result = threads.submit(() -> {
                Thread.currentThread().interrupt();
                try {
                    assertThrows(
                            Deadline.Interrupted.class,
                            () -> executor.read(c -> 1, Deadline.after(Duration.ofSeconds(1), Duration.ZERO)));
                    return executor.read(c -> 42, Deadline.after(Duration.ofSeconds(1), Duration.ZERO));
                } finally {
                    Thread.interrupted();
                }
            });
            assertEquals(42, result.get(2, TimeUnit.SECONDS));
        }
    }

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
