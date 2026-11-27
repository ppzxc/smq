package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.sqlite.Deadline.Interrupted;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionExecutorReadTest {

    @TempDir
    Path dir;

    private Connection connection;
    private TransactionExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        this.connection = SqliteStorage.connect(this.dir.resolve("t.db"), Durability.POWER_LOSS);

        try (Statement s = this.connection.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("CREATE TABLE t (v INTEGER)");
        }

        this.executor = new TransactionExecutor(this.connection);
    }

    @AfterEach
    void tearDown() throws Exception {
        this.connection.close();
    }

    @Test
    @DisplayName("확실히 비어 있으면 없다고 답한다")
    void ifItIsDefinitelyEmptyAnswerNone() {
        Assertions.assertFalse(this.executor.anyOrUncertain(c -> false), "The read should report no match.");
    }

    @Test
    @DisplayName("읽기가 있다고 하면 있다고 답한다")
    void ifThereIsAReadOperationItRespondsWithYes() {
        Assertions.assertTrue(this.executor.anyOrUncertain(c -> true), "The read should report a match.");
    }

    @Test
    @DisplayName("실패한 읽기 뒤 다른 스레드에서 연산할 수 있다 — check 1")
    void afterAFailedReadOperationsCanBePerformedInAnotherThreadCheck1() throws Exception {
        final Throwable assertionValue1;
        assertionValue1 = captureThrowable(() -> this.executor.read(c -> {
            throw new SQLException("injected");
        }));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue1, "Read should throw SmqStorageException.");

        try (ExecutorService threads = Executors.newSingleThreadExecutor()) {
            evaluate(threads.<Integer>submit(
                            () -> this.executor.read(c -> 42, Deadline.after(Duration.ofSeconds(1L), Duration.ZERO)))
                    .get(2L, TimeUnit.SECONDS));
        }
    }

    @Test
    @DisplayName("실패한 읽기 뒤 다른 스레드에서 연산할 수 있다 — check 2")
    void afterAFailedReadOperationsCanBePerformedInAnotherThreadCheck2() throws Exception {
        evaluate(captureThrowable(() -> this.executor.read(c -> {
            throw new SQLException("injected");
        })));

        try (ExecutorService threads = Executors.newSingleThreadExecutor()) {
            final Integer assertionValue2Argument2;
            assertionValue2Argument2 = threads.<Integer>submit(
                            () -> this.executor.read(c -> 42, Deadline.after(Duration.ofSeconds(1L), Duration.ZERO)))
                    .get(2L, TimeUnit.SECONDS);
            Assertions.assertEquals(42, assertionValue2Argument2, "Submit should match the expected value.");
        }
    }

    @Test
    void interruptedLockAttemptDoesNotPreventLaterOperations() throws Exception {
        try (ExecutorService threads = Executors.newSingleThreadExecutor()) {
            final Future<Integer> result;
            result = threads.submit(() -> {
                Thread.currentThread().interrupt();

                try {
                    Assertions.assertThrows(
                            Interrupted.class,
                            () -> this.executor.read(c -> 1, Deadline.after(Duration.ofSeconds(1L), Duration.ZERO)),
                            "Read should throw Deadline.Interrupted.");
                    return this.executor.read(c -> 42, Deadline.after(Duration.ofSeconds(1L), Duration.ZERO));
                } finally {
                    Thread.interrupted();
                }
            });
            Assertions.assertEquals(42, result.get(2L, TimeUnit.SECONDS), "Result should match the expected value.");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 517, 19})
    @DisplayName("SQL 오류는 코드와 상관없이 재시도 없이 SmqStorageException이다 — check 1")
    void regardlessOfTheCodeAnSQLErrorResultsInAnSmqStorageExceptionWithoutARetryCheck1(final int errorCode) {
        final int[] calls;
        final Throwable assertionValue1;
        calls = new int[] {0};
        assertionValue1 = captureThrowable(() -> this.executor.anyOrUncertain(c -> {
            calls[0]++;
            throw new SQLException("boom", null, errorCode);
        }));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue1, "Any or uncertain should throw SmqStorageException.");
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 517, 19})
    @DisplayName("SQL 오류는 코드와 상관없이 재시도 없이 SmqStorageException이다 — check 2")
    void regardlessOfTheCodeAnSQLErrorResultsInAnSmqStorageExceptionWithoutARetryCheck2(final int errorCode) {
        final int[] calls;
        final int assertionValue2Argument2;
        calls = new int[] {0};
        evaluate(captureThrowable(() -> this.executor.anyOrUncertain(c -> {
            calls[0]++;
            throw new SQLException("boom", null, errorCode);
        })));
        assertionValue2Argument2 = calls[0];
        Assertions.assertEquals(1, assertionValue2Argument2, "Calls should match the expected value.");
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }
}
