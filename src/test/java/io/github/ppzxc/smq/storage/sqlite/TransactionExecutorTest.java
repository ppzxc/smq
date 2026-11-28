package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.SmqStorageException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionExecutorTest {

    @TempDir
    Path dir;

    private Connection connection;

    @Test
    @DisplayName("체크포인트 오류를 기록하지 못해도 커밋 결과는 실패로 바뀌지 않는다 — check 1")
    void evenIfACheckpointErrorCannotBeRecordedTheCommitResultDoesNotChangeToAFailureCheck1() throws Throwable {
        final RuntimeException checkpointFailure;
        final RuntimeException loggingFailure;
        final boolean assertionValue1;
        checkpointFailure = new IllegalStateException("checkpoint failed");
        loggingFailure = new IllegalStateException("logger failed");
        assertionValue1 = executeWithoutThrow(() -> TransactionExecutor.logCheckpointFailure(checkpointFailure, () -> {
            throw loggingFailure;
        }));
        Assertions.assertTrue(assertionValue1, "Log checkpoint failure should complete without an exception.");
        evaluate(checkpointFailure.getSuppressed()[0]);
    }

    @Test
    @DisplayName("체크포인트 오류를 기록하지 못해도 커밋 결과는 실패로 바뀌지 않는다 — check 2")
    void evenIfACheckpointErrorCannotBeRecordedTheCommitResultDoesNotChangeToAFailureCheck2() throws Throwable {
        final RuntimeException checkpointFailure;
        final RuntimeException loggingFailure;
        final Throwable assertionValue2Argument2;
        checkpointFailure = new IllegalStateException("checkpoint failed");
        loggingFailure = new IllegalStateException("logger failed");
        evaluate(executeWithoutThrow(() -> TransactionExecutor.logCheckpointFailure(checkpointFailure, () -> {
            throw loggingFailure;
        })));
        assertionValue2Argument2 = checkpointFailure.getSuppressed()[0];
        Assertions.assertSame(loggingFailure, assertionValue2Argument2, "Suppressed should be the same instance.");
    }

    @Test
    @DisplayName("로그가 원래 오류를 다시 던져도 자기 자신을 억제하지 않는다 — check 1")
    void evenIfTheLogRethrowsTheOriginalErrorItDoesNotSuppressItselfCheck1() throws Throwable {
        final RuntimeException failure;
        final boolean assertionValue1;
        failure = new IllegalStateException("checkpoint failed");
        assertionValue1 = executeWithoutThrow(() -> TransactionExecutor.logCheckpointFailure(failure, () -> {
            throw failure;
        }));
        Assertions.assertTrue(assertionValue1, "Log checkpoint failure should complete without an exception.");
    }

    @Test
    @DisplayName("로그가 원래 오류를 다시 던져도 자기 자신을 억제하지 않는다 — check 2")
    void evenIfTheLogRethrowsTheOriginalErrorItDoesNotSuppressItselfCheck2() throws Throwable {
        final RuntimeException failure;
        failure = new IllegalStateException("checkpoint failed");
        evaluate(executeWithoutThrow(() -> TransactionExecutor.logCheckpointFailure(failure, () -> {
            throw failure;
        })));
        Assertions.assertEquals(0, failure.getSuppressed().length, "Length should match the expected value.");
    }

    @BeforeEach
    void setUp() throws Exception {
        this.connection = SqliteStorage.connect(this.dir.resolve("t.db"), Durability.POWER_LOSS);
        try (Statement s = this.connection.createStatement()) {
            s.execute("CREATE TABLE t (v INTEGER)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        this.connection.close();
    }

    private TransactionExecutor executor() {
        return new TransactionExecutor(this.connection);
    }

    private int count() throws SQLException {
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM t")) {
            return rs.getInt(1);
        }
    }

    @Test
    @DisplayName("실패한 트랜잭션은 ROLLBACK되고 다음 트랜잭션이 정상적으로 시작된다 — check 1")
    void failedTransactionsAreRolledBackAndTheNextTransactionStartsNormallyCheck1() throws Exception {
        final TransactionExecutor ex;
        final Throwable assertionValue1;
        ex = this.executor();
        assertionValue1 = captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw new SQLException("boom", null, 19);
        }));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue1, "Execute should throw SmqStorageException.");
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
    }

    @Test
    @DisplayName("실패한 트랜잭션은 ROLLBACK되고 다음 트랜잭션이 정상적으로 시작된다 — check 2")
    void failedTransactionsAreRolledBackAndTheNextTransactionStartsNormallyCheck2() throws Exception {
        final TransactionExecutor ex;
        final int assertionValue2Argument2;
        ex = this.executor();
        evaluate(captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw new SQLException("boom", null, 19);
        })));
        assertionValue2Argument2 = this.count();
        Assertions.assertEquals(0, assertionValue2Argument2, "Count should match the expected value.");
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
    }

    @Test
    @DisplayName("실패한 트랜잭션은 ROLLBACK되고 다음 트랜잭션이 정상적으로 시작된다 — check 3")
    void failedTransactionsAreRolledBackAndTheNextTransactionStartsNormallyCheck3() throws Exception {
        final TransactionExecutor ex;
        final int assertionValue3Argument2;
        ex = this.executor();
        evaluate(captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw new SQLException("boom", null, 19);
        })));
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        assertionValue3Argument2 = this.count();
        Assertions.assertEquals(1, assertionValue3Argument2, "Count should match the expected value.");
    }

    @Test
    @DisplayName("오류로 실패한 트랜잭션도 롤백하고 다음 연산을 허용한다 — check 1")
    void transactionsThatFailDueToErrorsAreAlsoRolledBackAndTheNextOperationIsAllowed() throws Exception {
        final TransactionExecutor ex;
        final AssertionError failure;
        ex = this.executor();
        failure = new AssertionError("injected");
        final var assertionValue3 = captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw failure;
        }));
        Assertions.assertInstanceOf(AssertionError.class, assertionValue3, "Execute should throw AssertionError.");
        final var assertionValue4Argument2 = (AssertionError) assertionValue3;
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue2Argument2 = this.count();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        final var assertionValue1Argument2 = this.count();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("오류로 실패한 트랜잭션도 롤백하고 다음 연산을 허용한다 — check 2")
    void transactionsThatFailDueToErrorsAreAlsoRolledBackAndTheNextOperationIsAllowedCheck2() throws Exception {
        final TransactionExecutor ex;
        final AssertionError failure;
        ex = this.executor();
        failure = new AssertionError("injected");
        final var assertionValue3 = captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw failure;
        }));
        final var assertionValue4Argument2 = (AssertionError) assertionValue3;
        Assertions.assertSame(failure, assertionValue4Argument2, "Assert throws should be the same instance.");
        final var assertionValue2Argument2 = this.count();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        final var assertionValue1Argument2 = this.count();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("오류로 실패한 트랜잭션도 롤백하고 다음 연산을 허용한다 — check 3")
    void transactionsThatFailDueToErrorsAreAlsoRolledBackAndTheNextOperationIsAllowedCheck3() throws Exception {
        final TransactionExecutor ex;
        final AssertionError failure;
        ex = this.executor();
        failure = new AssertionError("injected");
        final var assertionValue3 = captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw failure;
        }));
        final var assertionValue4Argument2 = (AssertionError) assertionValue3;
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue2Argument2 = this.count();
        Assertions.assertEquals(0, assertionValue2Argument2, "Count should match the expected value.");
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        final var assertionValue1Argument2 = this.count();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("오류로 실패한 트랜잭션도 롤백하고 다음 연산을 허용한다 — check 4")
    void transactionsThatFailDueToErrorsAreAlsoRolledBackAndTheNextOperationIsAllowedCheck4() throws Exception {
        final TransactionExecutor ex;
        final AssertionError failure;
        ex = this.executor();
        failure = new AssertionError("injected");
        final var assertionValue3 = captureThrowable(() -> ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1)");
            }
            throw failure;
        }));
        final var assertionValue4Argument2 = (AssertionError) assertionValue3;
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue2Argument2 = this.count();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        final var assertionValue1Argument2 = this.count();
        Assertions.assertEquals(1, assertionValue1Argument2, "Count should match the expected value.");
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 1")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCause() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue7, "Execute should throw SmqStorageException.");
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 2")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck2() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue5, "Execute should throw SmqStorageException.");
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 3")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck3() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        Assertions.assertSame(original, assertionValue6Argument2, "Exception cause should be the same instance.");
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 4")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck4() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue3, "Any or uncertain should throw SmqStorageException.");
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 5")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck5() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        Assertions.assertSame(original, assertionValue4Argument2, "Exception cause should be the same instance.");
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 6")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck6() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        Assertions.assertInstanceOf(
                SmqStorageException.class,
                assertionValue1,
                "Execute outside transaction should throw SmqStorageException.");
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        evaluatePmdArgument(assertionValue2Argument2);
        ex.close();
    }

    @Test
    @DisplayName("롤백이 실패하면 이후 모든 연산이 처음 원인으로 실패한다 — check 7")
    void ifTheRollbackFailsAllSubsequentOperationsWillFailDueToTheOriginalCauseCheck7() {
        final SQLException rollbackFailure;
        final TransactionExecutor ex;
        final SQLException original;
        rollbackFailure = new SQLException("rollback failed");
        ex = new TransactionExecutor(failingQuery(this.connection, "ROLLBACK", () -> rollbackFailure));
        original = new SQLException("body failed");
        final var assertionValue7 = captureThrowable(() -> ex.execute(c -> {
            throw original;
        }));
        evaluatePmdArgument(assertionValue7);
        final var assertionValue5 = captureThrowable(() -> ex.execute(c -> 1));
        final var assertionValue6Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue5).getCause();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue3 = captureThrowable(() -> ex.anyOrUncertain(c -> true));
        final var assertionValue4Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue3).getCause();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue1 = captureThrowable(() -> ex.executeOutsideTransaction(() -> 1));
        final var assertionValue2Argument2 = ((SmqStorageException) (SmqStorageException) assertionValue1).getCause();
        Assertions.assertSame(original, assertionValue2Argument2, "Exception cause should be the same instance.");
        ex.close();
    }

    @Test
    @DisplayName("WAL이 한도를 넘으면 되감겨 WAL이 계속 자라지 않는다 — check 1")
    void walIsRewoundWhenItExceedsTheLimitCheck1() throws Exception {
        final TransactionExecutor executor;
        final boolean assertionValue1Argument1;
        try (Statement s = this.connection.createStatement()) {
            TransactionExecutor.configureWal(s);
        }
        executor = this.executor();
        executor.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("CREATE TABLE big (b BLOB)");
                sx.execute("INSERT INTO big VALUES (zeroblob(20000000))");
            }
            return null;
        });
        assertionValue1Argument1 = Files.size(this.dir.resolve("t.db-wal")) > 16_777_216L;
        Assertions.assertTrue(assertionValue1Argument1, "The WAL should grow beyond the checkpoint threshold.");
        for (int i = 0; i < 16; i++) {
            executor.execute(c -> {
                try (Statement sx = c.createStatement()) {
                    sx.execute("INSERT INTO t VALUES (2)");
                }
                return null;
            });
        }
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(PASSIVE)")) {
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            evaluate(rs.getInt(2) < 100);
            evaluate("WAL에 프레임이 " + rs.getInt(2) + "개 남았다. 되감지 않았다");
        }
    }

    @Test
    @DisplayName("WAL이 한도를 넘으면 되감겨 WAL이 계속 자라지 않는다 — check 2")
    void walIsRewoundWhenItExceedsTheLimitCheck2() throws Exception {
        final TransactionExecutor executor;
        try (Statement s = this.connection.createStatement()) {
            TransactionExecutor.configureWal(s);
        }
        executor = this.executor();
        executor.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("CREATE TABLE big (b BLOB)");
                sx.execute("INSERT INTO big VALUES (zeroblob(20000000))");
            }
            return null;
        });
        evaluate(Files.size(this.dir.resolve("t.db-wal")) > 16_777_216L);
        for (int i = 0; i < 16; i++) {
            executor.execute(c -> {
                try (Statement sx = c.createStatement()) {
                    sx.execute("INSERT INTO t VALUES (2)");
                }
                return null;
            });
        }
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(PASSIVE)")) {
            final boolean assertionValue2Argument1;
            final String assertionValue2Argument2;
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            assertionValue2Argument1 = rs.getInt(2) < 100;
            assertionValue2Argument2 = "WAL에 프레임이 " + rs.getInt(2) + "개 남았다. 되감지 않았다";
            Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 517, 10})
    @DisplayName("커밋 뒤 체크포인트의 SQL 오류는 BUSY든 아니든 삼키고 커밋은 정상 반환된다 — check 1")
    void sqlErrorsInTheCheckpointAfterTheCommitAreSuppressedWhetherTheyAreBUSYOrNotAndTheCommitReturnsNormallyCheck1(
            final int errorCode) throws Throwable {
        final TransactionExecutor ex;
        final boolean assertionValue1;
        this.walExecutor();
        ex = new TransactionExecutor(failingCheckpoint(this.connection, errorCode));
        ex.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("INSERT INTO big VALUES (zeroblob(9437184))");
            }
            return null;
        });
        assertionValue1 = executeWithoutThrow(() -> commitBlobs(ex, 8));
        Assertions.assertTrue(assertionValue1, "Commit blobs should complete without an exception.");
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            evaluate(rs.getInt(1) >= 2);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 517, 10})
    @DisplayName("커밋 뒤 체크포인트의 SQL 오류는 BUSY든 아니든 삼키고 커밋은 정상 반환된다 — check 2")
    void sqlErrorsInTheCheckpointAfterTheCommitAreSuppressedWhetherTheyAreBUSYOrNotAndTheCommitReturnsNormallyCheck2(
            final int errorCode) throws Throwable {
        final TransactionExecutor ex;
        this.walExecutor();
        ex = new TransactionExecutor(failingCheckpoint(this.connection, errorCode));
        ex.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("INSERT INTO big VALUES (zeroblob(9437184))");
            }
            return null;
        });
        evaluate(executeWithoutThrow(() -> commitBlobs(ex, 8)));
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            final boolean assertionValue2Argument1;
            assertionValue2Argument1 = rs.getInt(1) >= 2;
            Assertions.assertTrue(assertionValue2Argument1, "체크포인트가 실패해도 커밋은 남는다");
        }
    }

    @Test
    @DisplayName("커밋 뒤 체크포인트의 런타임 예외도 삼키고 커밋은 정상 반환된다 — check 1")
    void runtimeExceptionsAtTheCheckpointAfterTheCommitAreAlsoSuppressedAndTheCommitReturnsNormallyCheck1()
            throws Throwable {
        final TransactionExecutor ex;
        final boolean assertionValue1;
        this.walExecutor();
        ex = new TransactionExecutor(
                failingCheckpoint(this.connection, () -> new InvalidPathException("x", "injected")));
        ex.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("INSERT INTO big VALUES (zeroblob(9437184))");
            }
            return null;
        });
        assertionValue1 = executeWithoutThrow(() -> commitBlobs(ex, 8));
        Assertions.assertTrue(assertionValue1, "Commit blobs should complete without an exception.");
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            evaluate(rs.getInt(1) >= 2);
        }
    }

    @Test
    @DisplayName("커밋 뒤 체크포인트의 런타임 예외도 삼키고 커밋은 정상 반환된다 — check 2")
    void runtimeExceptionsAtTheCheckpointAfterTheCommitAreAlsoSuppressedAndTheCommitReturnsNormallyCheck2()
            throws Throwable {
        final TransactionExecutor ex;
        this.walExecutor();
        ex = new TransactionExecutor(
                failingCheckpoint(this.connection, () -> new InvalidPathException("x", "injected")));
        ex.execute(c -> {
            try (Statement sx = c.createStatement()) {
                sx.execute("INSERT INTO big VALUES (zeroblob(9437184))");
            }
            return null;
        });
        evaluate(executeWithoutThrow(() -> commitBlobs(ex, 8)));
        try (Statement s = this.connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            final boolean assertionValue2Argument1;
            assertionValue2Argument1 = rs.getInt(1) >= 2;
            Assertions.assertTrue(assertionValue2Argument1, "Int should be at least 2.");
        }
    }

    @Test
    @DisplayName("커밋 뒤 체크포인트의 Error도 삼키고 커밋은 정상 반환된다 — check 1")
    void errorsInTheCheckpointAfterTheCommitAreAlsoSuppressedAndTheCommitReturnsNormallyCheck1() throws Throwable {
        for (final Supplier<Throwable> error : List.<Supplier<Throwable>>of(
                () -> new LinkageError("injected"),
                () -> new StackOverflowError("injected"),
                () -> new OutOfMemoryError("injected"))) {
            try (Connection c =
                    SqliteStorage.connect(this.dir.resolve("e" + System.nanoTime() + ".db"), Durability.POWER_LOSS)) {
                final TransactionExecutor ex;
                final boolean assertionValue1;
                try (Statement s = c.createStatement()) {
                    TransactionExecutor.configureWal(s);
                    s.execute("PRAGMA synchronous = NORMAL");
                    s.execute("CREATE TABLE big (b BLOB)");
                }
                ex = new TransactionExecutor(failingCheckpoint(c, error));
                commitBlobs(ex, 1, 9_437_184);
                assertionValue1 = executeWithoutThrow(() -> commitBlobs(ex, 7));
                Assertions.assertTrue(assertionValue1, "Commit blobs should complete without an exception.");
                try (Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
                    evaluate(rs.getInt(1));
                }
            }
        }
    }

    @Test
    @DisplayName("커밋 뒤 체크포인트의 Error도 삼키고 커밋은 정상 반환된다 — check 2")
    void errorsInTheCheckpointAfterTheCommitAreAlsoSuppressedAndTheCommitReturnsNormallyCheck2() throws Throwable {
        for (final Supplier<Throwable> error : List.<Supplier<Throwable>>of(
                () -> new LinkageError("injected"),
                () -> new StackOverflowError("injected"),
                () -> new OutOfMemoryError("injected"))) {
            try (Connection c =
                    SqliteStorage.connect(this.dir.resolve("e" + System.nanoTime() + ".db"), Durability.POWER_LOSS)) {
                final TransactionExecutor ex;
                try (Statement s = c.createStatement()) {
                    TransactionExecutor.configureWal(s);
                    s.execute("PRAGMA synchronous = NORMAL");
                    s.execute("CREATE TABLE big (b BLOB)");
                }
                ex = new TransactionExecutor(failingCheckpoint(c, error));
                commitBlobs(ex, 1, 9_437_184);
                evaluate(executeWithoutThrow(() -> commitBlobs(ex, 7)));
                try (Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
                    final int assertionValue2Argument2;
                    assertionValue2Argument2 = rs.getInt(1);
                    Assertions.assertEquals(8, assertionValue2Argument2, "Int should match the expected value.");
                }
            }
        }
    }

    @Test
    @DisplayName("예기치 못한 실패는 억제 없이 매번 스택 트레이스와 함께 WARNING으로 남는다")
    void unexpectedFailuresAreAlwaysLoggedAsWARNINGSWithStackTracesWithoutSuppression() throws Exception {
        final int every;
        final int checks;
        final List<LogRecord> warnings;
        every = 8;
        checks = 5;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            this.walExecutor();
            ex = new TransactionExecutor(failingCheckpoint(this.connection, () -> new LinkageError("injected")));
            commitBlobs(ex, 1, 9_437_184);
            Assertions.assertDoesNotThrow(
                    () -> commitBlobs(ex, every * checks - 1), "Commit blobs should complete without an exception.");
            Assertions.assertEquals(checks, warnings.size(), "Error는 억제 없이 확인마다 WARNING");
            for (final LogRecord w : warnings) {
                Assertions.assertInstanceOf(LinkageError.class, w.getThrown(), "스택 트레이스용 원인이 붙어야 한다");
            }
            try (Statement s = this.connection.createStatement();
                    ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
                Assertions.assertEquals(every * checks, rs.getInt(1), "Int should match the expected value.");
            }
        });
    }

    @Test
    @DisplayName("WAL 위치 확인 실패는 구분된 메시지로 간격 억제를 따라 남는다")
    void walLocationCheckFailuresAreLoggedAsSeparateMessagesFollowingIntervalSuppression() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            final String message;
            this.walExecutor();
            ex = new TransactionExecutor(
                    failingQuery(this.connection, "database_list", () -> new SQLException("injected")));
            Assertions.assertDoesNotThrow(
                    () -> commitBlobs(ex, every * 5), "Commit blobs should complete without an exception.");
            Assertions.assertEquals(1, warnings.size(), "Size should match the expected value.");
            message = warnings.get(0).getMessage();
            Assertions.assertTrue(message.contains("could not be located"), message);
            Assertions.assertFalse(message.contains("checkpoint did not complete"), message);
        });
    }

    @Test
    @DisplayName("WAL 크기 읽기의 런타임 예외도 크기 읽기 실패 메시지로 남는다")
    void runtimeExceptionsWhenReadingTheWALSizeAreAlsoLoggedAsSizeReadFailureMessages() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            this.walExecutor();
            ex = new TransactionExecutor(this.connection, path -> {
                throw new SecurityException("injected");
            });
            commitBlobs(ex, every);
            Assertions.assertEquals(1, warnings.size(), "Size should match the expected value.");
            Assertions.assertTrue(
                    warnings.get(0).getMessage().contains("size could not be read"),
                    "Message should contain the expected value.");
        });
    }

    @Test
    @DisplayName("되감기가 실패하면 낡은 기억 크기가 다음 시도를 막지 않는다 — initial attempt")
    void checkpointRunsBeforeRewindFailure() throws Exception {
        final int every;
        final AtomicBoolean failing;
        final AtomicInteger attempts;
        final TransactionExecutor ex;
        every = 8;
        this.walExecutor();
        failing = new AtomicBoolean(false);
        attempts = new AtomicInteger();
        ex = new TransactionExecutor(failingCheckpoint(this.connection, () -> {
            attempts.incrementAndGet();
            return failing.get() ? new IllegalStateException("injected") : null;
        }));
        commitBlobs(ex, 1, 20_971_520);
        commitBlobs(ex, every - 1);
        Assertions.assertEquals(1, attempts.get(), "Attempts should match the expected value.");
        failing.set(true);
        commitBlobs(ex, 1, 13_631_488);
        commitBlobs(ex, every - 1);
        try (Statement s = this.connection.createStatement()) {
            s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        commitBlobs(ex, 1, 10_485_760);
        commitBlobs(ex, every - 1);
    }

    @Test
    @DisplayName("되감기가 실패하면 낡은 기억 크기가 다음 시도를 막지 않는다 — failed attempt")
    void checkpointAttemptsFailedRewind() throws Exception {
        final int every;
        final AtomicBoolean failing;
        final AtomicInteger attempts;
        final TransactionExecutor ex;
        every = 8;
        this.walExecutor();
        failing = new AtomicBoolean(false);
        attempts = new AtomicInteger();
        ex = new TransactionExecutor(failingCheckpoint(this.connection, () -> {
            attempts.incrementAndGet();
            return failing.get() ? new IllegalStateException("injected") : null;
        }));
        commitBlobs(ex, 1, 20_971_520);
        commitBlobs(ex, every - 1);
        failing.set(true);
        commitBlobs(ex, 1, 13_631_488);
        commitBlobs(ex, every - 1);
        Assertions.assertEquals(2, attempts.get(), "Attempts should match the expected value.");
        try (Statement s = this.connection.createStatement()) {
            s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        commitBlobs(ex, 1, 10_485_760);
        commitBlobs(ex, every - 1);
    }

    @Test
    @DisplayName("되감기가 실패하면 낡은 기억 크기가 다음 시도를 막지 않는다 — retry")
    void checkpointRetriesAfterRewindFailure() throws Exception {
        final int every;
        final AtomicBoolean failing;
        final AtomicInteger attempts;
        final TransactionExecutor ex;
        every = 8;
        this.walExecutor();
        failing = new AtomicBoolean(false);
        attempts = new AtomicInteger();
        ex = new TransactionExecutor(failingCheckpoint(this.connection, () -> {
            attempts.incrementAndGet();
            return failing.get() ? new IllegalStateException("injected") : null;
        }));
        commitBlobs(ex, 1, 20_971_520);
        commitBlobs(ex, every - 1);
        failing.set(true);
        commitBlobs(ex, 1, 13_631_488);
        commitBlobs(ex, every - 1);
        try (Statement s = this.connection.createStatement()) {
            s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        commitBlobs(ex, 1, 10_485_760);
        commitBlobs(ex, every - 1);
        Assertions.assertEquals(3, attempts.get(), "실패한 되감기 뒤에는 다시 시도해야 한다");
    }

    @Test
    @DisplayName("체크포인트가 연속 실패해도 WARNING은 첫 실패에만 남고 성공하면 다시 남는다 — first warning")
    void consecutiveCheckpointFailuresLogFirstWarning() throws Exception {
        final List<LogRecord> warnings;
        final Logger logger;
        final Handler handler;
        warnings = new ArrayList<>();
        handler = new Handler() {

            @Override
            public void publish(final LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger = Logger.getLogger(TransactionExecutor.class.getName());
        logger.addHandler(handler);
        try {
            final AtomicBoolean failing;
            final TransactionExecutor ex;
            final int every;
            this.walExecutor();
            failing = new AtomicBoolean(true);
            ex = new TransactionExecutor(failingCheckpoint(
                    this.connection, () -> failing.get() ? new IllegalStateException("injected") : null));
            every = 8;
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every * 5);
            Assertions.assertEquals(1, warnings.size(), "연속 실패에는 WARNING이 한 번만");
            failing.set(false);
            commitBlobs(ex, every);
            commitBlobs(ex, 1, 11_534_336);
            failing.set(true);
            commitBlobs(ex, every * 3);
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    @DisplayName("체크포인트가 연속 실패해도 WARNING은 첫 실패에만 남고 성공하면 다시 남는다 — warning after success")
    void checkpointWarningResumesAfterSuccess() throws Exception {
        final List<LogRecord> warnings;
        final Logger logger;
        final Handler handler;
        warnings = new ArrayList<>();
        handler = new Handler() {

            @Override
            public void publish(final LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger = Logger.getLogger(TransactionExecutor.class.getName());
        logger.addHandler(handler);
        try {
            final AtomicBoolean failing;
            final TransactionExecutor ex;
            final int every;
            this.walExecutor();
            failing = new AtomicBoolean(true);
            ex = new TransactionExecutor(failingCheckpoint(
                    this.connection, () -> failing.get() ? new IllegalStateException("injected") : null));
            every = 8;
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every * 5);
            failing.set(false);
            commitBlobs(ex, every);
            commitBlobs(ex, 1, 11_534_336);
            failing.set(true);
            commitBlobs(ex, every * 3);
            Assertions.assertEquals(2, warnings.size(), "성공 뒤 첫 실패는 다시 WARNING");
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    @DisplayName("연속 실패는 N번째마다 횟수와 함께 WARNING으로 다시 남는다")
    void consecutiveFailuresAreRecordedAgainAsWARNINGSWithTheNumberOfOccurrencesEveryNthTime() throws Exception {
        final int n;
        final int every;
        final List<LogRecord> warnings;
        n = 64;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            this.walExecutor();
            ex = new TransactionExecutor(
                    failingCheckpoint(this.connection, () -> new IllegalStateException("injected")));
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1 + every * (n - 2));
            Assertions.assertEquals(1, warnings.size(), "n번째 전에는 첫 실패만");
            commitBlobs(ex, every);
            Assertions.assertEquals(2, warnings.size(), "Size should match the expected value.");
            Assertions.assertTrue(warnings.get(1).getMessage().contains(String.valueOf(n)), "실패 횟수가 메시지에 있다");
            commitBlobs(ex, every);
            Assertions.assertEquals(2, warnings.size(), "Size should match the expected value.");
        });
    }

    @Test
    @DisplayName("WAL이 한도 아래로 돌아오면 연속 실패 상태가 리셋된다")
    void whenTheWALReturnsBelowTheLimitTheConsecutiveFailureStatusIsReset() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            this.walExecutor();
            ex = new TransactionExecutor(
                    failingCheckpoint(this.connection, () -> new IllegalStateException("injected")));
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1);
            Assertions.assertEquals(1, warnings.size(), "Size should match the expected value.");
            try (Statement s = this.connection.createStatement()) {
                s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            commitBlobs(ex, every);
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1);
            Assertions.assertEquals(2, warnings.size(), "리셋 뒤 첫 실패는 다시 WARNING");
        });
    }

    @Test
    @DisplayName("되감기 성공 뒤 크기 읽기 실패는 체크포인트 실패로 기록되지 않는다")
    void afterASuccessfulRewindAFailureToReadTheSizeIsNotRecordedAsACheckpointFailure() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final AtomicInteger reads;
            final AtomicBoolean failing;
            final TransactionExecutor ex;
            this.walExecutor();
            reads = new AtomicInteger();
            failing = new AtomicBoolean(false);
            ex = new TransactionExecutor(
                    failingCheckpoint(
                            this.connection, () -> failing.get() ? new IllegalStateException("injected") : null),
                    path -> {
                        if (reads.incrementAndGet() == 2) {
                            throw new SecurityException("injected");
                        } else {
                            return Files.size(path);
                        }
                    });
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1);
            Assertions.assertEquals(1, warnings.size(), "크기 읽기 실패는 따로 한 번 로그한다");
            Assertions.assertFalse(
                    warnings.get(0).getMessage().contains("checkpoint did not complete"), "체크포인트 실패 메시지가 아니다");
            failing.set(true);
            commitBlobs(ex, every);
            Assertions.assertEquals(2, warnings.size(), "Size should match the expected value.");
            Assertions.assertTrue(
                    warnings.get(1).getMessage().contains("checkpoint did not complete"),
                    "Message should contain the expected value.");
        });
    }

    @Test
    @DisplayName("WAL 크기 읽기 실패는 구분된 메시지로 한 번만 WARNING으로 남는다")
    void walSizeReadFailuresAreOnlyLoggedAsWARNINGSOnceAsSeparateMessages() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final TransactionExecutor ex;
            final String message;
            this.walExecutor();
            ex = new TransactionExecutor(this.connection, path -> {
                throw new IOException("injected");
            });
            commitBlobs(ex, every * 5);
            Assertions.assertEquals(1, warnings.size(), "연속 읽기 실패에는 WARNING이 한 번만");
            message = warnings.get(0).getMessage();
            Assertions.assertTrue(message.contains("size could not be read"), message);
            Assertions.assertFalse(message.contains("checkpoint did not complete"), "체크포인트 실패 메시지가 아니다");
        });
    }

    @Test
    @DisplayName("WAL 크기 읽기 실패는 연속 실패 횟수를 리셋하지 않는다")
    void aWALSizeReadFailureDoesNotResetTheConsecutiveFailureCount() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final AtomicBoolean sizeReadFails;
            final TransactionExecutor ex;
            this.walExecutor();
            sizeReadFails = new AtomicBoolean(false);
            ex = new TransactionExecutor(
                    failingCheckpoint(this.connection, () -> new IllegalStateException("injected")), path -> {
                        if (sizeReadFails.get()) {
                            throw new IOException("injected");
                        } else {
                            return Files.size(path);
                        }
                    });
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1);
            Assertions.assertEquals(1, warnings.size(), "Size should match the expected value.");
            sizeReadFails.set(true);
            commitBlobs(ex, every);
            sizeReadFails.set(false);
            commitBlobs(ex, every);
            Assertions.assertEquals(1, warnings.size(), "읽기 실패가 횟수를 지웠다면 여기서 다시 WARNING이 난다");
        });
    }

    @Test
    @DisplayName("WAL 파일이 없으면 크기 0으로 보고 경고도 실패 횟수 증가도 없다")
    void ifThereIsNoWALFileItIsReportedAsSize0AndThereIsNoWarningOrIncreaseInTheNumberOfFailures() throws Exception {
        final int every;
        final List<LogRecord> warnings;
        every = 8;
        warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            final AtomicBoolean missing;
            final TransactionExecutor ex;
            this.walExecutor();
            missing = new AtomicBoolean(true);
            ex = new TransactionExecutor(
                    failingCheckpoint(this.connection, () -> new IllegalStateException("injected")), path -> {
                        if (missing.get()) {
                            throw new NoSuchFileException(path.toString());
                        } else {
                            return Files.size(path);
                        }
                    });
            commitBlobs(ex, every * 3);
            Assertions.assertEquals(0, warnings.size(), "파일 없음은 경고가 아니다");
            missing.set(false);
            commitBlobs(ex, 1, 9_437_184);
            commitBlobs(ex, every - 1);
            Assertions.assertEquals(1, warnings.size(), "파일 없음이 횟수를 올리지 않았으므로 첫 실패로 WARNING");
        });
    }

    @Test
    @DisplayName("파일 없는 데이터베이스에서는 WAL 위치를 한 번만 확인한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The proxy delegates to memory, whose try-with-resources owns and closes the connection.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void inAFilelessDatabaseTheWALLocationIsCheckedOnlyOnce() throws Exception {
        final int every;
        every = 8;
        try (Connection memory = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            final AtomicInteger lookups;
            final TransactionExecutor ex;
            final Connection counting;
            lookups = new AtomicInteger();
            counting = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class[] {Connection.class}, (proxy, method, args) -> {
                        final Object result;
                        final Statement st;
                        result = invoke(method, memory, args);
                        if (!method.getName().equals("createStatement")) {
                            return result;
                        }
                        st = (Statement) result;
                        return Proxy.newProxyInstance(
                                Statement.class.getClassLoader(), new Class[] {Statement.class}, (p, m, a) -> {
                                    if (m.getName().equals("executeQuery")
                                            && ((String) a[0]).contains("database_list")) {
                                        lookups.incrementAndGet();
                                    }
                                    return invoke(m, st, a);
                                });
                    });
            try (Statement s = memory.createStatement()) {
                s.execute("CREATE TABLE t (v INTEGER)");
            }
            ex = new TransactionExecutor(counting);
            for (int i = 0; i < every * 5; i++) {
                ex.execute(c -> {
                    try (Statement s = c.createStatement()) {
                        s.execute("INSERT INTO t VALUES (1)");
                    }
                    return null;
                });
            }
            Assertions.assertEquals(1, lookups.get(), "Lookups should match the expected value.");
        }
    }

    private static void withWarnings(
            final List<LogRecord> warnings, final TransactionExecutorTest.ThrowingRunnable body) throws Exception {
        final Logger logger;
        final Handler handler;
        handler = new Handler() {

            @Override
            public void publish(final LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger = Logger.getLogger(TransactionExecutor.class.getName());
        logger.addHandler(handler);
        try {
            body.run();
        } finally {
            logger.removeHandler(handler);
        }
    }

    private static Connection failingCheckpoint(final Connection real, final int errorCode) {
        return failingCheckpoint(real, () -> new SQLException("injected", null, errorCode));
    }

    private static Connection failingCheckpoint(final Connection real, final Supplier<Throwable> failure) {
        return failingQuery(real, "wal_checkpoint", failure);
    }

    private static Connection failingQuery(
            final Connection real, final String fragment, final Supplier<Throwable> failure) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class[] {Connection.class}, (proxy, method, args) -> {
                    final Object result;
                    final Statement st;
                    result = invoke(method, real, args);
                    if (!method.getName().equals("createStatement")) {
                        return result;
                    }
                    st = (Statement) result;
                    return Proxy.newProxyInstance(
                            Statement.class.getClassLoader(), new Class[] {Statement.class}, (p, m, a) -> {
                                if ((m.getName().equals("executeQuery")
                                                || m.getName().equals("execute"))
                                        && a != null
                                        && a.length > 0
                                        && a[0] instanceof String
                                        && ((String) a[0]).contains(fragment)) {
                                    final Throwable t;
                                    t = failure.get();
                                    if (t != null) {
                                        throw t;
                                    }
                                }
                                return invoke(m, st, a);
                            });
                });
    }

    private static Object invoke(final Method method, final Object target, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private TransactionExecutor walExecutor() throws SQLException {
        try (Statement s = this.connection.createStatement()) {
            TransactionExecutor.configureWal(s);
            s.execute("PRAGMA synchronous = NORMAL");
            s.execute("CREATE TABLE big (b BLOB)");
        }
        return this.executor();
    }

    private static void commitBlobs(final TransactionExecutor executor, final int commits) {
        commitBlobs(executor, commits, 65_536);
    }

    private static void commitBlobs(final TransactionExecutor executor, final int commits, final int blobBytes) {
        for (int i = 0; i < commits; i++) {
            executor.execute(c -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO big VALUES (zeroblob(" + blobBytes + "))");
                }
                return null;
            });
        }
    }

    private long walFileSize() throws Exception {
        return Files.size(this.dir.resolve("t.db-wal"));
    }

    @Test
    @DisplayName("한도 아래에서는 되감지 않아 WAL 파일이 쓴 만큼 자란다 — WAL grows")
    void walBelowLimitGrowsWithWrites() throws Exception {
        final TransactionExecutor executor;
        final long size;
        executor = this.walExecutor();
        commitBlobs(executor, 80);
        size = this.walFileSize();
        Assertions.assertTrue(size >= 5_242_880L, "WAL 파일이 " + size + "바이트다. 5MB를 썼으니 그보다 커야 한다. 한도 아래에서 되감았다");
    }

    @Test
    @DisplayName("한도 아래에서는 되감지 않아 WAL 파일이 쓴 만큼 자란다 — below threshold")
    void walBelowLimitStaysBelowCheckpointThreshold() throws Exception {
        final TransactionExecutor executor;
        final long size;
        executor = this.walExecutor();
        commitBlobs(executor, 80);
        size = this.walFileSize();
        Assertions.assertTrue(size < 8_388_608L, "WAL 파일이 " + size + "바이트다. 한도 아래까지만 쓰려 했다");
    }

    @Test
    @DisplayName("한도를 한참 넘게 써도 WAL 파일이 한도와 확인 주기 분량을 넘지 않는다 — check 1")
    void
            evenIfTheLimitIsExceededByASignificantMarginTheWALFileDoesNotSurpassTheLimitAndTheAmountCorrespondingToTheCheckpointIntervalCheck1()
                    throws Exception {
        final int blobBytes;
        final TransactionExecutor executor;
        final long afterOne;
        final long perCommit;
        final boolean assertionValue1Argument1;
        final String assertionValue1Argument2;
        final long limit;
        final long writeBytes;
        final long rewinds;
        blobBytes = 16_384;
        executor = this.walExecutor();
        commitBlobs(executor, 1, blobBytes);
        afterOne = this.walFileSize();
        commitBlobs(executor, 1, blobBytes);
        perCommit = this.walFileSize() - afterOne;
        assertionValue1Argument1 = perCommit >= blobBytes;
        assertionValue1Argument2 = "커밋 하나가 WAL을 " + perCommit + "바이트 늘렸다";
        Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
        limit = 8_388_608L;
        writeBytes = 3L * limit;
        rewinds = writeBytes / limit + 1L;
        evaluate(limit + rewinds * 8L * perCommit);
        commitBlobs(executor, (int) (writeBytes / perCommit), blobBytes);
        evaluate(this.walFileSize());
    }

    @Test
    @DisplayName("한도를 한참 넘게 써도 WAL 파일이 한도와 확인 주기 분량을 넘지 않는다 — check 2")
    void
            evenIfTheLimitIsExceededByASignificantMarginTheWALFileDoesNotSurpassTheLimitAndTheAmountCorrespondingToTheCheckpointIntervalCheck2()
                    throws Exception {
        final int blobBytes;
        final TransactionExecutor executor;
        final long afterOne;
        final long perCommit;
        final long limit;
        final long writeBytes;
        final long rewinds;
        final long bound;
        final long size;
        final boolean assertionValue2Argument1;
        final String assertionValue2Argument2;
        blobBytes = 16_384;
        executor = this.walExecutor();
        commitBlobs(executor, 1, blobBytes);
        afterOne = this.walFileSize();
        commitBlobs(executor, 1, blobBytes);
        perCommit = this.walFileSize() - afterOne;
        limit = 8_388_608L;
        writeBytes = 3L * limit;
        rewinds = writeBytes / limit + 1L;
        bound = limit + rewinds * 8L * perCommit;
        commitBlobs(executor, (int) (writeBytes / perCommit), blobBytes);
        size = this.walFileSize();
        assertionValue2Argument1 = size <= bound;
        assertionValue2Argument2 = "WAL 파일이 " + size + "바이트다. 한도와 확인 주기 분량(" + bound + ")을 넘었다. 한도를 넘고도 늦게 되감았다";
        Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static boolean executeWithoutThrow(final Executable action) throws Throwable {
        action.execute();
        return true;
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {

        void run() throws Exception;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
