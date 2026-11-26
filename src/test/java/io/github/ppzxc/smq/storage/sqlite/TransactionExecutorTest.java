package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.SmqStorageException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionExecutorTest {

    private static final int SQLITE_CONSTRAINT = 19;
    private static final long MB = 1024L * 1024;

    private static final int BLOB_BYTES = 64 * 1024;

    @TempDir
    Path dir;

    private Connection connection;

    @Test
    void 체크포인트_오류를_기록하지_못해도_커밋_결과는_실패로_바뀌지_않는다() {
        RuntimeException checkpointFailure = new IllegalStateException("checkpoint failed");
        RuntimeException loggingFailure = new IllegalStateException("logger failed");

        assertDoesNotThrow(() -> TransactionExecutor.logCheckpointFailure(checkpointFailure, () -> {
            throw loggingFailure;
        }));
        assertSame(loggingFailure, checkpointFailure.getSuppressed()[0]);
    }

    @Test
    void 로그가_원래_오류를_다시_던져도_자기_자신을_억제하지_않는다() {
        RuntimeException failure = new IllegalStateException("checkpoint failed");
        assertDoesNotThrow(() -> TransactionExecutor.logCheckpointFailure(failure, () -> {
            throw failure;
        }));
        assertEquals(0, failure.getSuppressed().length);
    }

    @BeforeEach
    void setUp() throws Exception {
        connection = SqliteStorage.connect(dir.resolve("t.db"), Durability.POWER_LOSS);
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE t (v INTEGER)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    private TransactionExecutor executor() {
        return new TransactionExecutor(connection);
    }

    private int count() throws SQLException {
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM t")) {
            return rs.getInt(1);
        }
    }

    @Test
    void 실패한_트랜잭션은_ROLLBACK되고_다음_트랜잭션이_정상적으로_시작된다() throws Exception {
        TransactionExecutor ex = executor();
        assertThrows(
                SmqStorageException.class,
                () -> ex.execute(c -> {
                    try (Statement s = c.createStatement()) {
                        s.execute("INSERT INTO t VALUES (1)");
                    }
                    throw new SQLException("boom", null, SQLITE_CONSTRAINT);
                }));
        assertEquals(0, count());
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        assertEquals(1, count());
    }

    @Test
    void 오류로_실패한_트랜잭션도_롤백하고_다음_연산을_허용한다() throws Exception {
        TransactionExecutor ex = executor();
        AssertionError failure = new AssertionError("injected");
        assertSame(
                failure,
                assertThrows(
                        AssertionError.class,
                        () -> ex.execute(c -> {
                            try (Statement s = c.createStatement()) {
                                s.execute("INSERT INTO t VALUES (1)");
                            }
                            throw failure;
                        })));
        assertEquals(0, count());
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (2)");
            }
            return null;
        });
        assertEquals(1, count());
    }

    @Test
    void 롤백이_실패하면_이후_모든_연산이_처음_원인으로_실패한다() {
        SQLException rollbackFailure = new SQLException("rollback failed");
        TransactionExecutor ex = new TransactionExecutor(failingQuery(connection, "ROLLBACK", () -> rollbackFailure));
        SQLException original = new SQLException("body failed");
        assertThrows(
                SmqStorageException.class,
                () -> ex.execute(c -> {
                    throw original;
                }));
        assertSame(
                original,
                assertThrows(SmqStorageException.class, () -> ex.execute(c -> 1))
                        .getCause());
        assertSame(
                original,
                assertThrows(SmqStorageException.class, () -> ex.anyOrUncertain(c -> true))
                        .getCause());
        assertSame(
                original,
                assertThrows(SmqStorageException.class, () -> ex.executeOutsideTransaction(() -> 1))
                        .getCause());
        ex.close();
    }

    @Test
    void WAL이_한도를_넘으면_되감겨_WAL이_계속_자라지_않는다() throws Exception {
        try (Statement s = connection.createStatement()) {
            TransactionExecutor.configureWal(s);
        }
        TransactionExecutor executor = executor();
        executor.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE big (b BLOB)");
                s.execute("INSERT INTO big VALUES (zeroblob(20000000))");
            }
            return null;
        });
        assertTrue(Files.size(dir.resolve("t.db-wal")) > 16L * 1024 * 1024);

        for (int i = 0; i < 16; i++) {
            executor.execute(c -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO t VALUES (2)");
                }
                return null;
            });
        }

        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(PASSIVE)")) {
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            assertTrue(rs.getInt(2) < 100, "WAL에 프레임이 " + rs.getInt(2) + "개 남았다. 되감지 않았다");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 517, 10})
    void 커밋_뒤_체크포인트의_SQL_오류는_BUSY든_아니든_삼키고_커밋은_정상_반환된다(int errorCode) throws Exception {
        walExecutor();
        TransactionExecutor ex = new TransactionExecutor(failingCheckpoint(connection, errorCode));
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO big VALUES (zeroblob(" + (9 * MB) + "))");
            }
            return null;
        });

        assertDoesNotThrow(() -> commitBlobs(ex, TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS));
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            assertTrue(rs.getInt(1) >= 2, "체크포인트가 실패해도 커밋은 남는다");
        }
    }

    @Test
    void 커밋_뒤_체크포인트의_런타임_예외도_삼키고_커밋은_정상_반환된다() throws Exception {
        walExecutor();
        TransactionExecutor ex = new TransactionExecutor(
                failingCheckpoint(connection, () -> new java.nio.file.InvalidPathException("x", "injected")));
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO big VALUES (zeroblob(" + (9 * MB) + "))");
            }
            return null;
        });
        assertDoesNotThrow(() -> commitBlobs(ex, TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS));
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            assertTrue(rs.getInt(1) >= 2);
        }
    }

    @Test
    void 커밋_뒤_체크포인트의_Error도_삼키고_커밋은_정상_반환된다() throws Exception {
        List<Supplier<Throwable>> errors = List.of(
                () -> new LinkageError("injected"),
                () -> new StackOverflowError("injected"),
                () -> new OutOfMemoryError("injected"));
        for (Supplier<Throwable> error : errors) {
            try (Connection c =
                    SqliteStorage.connect(dir.resolve("e" + System.nanoTime() + ".db"), Durability.POWER_LOSS)) {
                try (Statement s = c.createStatement()) {
                    TransactionExecutor.configureWal(s);
                    s.execute("PRAGMA synchronous = NORMAL");
                    s.execute("CREATE TABLE big (b BLOB)");
                }
                TransactionExecutor ex = new TransactionExecutor(failingCheckpoint(c, error));
                commitBlobs(ex, 1, (int) (9 * MB));
                assertDoesNotThrow(() -> commitBlobs(ex, TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS - 1));
                try (Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
                    assertEquals(TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS, rs.getInt(1));
                }
            }
        }
    }

    @Test
    void 예기치_못한_실패는_억제_없이_매번_스택_트레이스와_함께_WARNING으로_남는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        int checks = 5;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex =
                    new TransactionExecutor(failingCheckpoint(connection, () -> new LinkageError("injected")));
            commitBlobs(ex, 1, (int) (9 * MB));
            assertDoesNotThrow(() -> commitBlobs(ex, every * checks - 1));
            assertEquals(checks, warnings.size(), "Error는 억제 없이 확인마다 WARNING");
            for (LogRecord w : warnings) {
                assertInstanceOf(LinkageError.class, w.getThrown(), "스택 트레이스용 원인이 붙어야 한다");
            }
            try (Statement s = connection.createStatement();
                    ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
                assertEquals(every * checks, rs.getInt(1));
            }
        });
    }

    @Test
    void WAL_위치_확인_실패는_구분된_메시지로_간격_억제를_따라_남는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex = new TransactionExecutor(
                    failingQuery(connection, "database_list", () -> new SQLException("injected")));
            assertDoesNotThrow(() -> commitBlobs(ex, every * 5));
            assertEquals(1, warnings.size());
            String message = warnings.get(0).getMessage();
            assertTrue(message.contains("could not be located"), message);
            assertFalse(message.contains("checkpoint did not complete"), message);
        });
    }

    @Test
    void WAL_크기_읽기의_런타임_예외도_크기_읽기_실패_메시지로_남는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex = new TransactionExecutor(connection, path -> {
                throw new SecurityException("injected");
            });
            commitBlobs(ex, every);
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).getMessage().contains("size could not be read"));
        });
    }

    @Test
    void 되감기가_실패하면_낡은_기억_크기가_다음_시도를_막지_않는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        walExecutor();
        AtomicBoolean failing = new AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        TransactionExecutor ex = new TransactionExecutor(failingCheckpoint(connection, () -> {
            attempts.incrementAndGet();
            return failing.get() ? new IllegalStateException("injected") : null;
        }));
        commitBlobs(ex, 1, (int) (20 * MB));
        commitBlobs(ex, every - 1);
        assertEquals(1, attempts.get());
        failing.set(true);
        commitBlobs(ex, 1, (int) (13 * MB));
        commitBlobs(ex, every - 1);
        assertEquals(2, attempts.get());
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        commitBlobs(ex, 1, (int) (10 * MB));
        commitBlobs(ex, every - 1);
        assertEquals(3, attempts.get(), "실패한 되감기 뒤에는 다시 시도해야 한다");
    }

    @Test
    void 체크포인트가_연속_실패해도_WARNING은_첫_실패에만_남고_성공하면_다시_남는다() throws Exception {
        List<LogRecord> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        Logger logger = Logger.getLogger(TransactionExecutor.class.getName());
        logger.addHandler(handler);
        try {
            walExecutor();
            AtomicBoolean failing = new AtomicBoolean(true);
            TransactionExecutor ex = new TransactionExecutor(
                    failingCheckpoint(connection, () -> failing.get() ? new IllegalStateException("injected") : null));
            int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every * 5);
            assertEquals(1, warnings.size(), "연속 실패에는 WARNING이 한 번만");

            failing.set(false);
            commitBlobs(ex, every);
            commitBlobs(ex, 1, (int) (11 * MB));
            failing.set(true);
            commitBlobs(ex, every * 3);
            assertEquals(2, warnings.size(), "성공 뒤 첫 실패는 다시 WARNING");
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void 연속_실패는_N번째마다_횟수와_함께_WARNING으로_다시_남는다() throws Exception {
        int n = TransactionExecutor.CHECKPOINT_FAILURE_LOG_EVERY;
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex =
                    new TransactionExecutor(failingCheckpoint(connection, () -> new IllegalStateException("injected")));
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1 + every * (n - 2));
            assertEquals(1, warnings.size(), "n번째 전에는 첫 실패만");
            commitBlobs(ex, every);
            assertEquals(2, warnings.size());
            assertTrue(warnings.get(1).getMessage().contains(String.valueOf(n)), "실패 횟수가 메시지에 있다");
            commitBlobs(ex, every);
            assertEquals(2, warnings.size());
        });
    }

    @Test
    void WAL이_한도_아래로_돌아오면_연속_실패_상태가_리셋된다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex =
                    new TransactionExecutor(failingCheckpoint(connection, () -> new IllegalStateException("injected")));
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(1, warnings.size());
            try (Statement s = connection.createStatement()) {
                s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            commitBlobs(ex, every);
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(2, warnings.size(), "리셋 뒤 첫 실패는 다시 WARNING");
        });
    }

    @Test
    void 되감기_성공_뒤_크기_읽기_실패는_체크포인트_실패로_기록되지_않는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
            AtomicBoolean failing = new AtomicBoolean(false);
            TransactionExecutor ex = new TransactionExecutor(
                    failingCheckpoint(connection, () -> failing.get() ? new IllegalStateException("injected") : null),
                    path -> {
                        if (reads.incrementAndGet() == 2) {
                            throw new SecurityException("injected");
                        }
                        return Files.size(path);
                    });
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(1, warnings.size(), "크기 읽기 실패는 따로 한 번 로그한다");
            assertFalse(warnings.get(0).getMessage().contains("checkpoint did not complete"), "체크포인트 실패 메시지가 아니다");

            failing.set(true);
            commitBlobs(ex, every);
            assertEquals(2, warnings.size());
            assertTrue(warnings.get(1).getMessage().contains("checkpoint did not complete"));
        });
    }

    @Test
    void WAL_크기_읽기_실패는_구분된_메시지로_한_번만_WARNING으로_남는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex = new TransactionExecutor(connection, path -> {
                throw new java.io.IOException("injected");
            });
            commitBlobs(ex, every * 5);
            assertEquals(1, warnings.size(), "연속 읽기 실패에는 WARNING이 한 번만");
            String message = warnings.get(0).getMessage();
            assertTrue(message.contains("size could not be read"), message);
            assertFalse(message.contains("checkpoint did not complete"), "체크포인트 실패 메시지가 아니다");
        });
    }

    @Test
    void WAL_크기_읽기_실패는_연속_실패_횟수를_리셋하지_않는다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            AtomicBoolean sizeReadFails = new AtomicBoolean(false);
            TransactionExecutor ex = new TransactionExecutor(
                    failingCheckpoint(connection, () -> new IllegalStateException("injected")), path -> {
                        if (sizeReadFails.get()) {
                            throw new java.io.IOException("injected");
                        }
                        return Files.size(path);
                    });
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(1, warnings.size());
            sizeReadFails.set(true);
            commitBlobs(ex, every);
            sizeReadFails.set(false);
            commitBlobs(ex, every);
            assertEquals(1, warnings.size(), "읽기 실패가 횟수를 지웠다면 여기서 다시 WARNING이 난다");
        });
    }

    @Test
    void WAL_파일이_없으면_크기_0으로_보고_경고도_실패_횟수_증가도_없다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            AtomicBoolean missing = new AtomicBoolean(true);
            TransactionExecutor ex = new TransactionExecutor(
                    failingCheckpoint(connection, () -> new IllegalStateException("injected")), path -> {
                        if (missing.get()) {
                            throw new java.nio.file.NoSuchFileException(path.toString());
                        }
                        return Files.size(path);
                    });
            commitBlobs(ex, every * 3);
            assertEquals(0, warnings.size(), "파일 없음은 경고가 아니다");
            missing.set(false);
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(1, warnings.size(), "파일 없음이 횟수를 올리지 않았으므로 첫 실패로 WARNING");
        });
    }

    @Test
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The proxy delegates to memory, whose try-with-resources owns and closes the connection.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void 파일_없는_데이터베이스에서는_WAL_위치를_한_번만_확인한다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        try (Connection memory = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")) {
            java.util.concurrent.atomic.AtomicInteger lookups = new java.util.concurrent.atomic.AtomicInteger();
            Connection counting = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                        Object result = invoke(method, memory, args);
                        if (!method.getName().equals("createStatement")) {
                            return result;
                        }
                        Statement st = (Statement) result;
                        return Proxy.newProxyInstance(
                                Statement.class.getClassLoader(), new Class<?>[] {Statement.class}, (p, m, a) -> {
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
            TransactionExecutor ex = new TransactionExecutor(counting);
            for (int i = 0; i < every * 5; i++) {
                ex.execute(c -> {
                    try (Statement s = c.createStatement()) {
                        s.execute("INSERT INTO t VALUES (1)");
                    }
                    return null;
                });
            }
            assertEquals(1, lookups.get());
        }
    }

    private static void withWarnings(List<LogRecord> warnings, ThrowingRunnable body) throws Exception {
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        Logger logger = Logger.getLogger(TransactionExecutor.class.getName());
        logger.addHandler(handler);
        try {
            body.run();
        } finally {
            logger.removeHandler(handler);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static Connection failingCheckpoint(Connection real, int errorCode) {
        return failingCheckpoint(real, () -> new SQLException("injected", null, errorCode));
    }

    private static Connection failingCheckpoint(Connection real, Supplier<Throwable> failure) {
        return failingQuery(real, "wal_checkpoint", failure);
    }

    private static Connection failingQuery(Connection real, String fragment, Supplier<Throwable> failure) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    Object result = invoke(method, real, args);
                    if (!method.getName().equals("createStatement")) {
                        return result;
                    }
                    Statement st = (Statement) result;
                    return Proxy.newProxyInstance(
                            Statement.class.getClassLoader(), new Class<?>[] {Statement.class}, (p, m, a) -> {
                                if ((m.getName().equals("executeQuery")
                                                || m.getName().equals("execute"))
                                        && a != null
                                        && a.length > 0
                                        && a[0] instanceof String
                                        && ((String) a[0]).contains(fragment)) {
                                    Throwable t = failure.get();
                                    if (t != null) {
                                        throw t;
                                    }
                                }
                                return invoke(m, st, a);
                            });
                });
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private TransactionExecutor walExecutor() throws SQLException {
        try (Statement s = connection.createStatement()) {
            TransactionExecutor.configureWal(s);
            s.execute("PRAGMA synchronous = NORMAL");
            s.execute("CREATE TABLE big (b BLOB)");
        }
        return executor();
    }

    private static void commitBlobs(TransactionExecutor executor, int commits) {
        commitBlobs(executor, commits, BLOB_BYTES);
    }

    private static void commitBlobs(TransactionExecutor executor, int commits, int blobBytes) {
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
        return Files.size(dir.resolve("t.db-wal"));
    }

    @Test
    void 한도_아래에서는_되감지_않아_WAL_파일이_쓴_만큼_자란다() throws Exception {
        TransactionExecutor executor = walExecutor();

        commitBlobs(executor, 80);

        long size = walFileSize();
        assertTrue(size >= 5 * MB, "WAL 파일이 " + size + "바이트다. 5MB를 썼으니 그보다 커야 한다. 한도 아래에서 되감았다");
        assertTrue(size < TransactionExecutor.CHECKPOINT_WAL_BYTES, "WAL 파일이 " + size + "바이트다. 한도 아래까지만 쓰려 했다");
    }

    @Test
    void 한도를_한참_넘게_써도_WAL_파일이_한도와_확인_주기_분량을_넘지_않는다() throws Exception {
        int blobBytes = 16 * 1024;
        TransactionExecutor executor = walExecutor();
        commitBlobs(executor, 1, blobBytes);
        long afterOne = walFileSize();
        commitBlobs(executor, 1, blobBytes);
        long perCommit = walFileSize() - afterOne;
        assertTrue(perCommit >= blobBytes, "커밋 하나가 WAL을 " + perCommit + "바이트 늘렸다");
        long limit = TransactionExecutor.CHECKPOINT_WAL_BYTES;
        long writeBytes = 3 * limit;
        long rewinds = writeBytes / limit + 1;
        long bound = limit + rewinds * TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS * perCommit;

        commitBlobs(executor, (int) (writeBytes / perCommit), blobBytes);

        long size = walFileSize();
        assertTrue(size <= bound, "WAL 파일이 " + size + "바이트다. 한도와 확인 주기 분량(" + bound + ")을 넘었다. 한도를 넘고도 늦게 되감았다");
    }
}
