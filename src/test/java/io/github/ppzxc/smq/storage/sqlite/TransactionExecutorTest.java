package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
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

/** 트랜잭션 실행 모듈. */
class TransactionExecutorTest {

    private static final int SQLITE_CONSTRAINT = 19;
    private static final long MB = 1024L * 1024;
    /** 커밋 하나가 쓰는 BLOB 크기. 확인 주기(커밋 8번) 동안 쓰는 양이 한도(8MB)보다 한참 작다. */
    private static final int BLOB_BYTES = 64 * 1024;

    @TempDir
    Path dir;

    private Connection connection;

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

    /** WAL이 한도(8MB)를 넘으면 커밋 몇 번 뒤에 되감는다(ADR-0017). 되감은 뒤의 쓰기는 WAL의 앞에서 다시 시작한다. */
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

        // wal_checkpoint의 둘째 열은 WAL에 남은 프레임 수다. 되감지 않았다면 20MB(5000프레임 이상)가 그대로 있다.
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(PASSIVE)")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(2) < 100, "WAL에 프레임이 " + rs.getInt(2) + "개 남았다. 되감지 않았다");
        }
    }

    /**
     * 커밋 뒤 체크포인트의 SQL 오류는 BUSY(5)든 아니든 같은 규칙이다. 이미 커밋된 연산의 결과는 그대로
     * 돌려주고 오류는 삼킨다. 되감기는 다음 정규 확인이 다시 시도한다.
     */
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
        // 확인 주기(커밋 8번)가 돌아오면 WAL이 한도를 넘었으므로 체크포인트가 실패하지만 예외 없이 돌아온다.
        assertDoesNotThrow(() -> commitBlobs(ex, TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS));
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            assertTrue(rs.getInt(1) >= 2, "체크포인트가 실패해도 커밋은 남는다");
        }
    }

    /** 체크포인트의 RuntimeException(예: InvalidPathException)도 커밋을 실패로 올리지 않는다. */
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

    /** 커밋 뒤 체크포인트에서 Error가 나도 이미 커밋된 결과는 그대로 돌려주고 롤백하지 않는다. */
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

    /** WAL 위치 확인 실패는 체크포인트 실패가 아니다. 구분된 메시지로 남고, 연속 실패 간격 억제를 따른다. */
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

    /** 크기 읽기 중 런타임 예외(예: SecurityException)도 크기 읽기 실패 메시지로 남는다. */
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

    /**
     * 되감기가 실패하면 기억한 크기를 한도 아래로 되돌려, 파일이 이전 기억값 이하로 돌아와도 다음 확인에서 다시 시도한다.
     * 낡은 기억값이 남으면 "이미 되감은 구간"으로 보고 시도를 건너뛰며 연속 실패 횟수도 지운다.
     */
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
        commitBlobs(ex, every - 1); // 되감기 성공. 기억값은 12MB로 잘린다.
        assertEquals(1, attempts.get());
        failing.set(true);
        commitBlobs(ex, 1, (int) (13 * MB));
        commitBlobs(ex, every - 1); // WAL이 12MB를 넘었으므로 시도하고 실패한다.
        assertEquals(2, attempts.get());
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        commitBlobs(ex, 1, (int) (10 * MB)); // 한도(8MB)와 낡은 기억값(12MB) 사이
        commitBlobs(ex, every - 1);
        assertEquals(3, attempts.get(), "실패한 되감기 뒤에는 다시 시도해야 한다");
    }

    /** 연속 실패 중에는 첫 실패만 WARNING이고, 체크포인트가 성공하면 상태가 리셋돼 다음 실패가 다시 WARNING이다. */
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
            commitBlobs(ex, every); // 성공해 되감고 상태를 리셋한다.
            commitBlobs(ex, 1, (int) (11 * MB)); // 되감은 구간을 다시 넘게 채운다.
            failing.set(true);
            commitBlobs(ex, every * 3);
            assertEquals(2, warnings.size(), "성공 뒤 첫 실패는 다시 WARNING");
        } finally {
            logger.removeHandler(handler);
        }
    }

    /** 연속 실패가 길어져도 운영자가 알 수 있게 64번째(CHECKPOINT_FAILURE_LOG_EVERY) 연속 실패마다 횟수와 함께 WARNING을 다시 남긴다. */
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
            commitBlobs(ex, every - 1 + every * (n - 2)); // 연속 실패 n-1번
            assertEquals(1, warnings.size(), "n번째 전에는 첫 실패만");
            commitBlobs(ex, every); // n번째 연속 실패
            assertEquals(2, warnings.size());
            assertTrue(warnings.get(1).getMessage().contains(String.valueOf(n)), "실패 횟수가 메시지에 있다");
            commitBlobs(ex, every); // n+1번째
            assertEquals(2, warnings.size());
        });
    }

    /** WAL이 한도 아래로 돌아와 확인이 통과하면 연속 실패 상태가 지워져 다음 실패가 다시 첫 실패로 WARNING이 된다. */
    @Test
    void WAL이_한도_아래로_돌아오면_연속_실패_상태가_리셋된다() throws Exception {
        int every = TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS;
        List<LogRecord> warnings = new ArrayList<>();
        withWarnings(warnings, () -> {
            walExecutor();
            TransactionExecutor ex =
                    new TransactionExecutor(failingCheckpoint(connection, () -> new IllegalStateException("injected")));
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1); // 첫 확인에서 실패
            assertEquals(1, warnings.size());
            try (Statement s = connection.createStatement()) {
                s.execute("PRAGMA wal_checkpoint(TRUNCATE)"); // 바깥에서 WAL을 비운다.
            }
            commitBlobs(ex, every); // 한도 아래라 확인이 통과하고 상태를 지운다.
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1); // 다시 실패
            assertEquals(2, warnings.size(), "리셋 뒤 첫 실패는 다시 WARNING");
        });
    }

    /** 되감기에 성공한 뒤 WAL 크기를 못 읽어도 되감기는 실패가 아니다. 기억값은 0(다음 확인에서 다시 걸 수 있는 쪽)이고 따로 로그한다. */
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
                        if (reads.incrementAndGet() == 2) { // 되감기 직후의 읽기
                            throw new SecurityException("injected");
                        }
                        return Files.size(path);
                    });
            commitBlobs(ex, 1, (int) (9 * MB));
            commitBlobs(ex, every - 1);
            assertEquals(1, warnings.size(), "크기 읽기 실패는 따로 한 번 로그한다");
            assertFalse(warnings.get(0).getMessage().contains("checkpoint did not complete"), "체크포인트 실패 메시지가 아니다");
            // 연속 실패 상태가 켜지지 않았으므로 이어지는 실패가 첫 실패로 WARNING이 된다.
            failing.set(true);
            commitBlobs(ex, every); // 기억값이 0이므로 WAL이 여전히 한도 위라 다시 시도하고 실패한다.
            assertEquals(2, warnings.size());
            assertTrue(warnings.get(1).getMessage().contains("checkpoint did not complete"));
        });
    }

    /** WAL 크기를 못 읽는 것은 "한도 아래"가 아니다. 체크포인트 실패와 다른 메시지로 한 번 WARNING을 남기고, 계속돼도 간격 억제를 따른다. */
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

    /** 크기 읽기 실패는 연속 실패 상태를 지우지 않는다. 사이에 끼어도 이어지는 체크포인트 실패는 첫 실패로 다시 WARNING이 되지 않는다. */
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
            commitBlobs(ex, every - 1); // 첫 확인에서 체크포인트 실패: 1번째
            assertEquals(1, warnings.size());
            sizeReadFails.set(true);
            commitBlobs(ex, every); // 크기 읽기 실패: 2번째라 로그 없음
            sizeReadFails.set(false);
            commitBlobs(ex, every); // 체크포인트 실패: 3번째라 로그 없음
            assertEquals(1, warnings.size(), "읽기 실패가 횟수를 지웠다면 여기서 다시 WARNING이 난다");
        });
    }

    /** -wal 파일이 없는 것(NoSuchFileException)은 오류가 아니라 크기 0이다. 경고도 실패 횟수 증가도 없다. */
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
            commitBlobs(ex, every - 1); // 체크포인트 실패. 횟수가 늘었다면 첫 실패가 아니라 로그가 없다.
            assertEquals(1, warnings.size(), "파일 없음이 횟수를 올리지 않았으므로 첫 실패로 WARNING");
        });
    }

    /** 파일 없는 데이터베이스(WAL 없음)에서는 WAL 위치를 한 번만 확인한다. */
    @Test
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

    /** {@code wal_checkpoint}를 실행하면 주어진 코드의 {@link SQLException}을 던지는 연결. 나머지는 실제 연결에 맡긴다. */
    private static Connection failingCheckpoint(Connection real, int errorCode) {
        return failingCheckpoint(real, () -> new SQLException("injected", null, errorCode));
    }

    /** {@code wal_checkpoint}를 실행할 때마다 {@code failure}가 돌려주는 예외를 던진다. null이면 실제로 실행한다. */
    private static Connection failingCheckpoint(Connection real, Supplier<Throwable> failure) {
        return failingQuery(real, "wal_checkpoint", failure);
    }

    /** SQL에 {@code fragment}가 든 {@code executeQuery}마다 {@code failure}의 예외를 던진다. null이면 실제로 실행한다. */
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
                                if (m.getName().equals("executeQuery") && ((String) a[0]).contains(fragment)) {
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

    /** 저장소가 열 때와 같은 WAL 설정으로 바꾸고 BLOB 테이블을 만든다. */
    private TransactionExecutor walExecutor() throws SQLException {
        try (Statement s = connection.createStatement()) {
            TransactionExecutor.configureWal(s);
            s.execute("PRAGMA synchronous = NORMAL");
            s.execute("CREATE TABLE big (b BLOB)");
        }
        return executor();
    }

    /** {@link #BLOB_BYTES} 크기 BLOB 하나를 넣는 커밋을 {@code commits}번 한다. */
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

    /**
     * 한도(8MB) 아래에서는 되감지 않는다(ADR-0017). {@code RESTART}는 파일을 줄이지 않고 앞에서부터 다시 쓰게 하므로, 한도와
     * 상관없이 되감는 구현이면 파일이 커밋 몇 번 분량에서 멈춘다.
     */
    @Test
    void 한도_아래에서는_되감지_않아_WAL_파일이_쓴_만큼_자란다() throws Exception {
        TransactionExecutor executor = walExecutor();

        commitBlobs(executor, 80); // 5MB

        long size = walFileSize();
        assertTrue(size >= 5 * MB, "WAL 파일이 " + size + "바이트다. 5MB를 썼으니 그보다 커야 한다. 한도 아래에서 되감았다");
        assertTrue(size < TransactionExecutor.CHECKPOINT_WAL_BYTES, "WAL 파일이 " + size + "바이트다. 한도 아래까지만 쓰려 했다");
    }

    /**
     * 한도를 한참 넘게 써도 WAL 파일은 한도에서 크게 벗어나지 않는다(ADR-0017). 되감은 직후 크기를 기억하므로 되감을 때마다 파일은 확인 주기
     * 동안 쓴 양 이하로 조금씩 늘 수 있다. 그래서 상한은 "한도 + 되감기 횟수 × 확인 주기 동안 쓴 양"이다. 한도를 넘고도 늦게 되감거나 되감은
     * 직후 크기를 잘못 기억하면 이보다 커진다. 커밋이 작아야 이 여유가 한도보다 한참 작다. 커밋 하나의 증가량은 측정하므로 페이지 크기나 레코드
     * 형식에 기대지 않는다.
     */
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
