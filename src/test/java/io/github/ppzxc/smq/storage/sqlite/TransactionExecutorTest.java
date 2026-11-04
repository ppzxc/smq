package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.SmqStorageException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
        connection = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("t.db"));
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

    /** WAL이 한도(8MB)를 넘으면 커밋 몇 번 뒤에 되감는다(ADR-0010). 되감은 뒤의 쓰기는 WAL의 앞에서 다시 시작한다. */
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
     * 커밋 뒤 체크포인트의 SQL 오류는 BUSY(5)든 아니든 같은 규칙이다. 이미 커밋됐고 효력을 알 수 없는
     * {@link SmqStorageException}이며, 삼키지 않는다.
     */
    @ParameterizedTest
    @ValueSource(ints = {5, 517, 10})
    void 커밋_뒤_체크포인트의_SQL_오류는_BUSY든_아니든_효력_불명인_SmqStorageException이다(int errorCode) throws Exception {
        walExecutor();
        TransactionExecutor ex = new TransactionExecutor(failingCheckpoint(connection, errorCode));
        ex.execute(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO big VALUES (zeroblob(" + (9 * MB) + "))");
            }
            return null;
        });
        // 확인 주기(커밋 8번)가 돌아오면 WAL이 한도를 넘었으므로 체크포인트가 실패한다.
        assertThrows(
                SmqStorageException.class, () -> commitBlobs(ex, TransactionExecutor.CHECKPOINT_CHECK_EVERY_COMMITS));
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM big")) {
            assertTrue(rs.getInt(1) >= 2, "체크포인트가 실패해도 커밋은 남는다");
        }
    }

    /** {@code wal_checkpoint}를 실행하면 주어진 코드의 {@link SQLException}을 던지는 연결. 나머지는 실제 연결에 맡긴다. */
    private static Connection failingCheckpoint(Connection real, int errorCode) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    Object result = invoke(method, real, args);
                    if (!method.getName().equals("createStatement")) {
                        return result;
                    }
                    Statement st = (Statement) result;
                    return Proxy.newProxyInstance(
                            Statement.class.getClassLoader(), new Class<?>[] {Statement.class}, (p, m, a) -> {
                                if (m.getName().equals("executeQuery") && ((String) a[0]).contains("wal_checkpoint")) {
                                    throw new SQLException("injected", null, errorCode);
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
     * 한도(8MB) 아래에서는 되감지 않는다(ADR-0010). {@code RESTART}는 파일을 줄이지 않고 앞에서부터 다시 쓰게 하므로, 한도와
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
     * 한도를 한참 넘게 써도 WAL 파일은 한도에서 크게 벗어나지 않는다(ADR-0010). 되감은 직후 크기를 기억하므로 되감을 때마다 파일은 확인 주기
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
