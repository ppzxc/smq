package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.SmqStorageException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 트랜잭션 실행 모듈. 재시도 정책을 주입하므로 실제 경합도 대기 시간도 쓰지 않는다. */
class TransactionExecutorTest {

    private static final int SQLITE_BUSY = 5;
    /** 확장 결과 코드: SQLITE_BUSY_SNAPSHOT. 아래 8비트가 SQLITE_BUSY다. */
    private static final int SQLITE_BUSY_SNAPSHOT = 517;
    private static final int SQLITE_CONSTRAINT = 19;

    @TempDir
    Path dir;

    private Connection connection;
    private final List<Integer> waits = new ArrayList<>();

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

    private TransactionExecutor executor(int maxAttempts) {
        return new TransactionExecutor(connection, new TransactionExecutor.RetryPolicy(maxAttempts, waits::add));
    }

    private static SQLException busy(int extendedCode) {
        return new SQLException("busy", null, extendedCode);
    }

    private int count() throws SQLException {
        try (Statement s = connection.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM t")) {
            return rs.getInt(1);
        }
    }

    @Test
    void BUSY가_정해진_횟수만큼_난_뒤_성공하면_결과가_그대로_돌아온다() {
        TransactionExecutor ex = executor(5);
        int[] calls = {0};
        String result = ex.retryOnBusy(() -> {
            if (++calls[0] <= 2) {
                throw busy(calls[0] == 1 ? SQLITE_BUSY : SQLITE_BUSY_SNAPSHOT);
            }
            return "done";
        });
        assertEquals("done", result);
        assertEquals(3, calls[0]);
        assertEquals(List.of(1, 2), waits);
    }

    @Test
    void 마지막_시도까지_BUSY면_SmqStorageException이_난다() {
        TransactionExecutor ex = executor(3);
        int[] calls = {0};
        SmqStorageException e = assertThrows(SmqStorageException.class, () -> ex.retryOnBusy(() -> {
            calls[0]++;
            throw busy(SQLITE_BUSY);
        }));
        assertEquals(3, calls[0]);
        assertEquals(List.of(1, 2), waits);
        assertInstanceOf(SQLException.class, e.getCause());
    }

    @Test
    void BUSY가_아닌_SQL_오류는_재시도_없이_바로_SmqStorageException이_된다() {
        TransactionExecutor ex = executor(5);
        int[] calls = {0};
        assertThrows(SmqStorageException.class, () -> ex.retryOnBusy(() -> {
            calls[0]++;
            throw new SQLException("constraint", null, SQLITE_CONSTRAINT);
        }));
        assertEquals(1, calls[0]);
        assertEquals(List.of(), waits);
    }

    @Test
    void 재시도_대기_중_interrupt되면_interrupt_상태를_되살리고_SmqStorageException을_던진다() {
        TransactionExecutor ex = new TransactionExecutor(connection, new TransactionExecutor.RetryPolicy(5, attempt -> {
            throw new InterruptedException();
        }));
        int[] calls = {0};
        try {
            assertThrows(SmqStorageException.class, () -> ex.retryOnBusy(() -> {
                calls[0]++;
                throw busy(SQLITE_BUSY);
            }));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, calls[0]);
        } finally {
            Thread.interrupted(); // 다른 테스트로 새지 않게 지운다.
        }
    }

    @Test
    void 확인_없는_실행의_결과가_기준_버전이_되고_확인하는_실행은_그_버전이면_통과한다() {
        TransactionExecutor ex = executor(1);
        assertEquals(0, ex.executeUnverified(c -> userVersion(c)));
        assertEquals("ok", ex.execute(c -> "ok"));
    }

    @Test
    void 실패한_트랜잭션은_ROLLBACK되고_다음_트랜잭션이_정상적으로_시작된다() throws Exception {
        TransactionExecutor ex = executor(1);
        ex.executeUnverified(c -> userVersion(c));
        assertThrows(SmqStorageException.class, () -> ex.execute(c -> {
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
    void 열린_뒤_스키마_버전이_바뀌면_실패하고_버전이_돌아와도_영구_실패로_남는다() throws Exception {
        TransactionExecutor ex = executor(1);
        ex.executeUnverified(c -> userVersion(c));
        setUserVersion(7);
        int[] bodyRuns = {0};
        assertThrows(IncompatibleStorageException.class, () -> ex.execute(c -> bodyRuns[0]++));
        setUserVersion(0);
        assertThrows(IncompatibleStorageException.class, () -> ex.execute(c -> bodyRuns[0]++));
        assertEquals(0, bodyRuns[0]);
    }

    private static int userVersion(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("PRAGMA user_version")) {
            return rs.getInt(1);
        }
    }

    private void setUserVersion(int v) throws SQLException {
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("t.db"));
                Statement s = other.createStatement()) {
            s.execute("PRAGMA user_version = " + v);
        }
    }

    /**
     * 읽는 연결 때문에 WAL을 되감지 못하는 동안 커밋마다 체크포인트 대기(50ms)가 쌓이면 안 된다(ADR-0010).
     * 실패한 뒤에는 한동안 다시 시도하지 않는다.
     */
    @Test
    void WAL을_되감지_못해도_커밋마다_체크포인트를_기다리지_않는다() throws Exception {
        try (Statement s = connection.createStatement()) {
            s.execute("PRAGMA journal_mode = WAL");
            s.execute("PRAGMA wal_autocheckpoint = 0");
        }
        try (Connection reader = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("t.db"));
                Statement rs = reader.createStatement()) {
            rs.execute("BEGIN");
            try (ResultSet r = rs.executeQuery("SELECT count(*) FROM t")) {
                r.next();
            }
            TransactionExecutor executor = executor(1);
            executor.execute(c -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO t VALUES (1)");
                    s.execute("CREATE TABLE big (b BLOB)");
                    s.execute("INSERT INTO big VALUES (zeroblob(20000000))");
                }
                return null;
            });
            assertTrue(Files.size(dir.resolve("t.db-wal")) > 16L * 1024 * 1024);

            long start = System.nanoTime();
            for (int i = 0; i < 40; i++) {
                executor.execute(c -> {
                    try (Statement s = c.createStatement()) {
                        s.execute("INSERT INTO t VALUES (2)");
                    }
                    return null;
                });
            }
            long millis = (System.nanoTime() - start) / 1_000_000;

            assertTrue(millis < 1000, "40번 커밋에 " + millis + "ms. 커밋마다 체크포인트를 기다린다");
            rs.execute("COMMIT");
        }
    }
}
