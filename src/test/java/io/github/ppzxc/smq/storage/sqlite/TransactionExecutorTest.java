package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.SmqStorageException;
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

/** 트랜잭션 실행 모듈. */
class TransactionExecutorTest {

    private static final int SQLITE_CONSTRAINT = 19;

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
            s.execute("PRAGMA journal_mode = WAL");
            s.execute("PRAGMA wal_autocheckpoint = 0");
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
}
