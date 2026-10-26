package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqliteStorageTest {

    @TempDir
    Path dir;

    private String pragma(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            return rs.getString(1);
        }
    }

    @Test
    void 새_파일에_스키마_v1과_표식을_기록한다() throws Exception {
        Path db = dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            assertEquals(String.valueOf(SqliteStorage.APPLICATION_ID), pragma(c, "application_id"));
            assertEquals("1", pragma(c, "user_version"));
            assertEquals("wal", pragma(c, "journal_mode"));
        }
    }

    @Test
    void 다시_열어도_같은_파일로_열린다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
        }
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            assertTrue(s.exists("q"));
        }
    }

    @Test
    void smq가_아닌_비어_있지_않은_파일은_열지_않는다() throws Exception {
        Path db = dir.resolve("other.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE foreign_table(x)");
        }
        assertThrows(IncompatibleStorageException.class, () -> SqliteStorage.open(db, Durability.POWER_LOSS));
    }

    @Test
    void 연결은_WAL_synchronous_FULL_busy_timeout을_쓴다() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            assertEquals("wal", s.inTransaction(c -> pragma(c, "journal_mode")));
            assertEquals("2", s.inTransaction(c -> pragma(c, "synchronous")));
            assertTrue(Integer.parseInt(s.inTransaction(c -> pragma(c, "busy_timeout"))) >= 5000);
        }
    }

    @Test
    void 쓰기_트랜잭션은_BEGIN_IMMEDIATE로_시작한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            s.inTransaction(c -> {
                // 즉시 쓰기 락을 잡았으므로 다른 연결의 쓰기는 busy가 된다
                try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + db + "?busy_timeout=0");
                        Statement st = other.createStatement()) {
                    SQLException e = assertThrows(SQLException.class, () -> st.execute("BEGIN IMMEDIATE"));
                    assertTrue(e.getMessage().contains("BUSY") || e.getMessage().contains("locked"), e.getMessage());
                }
                return null;
            });
        }
    }

    @Test
    void 꺼내기_쿼리는_인덱스를_탄다() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            List<String> plan = s.inTransaction(c -> {
                List<String> rows = new ArrayList<>();
                try (var ps = c.prepareStatement("EXPLAIN QUERY PLAN " + SqliteStorage.SELECT_READY);
                        ) {
                    ps.setLong(1, 1);
                    ps.setLong(2, 0);
                    ps.setInt(3, 10);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            rows.add(rs.getString("detail"));
                        }
                    }
                }
                return rows;
            });
            String joined = String.join("\n", plan);
            assertTrue(joined.contains("USING INDEX") || joined.contains("USING COVERING INDEX"), joined);
            assertFalse(joined.contains("TEMP B-TREE"), joined);
            assertFalse(joined.contains("SCAN message"), joined);
        }
    }

    @Test
    void 파일이_생성된다() throws Exception {
        Path db = dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        assertTrue(Files.exists(db));
    }

    @Test
    void 발행_배치_중간에_실패하면_저장소가_하나도_반영하지_않는다() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
            var ok = new io.github.ppzxc.smq.storage.NewMessage(new byte[] {1}, java.util.Map.of());
            var bad = new io.github.ppzxc.smq.storage.NewMessage(null, java.util.Map.of());
            assertThrows(RuntimeException.class, () -> s.publish("q", List.of(ok, bad), Instant.EPOCH));
            assertEquals(0, s.counts("q", Instant.EPOCH).ready());
        }
    }

    @Test
    void POWER_LOSS는_synchronous_FULL로_연다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            assertEquals("2", s.inTransaction(c -> pragma(c, "synchronous")));
        }
    }

    @Test
    void PROCESS_CRASH는_WAL과_함께_synchronous_NORMAL로_연다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            assertEquals("wal", s.inTransaction(c -> pragma(c, "journal_mode")));
            assertEquals("1", s.inTransaction(c -> pragma(c, "synchronous")));
        }
    }

}
