package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.NewMessage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SqliteStorageTest {

    @TempDir
    Path dir;

    private String pragma(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            return rs.getString(1);
        }
    }

    @Test
    void 열기에서_Error가_나면_락을_놓고_같은_파일을_다시_연다() {
        Path db = dir.resolve("error.db");
        Error failure = new LinkageError("injected");
        assertSame(
                failure,
                assertThrows(
                        LinkageError.class,
                        () -> SqliteStorage.open(
                                db, Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                                    throw failure;
                                })));
        try (SqliteStorage ignored = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            assertNotNull(ignored);
        }
    }

    @Test
    void 새_파일에_스키마_v2와_표식을_기록한다() throws Exception {
        Path db = dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            assertEquals(String.valueOf(SqliteStorage.APPLICATION_ID), pragma(c, "application_id"));
            assertEquals("2", pragma(c, "user_version"));
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
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE foreign_table(x)");
        }
        assertThrows(IncompatibleStorageException.class, () -> SqliteStorage.open(db, Durability.POWER_LOSS));
    }

    @Test
    void 연결은_WAL_synchronous_FULL을_쓴다() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            assertEquals("wal", s.inTransaction(c -> pragma(c, "journal_mode")));
            assertEquals("2", s.inTransaction(c -> pragma(c, "synchronous")));
        }
    }

    @Test
    void pullQueriesSeekByStoredStateAndDeadline() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            for (String sql : List.of(SqliteStorage.SELECT_PULLABLE, SqliteStorage.SELECT_PULLABLE_STATE)) {
                String plan = explain(s, sql, 1L, 3, 10);
                assertSearchesIndex(plan, "message_stored_ready", "(queue_id=?)");
            }
            String exhausted = explain(s, SqliteStorage.SELECT_EXHAUSTED, 1L, 3, 10);
            assertSearchesIndex(exhausted, "message_redelivered_ready", "(queue_id=? AND delivery_count>?)");

            String active = explain(s, SqliteStorage.SELECT_ACTIVE_LEASE, 1L, 0L);
            assertSearchesIndex(active, "message_lease_deadline", "(queue_id=? AND lease_deadline>?)");
            assertTrue(active.contains("USING COVERING INDEX"), active);

            assertSearchesIndex(
                    explain(s, SqliteStorage.PROMOTE_EXPIRED_LEASES, 1L, 0L),
                    "message_lease_deadline",
                    "(queue_id=? AND lease_deadline<?)");
            assertSearchesIndex(
                    explain(s, SqliteStorage.PROMOTE_DUE_DELAYS, 1L, 0L),
                    "message_available_at",
                    "(queue_id=? AND available_at<?)");

            String read = explain(s, SqliteStorage.ANY_READY, "q", "q", 0L, "q", 0L, "q");
            assertSearchesIndex(read, "message_stored_ready", "(queue_id=?)");
            assertSearchesIndex(read, "message_lease_deadline", "(queue_id=? AND lease_deadline<?)");
            assertSearchesIndex(read, "message_available_at", "(queue_id=? AND available_at<?)");
        }
    }

    private static String explain(SqliteStorage storage, String sql, Object... bindings) {
        return storage.inTransaction(c -> {
            List<String> rows = new ArrayList<>();
            try (var ps = c.prepareStatement("EXPLAIN QUERY PLAN " + sql)) {
                for (int i = 0; i < bindings.length; i++) {
                    ps.setObject(i + 1, bindings[i]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(rs.getString("detail"));
                    }
                }
            }
            String plan = String.join("\n", rows);
            assertFalse(plan.contains("SCAN message"), plan);
            assertFalse(plan.contains("TEMP B-TREE"), plan);
            return plan;
        });
    }

    private static void assertSearchesIndex(String plan, String index, String constraints) {
        assertTrue(
                plan.lines()
                        .anyMatch(row -> row.startsWith("SEARCH message USING ")
                                && row.endsWith("INDEX " + index + " " + constraints)),
                plan);
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
            assertEquals(0, s.counts("q").ready());
        }
    }

    @Test
    void 큐_존재와_옵션_조회는_쓰기_트랜잭션을_열지_않는다() {
        AtomicInteger begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                dir.resolve("read.db"), Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (path, durability) -> {
                    Connection real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (p, m, a) -> {
                                Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) return result;
                                Statement st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class<?>[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) begins.incrementAndGet();
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            int before = begins.get();
            assertTrue(s.exists("q"));
            assertTrue(s.options("q").isPresent());
            assertFalse(s.exists("missing"));
            assertTrue(s.options("missing").isEmpty());
            assertEquals(before, begins.get());
        }
    }

    @Test
    void pullTransfersLargePromotionCountWithoutOverflow() {
        long promoted = (long) Integer.MAX_VALUE + 1;
        try (SqliteStorage s = SqliteStorage.open(
                dir.resolve("large-count.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    Connection real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (p, m, a) -> {
                                Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("prepareStatement")
                                        || a == null
                                        || !SqliteStorage.PROMOTE_DUE_DELAYS.equals(a[0])) return result;
                                PreparedStatement statement = (PreparedStatement) result;
                                return Proxy.newProxyInstance(
                                        PreparedStatement.class.getClassLoader(),
                                        new Class<?>[] {PreparedStatement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("executeLargeUpdate")) {
                                                statement.executeLargeUpdate();
                                                return promoted;
                                            }
                                            if (m2.getName().equals("executeUpdate")) {
                                                statement.executeUpdate();
                                                return (int) promoted;
                                            }
                                            try {
                                                return m2.invoke(statement, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            s.pull("q", 1, Duration.ofSeconds(1), Instant.EPOCH);
            assertEquals(promoted, s.counts("q").ready());
            assertEquals(-promoted, s.counts("q").delayed());
        }
    }

    @Test
    void 손상된_헤더는_꺼내기에서_저장소_오류다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("bad.db"), Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            s.inTransaction(c -> {
                try (Statement st = c.createStatement()) {
                    st.execute("UPDATE message SET headers = X'000000017fffffff'");
                }
                return null;
            });
            assertThrows(SmqStorageException.class, () -> s.pull("q", 1, Duration.ofSeconds(1), Instant.EPOCH));
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

    @ParameterizedTest
    @EnumSource(Durability.class)
    void 다시_열어도_shm_없이_열리고_synchronous가_내구성_수준대로_걸린다(Durability durability) {
        Path db = dir.resolve("smq.db");
        Path shm = Path.of(db + "-shm");
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.declare("q", QueueOptions.defaults());
            assertEquals("wal", s.inTransaction(c -> pragma(c, "journal_mode")));
        }
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.publish("q", List.of(message()), Instant.EPOCH);
            assertFalse(Files.exists(shm), "다시 열어도 -shm이 생기면 안 된다");
            String expected = durability == Durability.POWER_LOSS ? "2" : "1";
            assertEquals(expected, s.inTransaction(c -> pragma(c, "synchronous")));
            assertEquals("exclusive", s.inTransaction(c -> pragma(c, "locking_mode")));
        }
    }

    private static NewMessage message() {
        return new NewMessage(new byte[] {1}, Map.of());
    }

    /**
     * SQLite가 EXCLUSIVE를 거부하는 상황은 실제 연결로 만들 수 없어(메모리·파일 모두 "exclusive"로 답한다) 판정 함수만 직접
     * 시험한다. 결과가 exclusive가 아니면 SQLException이고 대소문자는 가리지 않는다.
     */
    @Test
    void locking_mode_결과가_exclusive가_아니면_SQLException이다() {
        assertDoesNotThrow(() -> SqliteStorage.requireExclusive("exclusive"));
        assertDoesNotThrow(() -> SqliteStorage.requireExclusive("EXCLUSIVE"));
        assertThrows(SQLException.class, () -> SqliteStorage.requireExclusive("normal"));
        assertThrows(SQLException.class, () -> SqliteStorage.requireExclusive(null));
    }

    @Test
    void 연결을_열면_locking_mode가_exclusive다() throws Exception {
        try (Connection c = SqliteStorage.connect(dir.resolve("x.db"), Durability.POWER_LOSS);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA locking_mode")) {
            assertTrue(rs.next());
            assertEquals("exclusive", rs.getString(1));
        }
    }
}
