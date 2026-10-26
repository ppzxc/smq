package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    void 꺼내기가_쓰는_쿼리는_인덱스를_탄다() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            for (String sql : List.of(SqliteStorage.SELECT_PULLABLE, SqliteStorage.SELECT_EXHAUSTED)) {
                List<String> plan = s.inTransaction(c -> {
                    List<String> rows = new ArrayList<>();
                    try (var ps = c.prepareStatement("EXPLAIN QUERY PLAN " + sql)) {
                        ps.setLong(1, 1);
                        ps.setLong(2, 0);
                        ps.setLong(3, 0);
                        ps.setInt(4, 3);
                        ps.setInt(5, 10);
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

    /** 시도 횟수 제한이 풀리면 재시도가 끝나지 않으므로, 실패 경로 테스트가 멈추지 않고 실패하게 하는 한도. */
    private static final Duration RETRY_EXHAUST_LIMIT = Duration.ofSeconds(10);

    private static NewMessage message() {
        return new NewMessage(new byte[] {1}, Map.of());
    }

    @Test
    void 쓰기_락이_잠깐_잡혀_있다_풀리면_발행이_재시도로_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 20, 100);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            st.execute("BEGIN IMMEDIATE");
            Thread releaser = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    st.execute("COMMIT");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            releaser.start();
            assertEquals(1, s.publish("q", List.of(message()), Instant.EPOCH).size());
            releaser.join();
            assertEquals(1, s.counts("q", Instant.EPOCH).ready());
        }
    }

    @Test
    void 재시도로도_풀리지_않으면_SmqStorageException으로_올리고_아무것도_반영하지_않는다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 5, 3);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            st.execute("BEGIN IMMEDIATE");
            assertThrows(SmqStorageException.class,
                    () -> assertTimeoutPreemptively(RETRY_EXHAUST_LIMIT, () -> s.publish("q", List.of(message()), Instant.EPOCH)));
            st.execute("ROLLBACK");
            assertEquals(0, s.counts("q", Instant.EPOCH).ready());
        }
    }

    @Test
    void 빈_큐를_꺼내_보는_폴링은_다른_연결이_쓰기_락을_잡고_있어도_기다리지_않는다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 2000, 1);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            st.execute("BEGIN IMMEDIATE");
            long start = System.nanoTime();
            assertTrue(s.pull("q", 1, Duration.ofSeconds(1), Instant.EPOCH).messages().isEmpty());
            assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000);
            st.execute("ROLLBACK");
        }
    }


    private static void releaseAfter(Statement st, long millis, List<Thread> threads) {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(millis);
                st.execute("COMMIT");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        threads.add(t);
        t.start();
    }

    @Test
    void 쓰기_락이_잠깐_잡혀_있다_풀리면_꺼내기가_재시도로_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 20, 100);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            st.execute("BEGIN IMMEDIATE");
            List<Thread> threads = new ArrayList<>();
            releaseAfter(st, 300, threads);
            assertEquals(1, s.pull("q", 1, Duration.ofSeconds(60), Instant.EPOCH).messages().size());
            threads.get(0).join();
            assertEquals(new QueueCounts(0, 1, 0), s.counts("q", Instant.EPOCH));
        }
    }

    @Test
    void 재시도로도_풀리지_않으면_꺼내기는_SmqStorageException이고_아무것도_반영하지_않는다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 5, 3);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            st.execute("BEGIN IMMEDIATE");
            assertThrows(SmqStorageException.class,
                    () -> assertTimeoutPreemptively(RETRY_EXHAUST_LIMIT, () -> s.pull("q", 1, Duration.ofSeconds(60), Instant.EPOCH)));
            st.execute("ROLLBACK");
            assertEquals(new QueueCounts(1, 0, 0), s.counts("q", Instant.EPOCH));
        }
    }

    @Test
    void 쓰기_락이_잠깐_잡혀_있다_풀리면_정산이_재시도로_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 20, 100);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            PullResult pulled = s.pull("q", 1, Duration.ofSeconds(60), Instant.EPOCH);
            Disposition ack = new Disposition(pulled.messages().get(0).id(), Disposition.Kind.ACK);
            st.execute("BEGIN IMMEDIATE");
            List<Thread> threads = new ArrayList<>();
            releaseAfter(st, 300, threads);
            assertEquals(1, s.settle(pulled.token(), List.of(ack), Instant.EPOCH).size());
            threads.get(0).join();
            assertEquals(new QueueCounts(0, 0, 0), s.counts("q", Instant.EPOCH));
        }
    }

    @Test
    void 재시도로도_풀리지_않으면_정산은_SmqStorageException이고_아무것도_반영하지_않는다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 5, 3);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            PullResult pulled = s.pull("q", 1, Duration.ofSeconds(60), Instant.EPOCH);
            Disposition ack = new Disposition(pulled.messages().get(0).id(), Disposition.Kind.ACK);
            st.execute("BEGIN IMMEDIATE");
            assertThrows(SmqStorageException.class,
                    () -> assertTimeoutPreemptively(RETRY_EXHAUST_LIMIT, () -> s.settle(pulled.token(), List.of(ack), Instant.EPOCH)));
            st.execute("ROLLBACK");
            assertEquals(new QueueCounts(0, 1, 0), s.counts("q", Instant.EPOCH));
        }
    }

    private static void assertCheckpointTruncates(Connection other) throws SQLException {
        try (Statement st = other.createStatement(); ResultSet rs = st.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
            assertEquals(0, rs.getInt(1), "열린 읽기 트랜잭션이 checkpoint를 막고 있다");
        }
    }

    @Test
    void 비어_있는_큐의_폴링_뒤에도_읽기_트랜잭션이_남지_않아_checkpoint가_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            s.declare("q", QueueOptions.defaults());
            assertTrue(s.pull("q", 1, Duration.ofSeconds(1), Instant.EPOCH).messages().isEmpty());
            assertCheckpointTruncates(other);
        }
    }

    @Test
    void 메시지가_있는_큐의_폴링_뒤에도_읽기_트랜잭션이_남지_않아_checkpoint가_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS);
                Connection other = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), Instant.EPOCH);
            assertEquals(1, s.pull("q", 1, Duration.ofSeconds(1), Instant.EPOCH).messages().size());
            assertCheckpointTruncates(other);
        }
    }

    // ---- 열기 경로의 BUSY 재시도: 스키마 판정 트랜잭션과 WAL 전환 ----

    @Test
    void 쓰기_락이_잠깐_잡혀_있다_풀리면_새_파일_열기가_재시도로_성공한다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            st.execute("BEGIN IMMEDIATE");
            List<Thread> threads = new ArrayList<>();
            releaseAfter(st, 300, threads);
            try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 20, 100)) {
                s.declare("q", QueueOptions.defaults());
                assertTrue(s.exists("q"));
            }
            threads.get(0).join();
        }
    }

    @Test
    void 재시도로도_풀리지_않으면_새_파일_열기는_SmqStorageException이고_파일은_망가지지_않는다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            st.execute("BEGIN IMMEDIATE");
            assertThrows(SmqStorageException.class,
                    () -> assertTimeoutPreemptively(RETRY_EXHAUST_LIMIT, () -> SqliteStorage.open(db, Durability.POWER_LOSS, 5, 3)));
            st.execute("ROLLBACK");
        }
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
        }
    }

    /** WAL이 아닌 저널 모드의 smq 파일을 만든다. WAL 전환은 읽는 연결이 하나라도 있으면 BUSY다. */
    private Path rollbackJournalFile() throws Exception {
        Path db = dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db); Statement st = c.createStatement()) {
            st.execute("PRAGMA journal_mode = DELETE");
        }
        return db;
    }

    private static void holdReadTransaction(Statement st) throws SQLException {
        st.execute("BEGIN");
        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM sqlite_schema")) {
            rs.next();
        }
    }

    @Test
    void 읽는_연결이_잠깐_있다_없어지면_저널_모드가_WAL이_아닌_파일_열기가_재시도로_성공하고_WAL이_된다() throws Exception {
        Path db = rollbackJournalFile();
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            holdReadTransaction(st);
            List<Thread> threads = new ArrayList<>();
            releaseAfter(st, 300, threads);
            try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS, 20, 100)) {
                threads.get(0).join();
                try (Connection fresh = DriverManager.getConnection("jdbc:sqlite:" + db)) {
                    assertEquals("wal", pragma(fresh, "journal_mode"));
                }
            }
        }
    }

    @Test
    void 읽는_연결이_계속_있으면_저널_모드가_WAL이_아닌_파일_열기는_SmqStorageException이다() throws Exception {
        Path db = rollbackJournalFile();
        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = other.createStatement()) {
            holdReadTransaction(st);
            assertThrows(SmqStorageException.class,
                    () -> assertTimeoutPreemptively(RETRY_EXHAUST_LIMIT, () -> SqliteStorage.open(db, Durability.POWER_LOSS, 5, 3)));
            st.execute("ROLLBACK");
        }
    }
}
