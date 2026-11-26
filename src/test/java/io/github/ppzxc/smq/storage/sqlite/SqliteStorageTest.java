package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.Disposition;
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
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    void 새_DB는_FULL을_사용하고_기존_NONE_DB는_유지한다() throws Exception {
        Path fresh = dir.resolve("fresh.db");
        try (SqliteStorage ignored = SqliteStorage.open(fresh, Durability.PROCESS_CRASH)) {
            assertNotNull(ignored);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + fresh)) {
            assertEquals("1", pragma(c, "auto_vacuum"));
        }

        Path existing = dir.resolve("existing.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing)) {
            try (Statement s = c.createStatement()) {
                s.execute("PRAGMA auto_vacuum = NONE");
                SqliteSchema.configureNewDatabase(c, true);
                s.execute("BEGIN IMMEDIATE");
                new SqliteSchema(2, Map.of(1, SqliteSchema::upgradeV1ToV2)).establish(c);
                s.execute("COMMIT");
            }
        }

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA auto_vacuum = NONE");
            s.execute("VACUUM");
        }
        try (SqliteStorage ignored = SqliteStorage.open(existing, Durability.PROCESS_CRASH)) {
            assertNotNull(ignored);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing)) {
            assertEquals("0", pragma(c, "auto_vacuum"));
        }
    }

    @Test
    void 두_dead_letter_이동_경로가_커밋된_목적지를_알린다() throws Exception {
        InstantSource clock = InstantSource.fixed(Instant.EPOCH);
        Duration wait = Duration.ofSeconds(1);
        List<Optional<String>> destinations = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(dir.resolve("dead-notice.db"), Durability.PROCESS_CRASH)) {
            storage.declare("dead", QueueOptions.defaults());
            storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
            var message = new NewMessage(new byte[0], Map.of());
            var ids = storage.publish("q", List.of(message, message), clock);
            var chunk = storage.pull("q", 2, wait, clock);
            storage.settle(
                    java.util.Objects.requireNonNull(chunk.token()),
                    List.of(
                            new Disposition(ids.get(0), Disposition.Kind.REJECT),
                            new Disposition(ids.get(1), Disposition.Kind.NACK)),
                    clock,
                    destinations::add);
            assertEquals(List.of(Optional.of("dead")), destinations);
            storage.pull("q", 1, wait, clock, wait, destinations::add);
            assertEquals(List.of(Optional.of("dead"), Optional.of("dead")), destinations);
            assertEquals(2, storage.pull("dead", 2, wait, clock).messages().size());
        }
    }

    @Test
    void 단일_활성_큐는_앞선_지연_메시지_뒤에서도_소진된_메시지를_먼저_옮긴다() throws Exception {
        InstantSource clock = InstantSource.fixed(Instant.EPOCH);
        try (SqliteStorage storage = SqliteStorage.open(dir.resolve("serial-ready.db"), Durability.PROCESS_CRASH)) {
            storage.declare("dead", QueueOptions.defaults());
            storage.declare(
                    "q", QueueOptions.defaults().singleActiveConsumer(true).maxDeliveries(1, "dead"));
            var message = new NewMessage(new byte[0], Map.of());
            var ids = storage.publish("q", List.of(message, message), clock);
            var chunk = storage.pull("q", 2, Duration.ofSeconds(30), clock);
            storage.settle(
                    java.util.Objects.requireNonNull(chunk.token()),
                    List.of(
                            new Disposition(ids.get(0), Disposition.Kind.NACK, Duration.ofSeconds(10)),
                            new Disposition(ids.get(1), Disposition.Kind.NACK, Duration.ofSeconds(2))),
                    clock);

            assertEquals(
                    Instant.EPOCH.plusSeconds(2),
                    storage.nextReadyAt("q", clock, Duration.ofSeconds(1)).orElseThrow());
        }
    }

    @Test
    void 다음_준비_시각은_지연_종료와_리스_만료_중_이른_쪽이다() throws Exception {
        Instant now = Instant.EPOCH;
        InstantSource clock = InstantSource.fixed(now);
        Duration wait = Duration.ofSeconds(1);
        try (SqliteStorage storage = SqliteStorage.open(dir.resolve("ready-at.db"), Durability.PROCESS_CRASH)) {
            storage.declare("q", QueueOptions.defaults());
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ZERO)), clock);
            storage.pull("q", 1, Duration.ofSeconds(3), clock);
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ofSeconds(2))), clock);
            assertEquals(
                    now.plusSeconds(2), storage.nextReadyAt("q", clock, wait).orElseThrow());
            assertEquals(
                    now.plusSeconds(3),
                    storage.nextReadyAt("q", InstantSource.fixed(now.plusSeconds(2)), wait)
                            .orElseThrow());
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
    void existingV2SampleInstallsDelayedIndexAtomically() throws Exception {
        Path db = dir.resolve("v2.db");
        try (var sample = getClass().getResourceAsStream("/schema-samples/schema-v2-before-delayed-index.db")) {
            assertNotNull(sample);
            Files.copy(sample, db);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            c.setAutoCommit(false);
            SqliteSchema.ensureDelayedIdIndex(c);
            c.rollback();
            try (Statement st = c.createStatement();
                    ResultSet rs =
                            st.executeQuery("SELECT count(*) FROM sqlite_schema WHERE name = 'message_delayed_id'")) {
                if (!rs.next()) {
                    throw new AssertionError("Expected a query result row");
                }
                assertEquals(0, rs.getInt(1));
            }
        }
        try (SqliteStorage storage = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            assertSearchesIndex(
                    explain(storage, SqliteStorage.SELECT_DELAY_BARRIER, 1L), "message_delayed_id", "(queue_id=?)");
            assertEquals("2", storage.inTransaction(c -> pragma(c, "user_version")));
        }
    }

    @Test
    void pullQueriesSeekByStoredStateAndDeadline() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            for (String sql : List.of(SqliteStorage.SELECT_PULLABLE, SqliteStorage.SELECT_PULLABLE_STATE)) {
                String plan = explain(s, sql, 1L, 3, Long.MAX_VALUE, 10);
                assertSearchesIndex(plan, "message_stored_ready", "(queue_id=? AND id<?)");
            }
            String exhausted = explain(s, SqliteStorage.SELECT_EXHAUSTED, 1L, 3, 10);
            assertSearchesIndex(exhausted, "message_redelivered_ready", "(queue_id=? AND delivery_count>?)");

            assertSearchesIndex(
                    explain(s, SqliteStorage.SELECT_DELAY_BARRIER, 1L), "message_delayed_id", "(queue_id=?)");

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
            var ok = new NewMessage(new byte[] {1}, Map.of());
            var bad = new NewMessage(null, Map.of());
            assertThrows(
                    RuntimeException.class, () -> s.publish("q", List.of(ok, bad), InstantSource.fixed(Instant.EPOCH)));
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
    void 단일_활성_큐의_활성_리스가_있으면_준비_메시지가_있어도_쓰기_트랜잭션을_열지_않는다() {
        AtomicInteger begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                dir.resolve("single-active-pull.db"),
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
            s.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
            var message = new NewMessage(new byte[0], Map.of());
            s.publish("q", List.of(message, message), InstantSource.fixed(Instant.EPOCH));
            assertEquals(
                    1,
                    s.pull("q", 1, Duration.ofSeconds(30), InstantSource.fixed(Instant.EPOCH))
                            .messages()
                            .size());

            int beforeBlockedPull = begins.get();
            assertTrue(s.pull("q", 1, Duration.ofSeconds(30), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .isEmpty());
            assertEquals(beforeBlockedPull, begins.get());
        }
    }

    @Test
    void cachedStatementIsReusedAndClosedWithStorage() throws Exception {
        List<PreparedStatement> prepared = new ArrayList<>();
        SqliteStorage storage = SqliteStorage.open(
                dir.resolve("statement-cache.db"),
                Durability.PROCESS_CRASH,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    Connection real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (p, m, a) -> {
                                try {
                                    Object result = m.invoke(real, a);
                                    if (m.getName().equals("prepareStatement")
                                            && a != null
                                            && "SELECT id FROM queue WHERE name = ?".equals(a[0])) {
                                        prepared.add((PreparedStatement) result);
                                    }
                                    return result;
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                });
        try {
            storage.declare("q", QueueOptions.defaults());
            storage.counts("q");
            storage.counts("q");
            assertEquals(1, prepared.size());
            assertFalse(prepared.get(0).isClosed());
        } finally {
            storage.close();
        }
        assertTrue(prepared.get(0).isClosed());
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
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            s.pull("q", 1, Duration.ofSeconds(1), InstantSource.fixed(Instant.EPOCH));
            assertEquals(promoted, s.counts("q").ready());
            assertEquals(-promoted, s.counts("q").delayed());
        }
    }

    @Test
    void 손상된_헤더는_꺼내기에서_저장소_오류다() {
        try (SqliteStorage s = SqliteStorage.open(dir.resolve("bad.db"), Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            s.inTransaction(c -> {
                try (Statement st = c.createStatement()) {
                    st.execute("UPDATE message SET headers = X'000000017fffffff'");
                }
                return null;
            });
            assertThrows(
                    SmqStorageException.class,
                    () -> s.pull("q", 1, Duration.ofSeconds(1), InstantSource.fixed(Instant.EPOCH)));
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
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            assertFalse(Files.exists(shm), "다시 열어도 -shm이 생기면 안 된다");
            String expected = durability == Durability.POWER_LOSS ? "2" : "1";
            assertEquals(expected, s.inTransaction(c -> pragma(c, "synchronous")));
            assertEquals("exclusive", s.inTransaction(c -> pragma(c, "locking_mode")));
        }
    }

    private static NewMessage message() {
        return new NewMessage(new byte[] {1}, Map.of());
    }

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
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            assertEquals("exclusive", rs.getString(1));
        }
    }
}
