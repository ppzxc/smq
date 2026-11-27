package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Disposition.Kind;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import java.io.InputStream;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class SqliteStorageTest {

    @TempDir
    Path dir;

    private String pragma(final Connection c, final String name) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            return rs.getString(1);
        }
    }

    private Path createExistingNoneDatabase() throws Exception {
        final Path existing;
        existing = this.dir.resolve("existing.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA auto_vacuum = NONE");
            SqliteSchema.configureNewDatabase(c, true);
            s.execute("BEGIN IMMEDIATE");
            new SqliteSchema(2, Map.of(1, SqliteSchema::upgradeV1ToV2)).establish(c);
            s.execute("COMMIT");
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA auto_vacuum = NONE");
            s.execute("VACUUM");
        }
        return existing;
    }

    @Test
    @DisplayName("새 DB를 열면 저장소를 반환한다")
    void openingNewDatabaseReturnsStorage() throws Exception {
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("fresh.db"), Durability.PROCESS_CRASH)) {
            Assertions.assertNotNull(storage, "Opening a new database should return storage.");
        }
    }

    @Test
    @DisplayName("새 DB는 FULL 자동 vacuum을 사용한다")
    void newDatabaseUsesFullAutoVacuum() throws Exception {
        final Path fresh;
        fresh = this.dir.resolve("fresh.db");
        SqliteStorage.open(fresh, Durability.PROCESS_CRASH).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + fresh)) {
            Assertions.assertEquals(
                    "1", this.pragma(c, "auto_vacuum"), "The new database should use FULL auto vacuum.");
        }
    }

    @Test
    @DisplayName("기존 NONE DB를 열면 저장소를 반환한다")
    void openingExistingNoneDatabaseReturnsStorage() throws Exception {
        try (SqliteStorage storage = SqliteStorage.open(this.createExistingNoneDatabase(), Durability.PROCESS_CRASH)) {
            Assertions.assertNotNull(storage, "Opening an existing database should return storage.");
        }
    }

    @Test
    @DisplayName("기존 DB의 NONE 자동 vacuum을 유지한다")
    void existingDatabaseRetainsNoneAutoVacuum() throws Exception {
        final Path existing;
        existing = this.createExistingNoneDatabase();
        SqliteStorage.open(existing, Durability.PROCESS_CRASH).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + existing)) {
            Assertions.assertEquals(
                    "0", this.pragma(c, "auto_vacuum"), "The existing database should retain NONE auto vacuum.");
        }
    }

    @Test
    @DisplayName("두 dead letter 이동 경로가 커밋된 목적지를 알린다 — check 1")
    void theTwoDeadLetterMigrationPathsIndicateTheCommittedDestinationCheck1() throws Exception {
        final InstantSource clock;
        final Duration wait;
        final List<Optional<String>> destinations;
        clock = InstantSource.fixed(Instant.EPOCH);
        wait = Duration.ofSeconds(1L);
        destinations = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("dead-notice.db"), Durability.PROCESS_CRASH)) {
            final NewMessage message;
            final List<MessageId> ids;
            final PullResult chunk;
            final List<Optional<String>> assertionValue1Argument1;
            storage.declare("dead", QueueOptions.defaults());
            storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
            message = new NewMessage(new byte[0], Map.of());
            ids = storage.publish("q", List.of(message, message), clock);
            chunk = storage.pull("q", 2, wait, clock);
            storage.settle(
                    Objects.requireNonNull(chunk.token()),
                    List.of(new Disposition(ids.get(0), Kind.REJECT), new Disposition(ids.get(1), Kind.NACK)),
                    clock,
                    destinations::add);
            assertionValue1Argument1 = List.of(Optional.of("dead"));
            Assertions.assertEquals(
                    assertionValue1Argument1, destinations, "Destinations should match the expected value.");
            storage.pull("q", 1, wait, clock, wait, destinations::add);
            evaluate(storage.pull("dead", 2, wait, clock).messages().size());
        }
    }

    @Test
    @DisplayName("두 dead letter 이동 경로가 커밋된 목적지를 알린다 — check 2")
    void theTwoDeadLetterMigrationPathsIndicateTheCommittedDestinationCheck2() throws Exception {
        final InstantSource clock;
        final Duration wait;
        final List<Optional<String>> destinations;
        clock = InstantSource.fixed(Instant.EPOCH);
        wait = Duration.ofSeconds(1L);
        destinations = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("dead-notice.db"), Durability.PROCESS_CRASH)) {
            final NewMessage message;
            final List<MessageId> ids;
            final PullResult chunk;
            final List<Optional<String>> assertionValue2Argument1;
            storage.declare("dead", QueueOptions.defaults());
            storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
            message = new NewMessage(new byte[0], Map.of());
            ids = storage.publish("q", List.of(message, message), clock);
            chunk = storage.pull("q", 2, wait, clock);
            storage.settle(
                    Objects.requireNonNull(chunk.token()),
                    List.of(new Disposition(ids.get(0), Kind.REJECT), new Disposition(ids.get(1), Kind.NACK)),
                    clock,
                    destinations::add);
            storage.pull("q", 1, wait, clock, wait, destinations::add);
            assertionValue2Argument1 = List.of(Optional.of("dead"), Optional.of("dead"));
            Assertions.assertEquals(
                    assertionValue2Argument1, destinations, "Destinations should match the expected value.");
            evaluate(storage.pull("dead", 2, wait, clock).messages().size());
        }
    }

    @Test
    @DisplayName("두 dead letter 이동 경로가 커밋된 목적지를 알린다 — check 3")
    void theTwoDeadLetterMigrationPathsIndicateTheCommittedDestinationCheck3() throws Exception {
        final InstantSource clock;
        final Duration wait;
        final List<Optional<String>> destinations;
        clock = InstantSource.fixed(Instant.EPOCH);
        wait = Duration.ofSeconds(1L);
        destinations = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("dead-notice.db"), Durability.PROCESS_CRASH)) {
            final NewMessage message;
            final List<MessageId> ids;
            final PullResult chunk;
            final int assertionValue3Argument2;
            storage.declare("dead", QueueOptions.defaults());
            storage.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
            message = new NewMessage(new byte[0], Map.of());
            ids = storage.publish("q", List.of(message, message), clock);
            chunk = storage.pull("q", 2, wait, clock);
            storage.settle(
                    Objects.requireNonNull(chunk.token()),
                    List.of(new Disposition(ids.get(0), Kind.REJECT), new Disposition(ids.get(1), Kind.NACK)),
                    clock,
                    destinations::add);
            storage.pull("q", 1, wait, clock, wait, destinations::add);
            assertionValue3Argument2 =
                    storage.pull("dead", 2, wait, clock).messages().size();
            Assertions.assertEquals(2, assertionValue3Argument2, "Size should match the expected value.");
        }
    }

    @Test
    @DisplayName("단일 활성 큐는 앞선 지연 메시지 뒤에서도 소진된 메시지를 먼저 옮긴다")
    void aSingleActiveQueueMovesDepletedMessagesFirstEvenAfterPrecedingDelayedMessages() throws Exception {
        final InstantSource clock;
        clock = InstantSource.fixed(Instant.EPOCH);
        try (SqliteStorage storage =
                SqliteStorage.open(this.dir.resolve("serial-ready.db"), Durability.PROCESS_CRASH)) {
            final NewMessage message;
            final List<MessageId> ids;
            final PullResult chunk;
            storage.declare("dead", QueueOptions.defaults());
            storage.declare(
                    "q", QueueOptions.defaults().singleActiveConsumer(true).maxDeliveries(1, "dead"));
            message = new NewMessage(new byte[0], Map.of());
            ids = storage.publish("q", List.of(message, message), clock);
            chunk = storage.pull("q", 2, Duration.ofSeconds(30L), clock);
            storage.settle(
                    Objects.requireNonNull(chunk.token()),
                    List.of(
                            new Disposition(ids.get(0), Kind.NACK, Duration.ofSeconds(10L)),
                            new Disposition(ids.get(1), Kind.NACK, Duration.ofSeconds(2L))),
                    clock);
            Assertions.assertEquals(
                    Instant.EPOCH.plusSeconds(2L),
                    storage.nextReadyAt("q", clock, Duration.ofSeconds(1L)).orElseThrow(),
                    "Next ready at should match the expected value.");
        }
    }

    @Test
    @DisplayName("다음 준비 시각은 지연 종료와 리스 만료 중 이른 쪽이다 — next Ready Time Prefers Delay End")
    void nextReadyTimePrefersDelayEnd() throws Exception {
        final Instant now;
        final InstantSource clock;
        final Duration wait;
        now = Instant.EPOCH;
        clock = InstantSource.fixed(now);
        wait = Duration.ofSeconds(1L);
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("ready-at.db"), Durability.PROCESS_CRASH)) {
            storage.declare("q", QueueOptions.defaults());
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ZERO)), clock);
            storage.pull("q", 1, Duration.ofSeconds(3L), clock);
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ofSeconds(2L))), clock);
            Assertions.assertEquals(
                    now.plusSeconds(2L),
                    storage.nextReadyAt("q", clock, wait).orElseThrow(),
                    "Next ready at should match the expected value.");
        }
    }

    @Test
    @DisplayName("다음 준비 시각은 지연 종료와 리스 만료 중 이른 쪽이다 — next Ready Time Uses Lease Expiry After Delay End")
    void nextReadyTimeUsesLeaseExpiryAfterDelayEnd() throws Exception {
        final Instant now;
        final InstantSource clock;
        final Duration wait;
        now = Instant.EPOCH;
        clock = InstantSource.fixed(now);
        wait = Duration.ofSeconds(1L);
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("ready-at.db"), Durability.PROCESS_CRASH)) {
            storage.declare("q", QueueOptions.defaults());
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ZERO)), clock);
            storage.pull("q", 1, Duration.ofSeconds(3L), clock);
            storage.publish("q", List.of(new NewMessage(new byte[0], Map.of(), Duration.ofSeconds(2L))), clock);
            Assertions.assertEquals(
                    now.plusSeconds(3L),
                    storage.nextReadyAt("q", InstantSource.fixed(now.plusSeconds(2L)), wait)
                            .orElseThrow(),
                    "Next ready at should match the expected value.");
        }
    }

    @Test
    @DisplayName("열기에서 SQL 실패의 원인을 보존한다 — check 1")
    void preserveTheCauseOfTheSQLFailureDuringTheOpeningProcess() {
        final SQLException failure;
        final SmqStorageException thrown;
        failure = new SQLException("injected");
        final var assertionValue2 = captureThrowable(() -> SqliteStorage.open(
                this.dir.resolve("sql-error.db"), Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                    throw failure;
                }));
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue2, "Open should throw SmqStorageException.");
        thrown = (SmqStorageException) (SmqStorageException) assertionValue2;
        final var assertionValue1Argument2 = thrown.getCause();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("열기에서 SQL 실패의 원인을 보존한다 — check 2")
    void preserveTheCauseOfTheSQLFailureDuringTheOpeningProcessCheck2() {
        final SQLException failure;
        failure = new SQLException("injected");
        final var assertionValue2 = captureThrowable(() -> SqliteStorage.open(
                this.dir.resolve("sql-error.db"), Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                    throw failure;
                }));
        final var assertionValue1Argument2 = assertionValue2.getCause();
        Assertions.assertSame(failure, assertionValue1Argument2, "Exception cause should be the same instance.");
    }

    @Test
    @DisplayName("열기의 JDBC 프록시는 원래 RuntimeException을 전달한다 — check 1")
    void theJDBCProxyForOpeningPassesThroughTheOriginalRuntimeException() {
        final IllegalStateException failure;
        failure = new IllegalStateException("injected");
        final var assertionValue2 = captureThrowable(() -> SqliteStorage.open(
                this.dir.resolve("proxy-error.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (p, d) -> (Connection) Proxy.newProxyInstance(
                        Connection.class.getClassLoader(), new Class[] {Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                return null;
                            } else {
                                throw failure;
                            }
                        })));
        Assertions.assertInstanceOf(
                IllegalStateException.class, assertionValue2, "Open should throw IllegalStateException.");
    }

    @Test
    @DisplayName("열기의 JDBC 프록시는 원래 RuntimeException을 전달한다 — check 2")
    void theJDBCProxyForOpeningPassesThroughTheOriginalRuntimeExceptionCheck2() {
        final IllegalStateException failure;
        final IllegalStateException thrown;
        failure = new IllegalStateException("injected");
        final var assertionValue2 = captureThrowable(() -> SqliteStorage.open(
                this.dir.resolve("proxy-error.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (p, d) -> (Connection) Proxy.newProxyInstance(
                        Connection.class.getClassLoader(), new Class[] {Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                return null;
                            } else {
                                throw failure;
                            }
                        })));
        thrown = (IllegalStateException) (IllegalStateException) assertionValue2;
        Assertions.assertSame(failure, thrown, "Thrown should be the same instance.");
    }

    @Test
    @DisplayName("열기에서 Error가 나면 락을 놓고 같은 파일을 다시 연다 — check 1")
    void ifAnErrorOccursDuringOpeningReleaseTheLockAndReopenTheSameFile() {
        final Path db;
        final Error failure;
        db = this.dir.resolve("error.db");
        failure = new LinkageError("injected");
        final var assertionValue2 = captureThrowable(
                () -> SqliteStorage.open(db, Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                    throw failure;
                }));
        Assertions.assertInstanceOf(LinkageError.class, assertionValue2, "Open should throw LinkageError.");
        final var assertionValue3Argument2 = (LinkageError) assertionValue2;
        evaluatePmdArgument(assertionValue3Argument2);
        try (SqliteStorage ignored = SqliteStorage.open(db, Durability.POWER_LOSS)) {}
    }

    @Test
    @DisplayName("열기에서 Error가 나면 락을 놓고 같은 파일을 다시 연다 — check 2")
    void ifAnErrorOccursDuringOpeningReleaseTheLockAndReopenTheSameFileCheck2() {
        final Path db;
        final Error failure;
        db = this.dir.resolve("error.db");
        failure = new LinkageError("injected");
        final var assertionValue2 = captureThrowable(
                () -> SqliteStorage.open(db, Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                    throw failure;
                }));
        final var assertionValue3Argument2 = (LinkageError) assertionValue2;
        Assertions.assertSame(failure, assertionValue3Argument2, "Assert throws should be the same instance.");
        try (SqliteStorage ignored = SqliteStorage.open(db, Durability.POWER_LOSS)) {}
    }

    @Test
    @DisplayName("열기에서 Error가 나면 락을 놓고 같은 파일을 다시 연다 — check 3")
    void ifAnErrorOccursDuringOpeningReleaseTheLockAndReopenTheSameFileCheck3() {
        final Path db;
        final Error failure;
        db = this.dir.resolve("error.db");
        failure = new LinkageError("injected");
        final var assertionValue2 = captureThrowable(
                () -> SqliteStorage.open(db, Durability.POWER_LOSS, Deadline.DEFAULT_LOCK_WAIT_FLOOR, (p, d) -> {
                    throw failure;
                }));
        final var assertionValue3Argument2 = (LinkageError) assertionValue2;
        evaluatePmdArgument(assertionValue3Argument2);
        try (SqliteStorage ignored = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            Assertions.assertNotNull(ignored, "Ignored should be present.");
        }
    }

    @Test
    @DisplayName("새 파일에 스키마 v2와 표식을 기록한다 — new File Has Application Id")
    void newFileHasApplicationId() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            Assertions.assertEquals(
                    String.valueOf(1_397_575_985),
                    this.pragma(c, "application_id"),
                    "Pragma should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 파일에 스키마 v2와 표식을 기록한다 — new File Has Schema Version Two")
    void newFileHasSchemaVersionTwo() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            Assertions.assertEquals("2", this.pragma(c, "user_version"), "Pragma should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 파일에 스키마 v2와 표식을 기록한다 — new File Uses Wal Journal")
    void newFileUsesWalJournal() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            Assertions.assertEquals("wal", this.pragma(c, "journal_mode"), "Pragma should match the expected value.");
        }
    }

    @Test
    @DisplayName("다시 열어도 같은 파일로 열린다")
    void evenIfYouOpenItAgainItOpensAsTheSameFile() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
        }
        try (SqliteStorage s = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            Assertions.assertTrue(s.exists("q"), "Result should exist.");
        }
    }

    @Test
    @DisplayName("smq가 아닌 비어 있지 않은 파일은 열지 않는다")
    void nonEmptyFilesThatAreNotSmqFilesAreNotOpened() throws Exception {
        final Path db;
        db = this.dir.resolve("other.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE foreign_table(x)");
        }
        Assertions.assertThrows(
                IncompatibleStorageException.class,
                () -> SqliteStorage.open(db, Durability.POWER_LOSS),
                "Open should throw IncompatibleStorageException.");
    }

    @Test
    @DisplayName("연결은 WAL synchronous FULL을 쓴다 — connection Uses Wal Journal")
    void connectionUsesWalJournal() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            Assertions.assertEquals(
                    "wal",
                    s.inTransaction(c -> this.pragma(c, "journal_mode")),
                    "In transaction should match the expected value.");
        }
    }

    @Test
    @DisplayName("연결은 WAL synchronous FULL을 쓴다 — connection Uses Full Synchronous Mode")
    void connectionUsesFullSynchronousMode() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            Assertions.assertEquals(
                    "2",
                    s.inTransaction(c -> this.pragma(c, "synchronous")),
                    "In transaction should match the expected value.");
        }
    }

    private Path copyV2SampleAfterIndexRollback() throws Exception {
        final Path db;
        db = this.dir.resolve("v2.db");
        try (InputStream sample = Objects.requireNonNull(
                this.getClass().getResourceAsStream("/schema-samples/schema-v2-before-delayed-index.db"))) {
            Files.copy(sample, db);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            c.setAutoCommit(false);
            SqliteSchema.ensureDelayedIdIndex(c);
            c.rollback();
        }
        return db;
    }

    @Test
    void v2SampleIsAvailable() throws Exception {
        try (InputStream sample =
                this.getClass().getResourceAsStream("/schema-samples/schema-v2-before-delayed-index.db")) {
            Assertions.assertNotNull(sample, "The version 2 sample should be available.");
        }
    }

    @Test
    void rolledBackDelayedIndexIsAbsent() throws Exception {
        final Path db;
        db = this.copyV2SampleAfterIndexRollback();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery("SELECT count(*) FROM sqlite_schema WHERE name = 'message_delayed_id'")) {
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            Assertions.assertEquals(0, rs.getInt(1), "The rolled-back index should be absent.");
        }
    }

    @Test
    void reopeningV2SampleInstallsDelayedIndex() throws Exception {
        final Path db;
        db = this.copyV2SampleAfterIndexRollback();
        try (SqliteStorage storage = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            assertSearchesIndex(
                    explain(
                            storage,
                            "SELECT MIN(id) FROM message INDEXED BY message_delayed_id WHERE queue_id = ? AND available_at IS NOT NULL",
                            1L),
                    "message_delayed_id",
                    "(queue_id=?)",
                    false);
        }
    }

    @Test
    void reopeningV2SamplePreservesSchemaVersion() throws Exception {
        final Path db;
        db = this.copyV2SampleAfterIndexRollback();
        try (SqliteStorage storage = SqliteStorage.open(db, Durability.POWER_LOSS)) {
            Assertions.assertEquals(
                    "2",
                    storage.inTransaction(c -> this.pragma(c, "user_version")),
                    "The schema version should remain 2.");
        }
    }

    private static Stream<Arguments> pullQueryPlans() {
        final List<Object> pullBindings;
        final List<Object> readyBindings;
        pullBindings = List.of(1L, 3, Long.MAX_VALUE, 10);
        readyBindings = List.of("q", "q", 0L, "q", 0L, "q");
        return Stream.of(
                Arguments.of(new Object[] {
                    "SELECT id FROM message WHERE queue_id = ? AND lease_deadline IS NULL AND available_at IS NULL AND delivery_count < ? AND id <= ? ORDER BY id LIMIT ?",
                    pullBindings,
                    "message_stored_ready",
                    "(queue_id=? AND id<?)",
                    false
                }),
                Arguments.of(new Object[] {
                    "SELECT lease_token IS NOT NULL FROM message WHERE queue_id = ? AND lease_deadline IS NULL AND available_at IS NULL AND delivery_count < ? AND id <= ? ORDER BY id LIMIT ?",
                    pullBindings,
                    "message_stored_ready",
                    "(queue_id=? AND id<?)",
                    false
                }),
                Arguments.of(new Object[] {
                    "SELECT id, payload, headers, delivery_count, lease_token IS NOT NULL FROM message WHERE queue_id = ? AND lease_deadline IS NULL AND available_at IS NULL AND delivery_count > 0 AND delivery_count >= ? ORDER BY delivery_count, id LIMIT ?",
                    List.of(1L, 3, 10),
                    "message_redelivered_ready",
                    "(queue_id=? AND delivery_count>?)",
                    false
                }),
                Arguments.of(new Object[] {
                    "SELECT MIN(id) FROM message INDEXED BY message_delayed_id WHERE queue_id = ? AND available_at IS NOT NULL",
                    List.of(1L),
                    "message_delayed_id",
                    "(queue_id=?)",
                    false
                }),
                Arguments.of(new Object[] {
                    "SELECT 1 FROM message INDEXED BY message_lease_deadline WHERE queue_id = ? AND lease_deadline > ? LIMIT 1",
                    List.of(1L, 0L),
                    "message_lease_deadline",
                    "(queue_id=? AND lease_deadline>?)",
                    true
                }),
                Arguments.of(new Object[] {
                    "UPDATE message SET lease_deadline = NULL WHERE queue_id = ? AND lease_deadline <= ?",
                    List.of(1L, 0L),
                    "message_lease_deadline",
                    "(queue_id=? AND lease_deadline<?)",
                    false
                }),
                Arguments.of(new Object[] {
                    "UPDATE message SET available_at = NULL WHERE queue_id = ? AND available_at <= ?",
                    List.of(1L, 0L),
                    "message_available_at",
                    "(queue_id=? AND available_at<?)",
                    false
                }),
                Arguments.of(new Object[] {
                    SqliteStorage.ANY_READY, readyBindings, "message_stored_ready", "(queue_id=?)", false
                }),
                Arguments.of(new Object[] {
                    SqliteStorage.ANY_READY,
                    readyBindings,
                    "message_lease_deadline",
                    "(queue_id=? AND lease_deadline<?)",
                    false
                }),
                Arguments.of(new Object[] {
                    SqliteStorage.ANY_READY,
                    readyBindings,
                    "message_available_at",
                    "(queue_id=? AND available_at<?)",
                    false
                }));
    }

    @ParameterizedTest
    @MethodSource("pullQueryPlans")
    void pullQuerySeeksByStoredStateAndDeadline(
            final String sql,
            final List<Object> bindings,
            final String index,
            final String constraints,
            final boolean covering) {
        try (SqliteStorage storage = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            assertSearchesIndex(explain(storage, sql, bindings.toArray()), index, constraints, covering);
        }
    }

    private static String explain(final SqliteStorage storage, final String sql, final Object... bindings) {
        return storage.inTransaction(c -> {
            final List<String> rows;
            rows = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("EXPLAIN QUERY PLAN " + sql)) {
                for (int i = 0; i < bindings.length; i++) {
                    ps.setObject(i + 1, bindings[i]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(rs.getString("detail"));
                    }
                }
            }
            return String.join("\n", rows);
        });
    }

    private static void assertSearchesIndex(
            final String plan, final String index, final String constraints, final boolean covering) {
        Assertions.assertTrue(
                !plan.contains("SCAN message")
                        && !plan.contains("TEMP B-TREE")
                        && (!covering || plan.contains("USING COVERING INDEX"))
                        && plan.lines()
                                .anyMatch(row -> row.startsWith("SEARCH message USING ")
                                        && row.endsWith("INDEX " + index + " " + constraints)),
                plan);
    }

    @Test
    @DisplayName("파일이 생성된다")
    void aFileIsCreated() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        SqliteStorage.open(db, Durability.POWER_LOSS).close();
        Assertions.assertTrue(Files.exists(db), "Database should exist.");
    }

    @Test
    @DisplayName("발행 배치 중간에 실패하면 저장소가 하나도 반영하지 않는다 — check 1")
    void ifThePublishBatchFailsInTheMiddleNoChangesAreReflectedInTheRepositoryCheck1() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            final NewMessage ok;
            final NewMessage bad;
            final Throwable assertionValue1;
            s.declare("q", QueueOptions.defaults());
            ok = new NewMessage(new byte[] {1}, Map.of());
            bad = new NewMessage(null, Map.of());
            assertionValue1 =
                    captureThrowable(() -> s.publish("q", List.of(ok, bad), InstantSource.fixed(Instant.EPOCH)));
            Assertions.assertInstanceOf(
                    RuntimeException.class, assertionValue1, "Publish should throw RuntimeException.");
        }
    }

    @Test
    @DisplayName("발행 배치 중간에 실패하면 저장소가 하나도 반영하지 않는다 — check 2")
    void ifThePublishBatchFailsInTheMiddleNoChangesAreReflectedInTheRepositoryCheck2() throws Exception {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            final NewMessage ok;
            final NewMessage bad;
            final long assertionValue2Argument2;
            s.declare("q", QueueOptions.defaults());
            ok = new NewMessage(new byte[] {1}, Map.of());
            bad = new NewMessage(null, Map.of());
            evaluate(captureThrowable(() -> s.publish("q", List.of(ok, bad), InstantSource.fixed(Instant.EPOCH))));
            assertionValue2Argument2 = s.counts("q").ready();
            Assertions.assertEquals(0L, assertionValue2Argument2, "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("큐 존재와 옵션 조회는 쓰기 트랜잭션을 열지 않는다 — check 1")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void checkingTheExistenceOfAQueueAndQueryingOptionsDoesNotOpenAWriteTransaction() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("read.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            final var assertionValue5Argument1 = s.exists("q");
            Assertions.assertTrue(assertionValue5Argument1, "Result should exist.");
            final var assertionValue4Argument1 = s.options("q").isPresent();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = s.exists("missing");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument1 = s.options("missing").isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("큐 존재와 옵션 조회는 쓰기 트랜잭션을 열지 않는다 — check 2")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void checkingTheExistenceOfAQueueAndQueryingOptionsDoesNotOpenAWriteTransactionCheck2() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("read.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            final var assertionValue5Argument1 = s.exists("q");
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue4Argument1 = s.options("q").isPresent();
            Assertions.assertTrue(assertionValue4Argument1, "Options should be present.");
            final var assertionValue3Argument1 = s.exists("missing");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument1 = s.options("missing").isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("큐 존재와 옵션 조회는 쓰기 트랜잭션을 열지 않는다 — check 3")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void checkingTheExistenceOfAQueueAndQueryingOptionsDoesNotOpenAWriteTransactionCheck3() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("read.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            final var assertionValue5Argument1 = s.exists("q");
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue4Argument1 = s.options("q").isPresent();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = s.exists("missing");
            Assertions.assertFalse(assertionValue3Argument1, "Result should not exist.");
            final var assertionValue2Argument1 = s.options("missing").isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("큐 존재와 옵션 조회는 쓰기 트랜잭션을 열지 않는다 — check 4")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void checkingTheExistenceOfAQueueAndQueryingOptionsDoesNotOpenAWriteTransactionCheck4() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("read.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            final var assertionValue5Argument1 = s.exists("q");
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue4Argument1 = s.options("q").isPresent();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = s.exists("missing");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument1 = s.options("missing").isEmpty();
            Assertions.assertTrue(assertionValue2Argument1, "Options should be empty.");
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("큐 존재와 옵션 조회는 쓰기 트랜잭션을 열지 않는다 — check 5")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void checkingTheExistenceOfAQueueAndQueryingOptionsDoesNotOpenAWriteTransactionCheck5() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("read.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            final int before;
            s.declare("q", QueueOptions.defaults());
            before = begins.get();
            final var assertionValue5Argument1 = s.exists("q");
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue4Argument1 = s.options("q").isPresent();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = s.exists("missing");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument1 = s.options("missing").isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            Assertions.assertEquals(before, assertionValue1Argument2, "Begins should match the expected value.");
        }
    }

    @Test
    @DisplayName("단일 활성 큐의 활성 리스가 있으면 준비 메시지가 있어도 쓰기 트랜잭션을 열지 않는다 — check 1")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void ifThereIsAnActiveLeaseInASingleActiveQueueAWriteTransactionIsNotOpenedEvenIfThereIsAPrepareMessage() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("single-active-pull.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            final NewMessage message;
            s.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
            message = new NewMessage(new byte[0], Map.of());
            s.publish("q", List.of(message, message), InstantSource.fixed(Instant.EPOCH));
            final var assertionValue3Argument2 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .size();
            Assertions.assertEquals(1, assertionValue3Argument2, "Size should match the expected value.");
            final var assertionValue2Argument1 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("단일 활성 큐의 활성 리스가 있으면 준비 메시지가 있어도 쓰기 트랜잭션을 열지 않는다 — check 2")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void ifThereIsAnActiveLeaseInASingleActiveQueueAWriteTransactionIsNotOpenedEvenIfThereIsAPrepareMessageCheck2() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("single-active-pull.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            final NewMessage message;
            s.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
            message = new NewMessage(new byte[0], Map.of());
            s.publish("q", List.of(message, message), InstantSource.fixed(Instant.EPOCH));
            final var assertionValue3Argument2 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .size();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .isEmpty();
            Assertions.assertTrue(assertionValue2Argument1, "Messages should be empty.");
            final var assertionValue1Argument2 = begins.get();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("단일 활성 큐의 활성 리스가 있으면 준비 메시지가 있어도 쓰기 트랜잭션을 열지 않는다 — check 3")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The proxy records write transactions while forwarding connection and statement calls.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void ifThereIsAnActiveLeaseInASingleActiveQueueAWriteTransactionIsNotOpenedEvenIfThereIsAPrepareMessageCheck3() {
        final AtomicInteger begins;
        begins = new AtomicInteger();
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("single-active-pull.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Statement st;
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (!m.getName().equals("createStatement")) {
                                    return result;
                                }
                                st = (Statement) result;
                                return Proxy.newProxyInstance(
                                        Statement.class.getClassLoader(),
                                        new Class[] {Statement.class},
                                        (p2, m2, a2) -> {
                                            if (m2.getName().equals("execute")
                                                    && a2 != null
                                                    && a2.length > 0
                                                    && "BEGIN IMMEDIATE".equals(a2[0])) {
                                                begins.incrementAndGet();
                                            }
                                            try {
                                                return m2.invoke(st, a2);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            });
                })) {
            final NewMessage message;
            final int beforeBlockedPull;
            s.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
            message = new NewMessage(new byte[0], Map.of());
            s.publish("q", List.of(message, message), InstantSource.fixed(Instant.EPOCH));
            final var assertionValue3Argument2 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .size();
            evaluatePmdArgument(assertionValue3Argument2);
            beforeBlockedPull = begins.get();
            final var assertionValue2Argument1 = s.pull(
                            "q", 1, Duration.ofSeconds(30L), InstantSource.fixed(Instant.EPOCH))
                    .messages()
                    .isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument2 = begins.get();
            Assertions.assertEquals(
                    beforeBlockedPull, assertionValue1Argument2, "Begins should match the expected value.");
        }
    }

    @Test
    void cachedStatementIsReusedAndClosedWithStorageCheck1() throws Exception {
        final List<PreparedStatement> prepared;
        prepared = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(
                this.dir.resolve("statement-cache.db"),
                Durability.PROCESS_CRASH,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                try {
                                    final Object result;
                                    result = m.invoke(real, a);
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
                })) {
            final int assertionValue1Argument2;
            storage.declare("q", QueueOptions.defaults());
            storage.counts("q");
            storage.counts("q");
            assertionValue1Argument2 = prepared.size();
            Assertions.assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
            evaluate(prepared.get(0).isClosed());
        }
        evaluate(prepared.get(0).isClosed());
    }

    @Test
    void cachedStatementIsReusedAndClosedWithStorageCheck2() throws Exception {
        final List<PreparedStatement> prepared;
        prepared = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(
                this.dir.resolve("statement-cache.db"),
                Durability.PROCESS_CRASH,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                try {
                                    final Object result;
                                    result = m.invoke(real, a);
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
                })) {
            final boolean assertionValue2Argument1;
            storage.declare("q", QueueOptions.defaults());
            storage.counts("q");
            storage.counts("q");
            assertionValue2Argument1 = prepared.get(0).isClosed();
            Assertions.assertFalse(assertionValue2Argument1, "Prepared should not be closed.");
        }
        evaluate(prepared.get(0).isClosed());
    }

    @Test
    void cachedStatementIsReusedAndClosedWithStorageCheck3() throws Exception {
        final List<PreparedStatement> prepared;
        final boolean assertionValue3Argument1;
        prepared = new ArrayList<>();
        try (SqliteStorage storage = SqliteStorage.open(
                this.dir.resolve("statement-cache.db"),
                Durability.PROCESS_CRASH,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                try {
                                    final Object result;
                                    result = m.invoke(real, a);
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
                })) {
            storage.declare("q", QueueOptions.defaults());
            storage.counts("q");
            storage.counts("q");
            evaluate(prepared.get(0).isClosed());
        }
        assertionValue3Argument1 = prepared.get(0).isClosed();
        Assertions.assertTrue(assertionValue3Argument1, "Prepared should be closed.");
    }

    @Test
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The test creates a large promotion count and verifies its transfer across the storage boundary.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void pullTransfersLargeReadyCountWithoutOverflow() {
        final long promoted;
        promoted = 2_147_483_648L;
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("large-count.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (m.getName().equals("prepareStatement")
                                        && a != null
                                        && "UPDATE message SET available_at = NULL WHERE queue_id = ? AND available_at <= ?"
                                                .equals(a[0])) {
                                    final PreparedStatement statement;
                                    statement = (PreparedStatement) result;
                                    return Proxy.newProxyInstance(
                                            PreparedStatement.class.getClassLoader(),
                                            new Class[] {PreparedStatement.class},
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
                                } else {
                                    return result;
                                }
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            s.pull("q", 1, Duration.ofSeconds(1L), InstantSource.fixed(Instant.EPOCH));
            Assertions.assertEquals(promoted, s.counts("q").ready(), "Ready should match the expected value.");
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The test creates a large promotion count and verifies its transfer across the storage boundary.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void pullTransfersLargeDelayedCountWithoutOverflow() {
        final long promoted;
        promoted = 2_147_483_648L;
        try (SqliteStorage s = SqliteStorage.open(
                this.dir.resolve("large-count.db"),
                Durability.POWER_LOSS,
                Deadline.DEFAULT_LOCK_WAIT_FLOOR,
                (path, durability) -> {
                    final Connection real;
                    real = SqliteStorage.connect(path, durability);
                    return (Connection) Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class[] {Connection.class}, (p, m, a) -> {
                                final Object result;
                                try {
                                    result = m.invoke(real, a);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                                if (m.getName().equals("prepareStatement")
                                        && a != null
                                        && "UPDATE message SET available_at = NULL WHERE queue_id = ? AND available_at <= ?"
                                                .equals(a[0])) {
                                    final PreparedStatement statement;
                                    statement = (PreparedStatement) result;
                                    return Proxy.newProxyInstance(
                                            PreparedStatement.class.getClassLoader(),
                                            new Class[] {PreparedStatement.class},
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
                                } else {
                                    return result;
                                }
                            });
                })) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            s.pull("q", 1, Duration.ofSeconds(1L), InstantSource.fixed(Instant.EPOCH));
            Assertions.assertEquals(-promoted, s.counts("q").delayed(), "Delayed should match the expected value.");
        }
    }

    @Test
    @DisplayName("손상된 헤더는 꺼내기에서 저장소 오류다")
    void aCorruptedHeaderIsAStorageErrorDuringDequeueing() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("bad.db"), Durability.POWER_LOSS)) {
            s.declare("q", QueueOptions.defaults());
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            s.inTransaction(c -> {
                try (Statement st = c.createStatement()) {
                    st.execute("UPDATE message SET headers = X'000000017fffffff'");
                }
                return null;
            });
            Assertions.assertThrows(
                    SmqStorageException.class,
                    () -> s.pull("q", 1, Duration.ofSeconds(1L), InstantSource.fixed(Instant.EPOCH)),
                    "Pull should throw SmqStorageException.");
        }
    }

    @Test
    @DisplayName("POWER LOSS는 synchronous FULL로 연다")
    void powerLOSSOpensInSynchronousFULLMode() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.POWER_LOSS)) {
            Assertions.assertEquals(
                    "2",
                    s.inTransaction(c -> this.pragma(c, "synchronous")),
                    "In transaction should match the expected value.");
        }
    }

    @Test
    @DisplayName("PROCESS CRASH는 WAL과 함께 synchronous NORMAL로 연다 — process Crash Uses Wal Journal")
    void processCrashUsesWalJournal() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            Assertions.assertEquals(
                    "wal",
                    s.inTransaction(c -> this.pragma(c, "journal_mode")),
                    "In transaction should match the expected value.");
        }
    }

    @Test
    @DisplayName("PROCESS CRASH는 WAL과 함께 synchronous NORMAL로 연다 — process Crash Uses Normal Synchronous Mode")
    void processCrashUsesNormalSynchronousMode() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            Assertions.assertEquals(
                    "1",
                    s.inTransaction(c -> this.pragma(c, "synchronous")),
                    "In transaction should match the expected value.");
        }
    }

    @ParameterizedTest
    @EnumSource(Durability.class)
    @DisplayName("다시 열어도 shm 없이 열리고 synchronous가 내구성 수준대로 걸린다 — check 1")
    void evenIfItIsReopenedItOpensWithoutShmAndSynchronousIsSetAccordingToTheDurabilityLevelCheck1(
            final Durability durability) {
        final Path db;
        final Path shm;
        db = this.dir.resolve("smq.db");
        shm = Path.of(db + "-shm");
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            final String assertionValue1Argument2;
            s.declare("q", QueueOptions.defaults());
            assertionValue1Argument2 = s.inTransaction(c -> this.pragma(c, "journal_mode"));
            Assertions.assertEquals("wal", assertionValue1Argument2, "In transaction should match the expected value.");
        }
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            evaluate(Files.exists(shm));
            evaluate(durability == Durability.POWER_LOSS ? "2" : "1");
            evaluate(s.inTransaction(c -> this.pragma(c, "synchronous")));
            evaluate(s.inTransaction(c -> this.pragma(c, "locking_mode")));
        }
    }

    @ParameterizedTest
    @EnumSource(Durability.class)
    @DisplayName("다시 열어도 shm 없이 열리고 synchronous가 내구성 수준대로 걸린다 — check 2")
    void evenIfItIsReopenedItOpensWithoutShmAndSynchronousIsSetAccordingToTheDurabilityLevelCheck2(
            final Durability durability) {
        final Path db;
        final Path shm;
        db = this.dir.resolve("smq.db");
        shm = Path.of(db + "-shm");
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.declare("q", QueueOptions.defaults());
            evaluate(s.inTransaction(c -> this.pragma(c, "journal_mode")));
        }
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            final boolean assertionValue2Argument1;
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            assertionValue2Argument1 = Files.exists(shm);
            Assertions.assertFalse(assertionValue2Argument1, "다시 열어도 -shm이 생기면 안 된다");
            evaluate(durability == Durability.POWER_LOSS ? "2" : "1");
            evaluate(s.inTransaction(c -> this.pragma(c, "synchronous")));
            evaluate(s.inTransaction(c -> this.pragma(c, "locking_mode")));
        }
    }

    @ParameterizedTest
    @EnumSource(Durability.class)
    @DisplayName("다시 열어도 shm 없이 열리고 synchronous가 내구성 수준대로 걸린다 — check 3")
    void evenIfItIsReopenedItOpensWithoutShmAndSynchronousIsSetAccordingToTheDurabilityLevelCheck3(
            final Durability durability) {
        final Path db;
        final Path shm;
        db = this.dir.resolve("smq.db");
        shm = Path.of(db + "-shm");
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.declare("q", QueueOptions.defaults());
            evaluate(s.inTransaction(c -> this.pragma(c, "journal_mode")));
        }
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            final String expected;
            final String assertionValue3Argument2;
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            evaluate(Files.exists(shm));
            expected = durability == Durability.POWER_LOSS ? "2" : "1";
            assertionValue3Argument2 = s.inTransaction(c -> this.pragma(c, "synchronous"));
            Assertions.assertEquals(
                    expected, assertionValue3Argument2, "In transaction should match the expected value.");
            evaluate(s.inTransaction(c -> this.pragma(c, "locking_mode")));
        }
    }

    @ParameterizedTest
    @EnumSource(Durability.class)
    @DisplayName("다시 열어도 shm 없이 열리고 synchronous가 내구성 수준대로 걸린다 — check 4")
    void evenIfItIsReopenedItOpensWithoutShmAndSynchronousIsSetAccordingToTheDurabilityLevelCheck4(
            final Durability durability) {
        final Path db;
        final Path shm;
        db = this.dir.resolve("smq.db");
        shm = Path.of(db + "-shm");
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            s.declare("q", QueueOptions.defaults());
            evaluate(s.inTransaction(c -> this.pragma(c, "journal_mode")));
        }
        try (SqliteStorage s = SqliteStorage.open(db, durability)) {
            final String assertionValue4Argument2;
            s.publish("q", List.of(message()), InstantSource.fixed(Instant.EPOCH));
            evaluate(Files.exists(shm));
            evaluate(durability == Durability.POWER_LOSS ? "2" : "1");
            evaluate(s.inTransaction(c -> this.pragma(c, "synchronous")));
            assertionValue4Argument2 = s.inTransaction(c -> this.pragma(c, "locking_mode"));
            Assertions.assertEquals(
                    "exclusive", assertionValue4Argument2, "In transaction should match the expected value.");
        }
    }

    private static NewMessage message() {
        return new NewMessage(new byte[] {1}, Map.of());
    }

    @Test
    @DisplayName("locking mode 결과가 exclusive가 아니면 SQLException이다 — lockingModeAcceptsExclusive")
    void lockingModeAcceptsExclusive() {
        Assertions.assertDoesNotThrow(
                () -> SqliteStorage.requireExclusive("exclusive"),
                "Require exclusive should complete without an exception.");
    }

    @Test
    @DisplayName("locking mode 결과가 exclusive가 아니면 SQLException이다 — lockingModeAcceptsUppercaseExclusive")
    void lockingModeAcceptsUppercaseExclusive() {
        Assertions.assertDoesNotThrow(
                () -> SqliteStorage.requireExclusive("EXCLUSIVE"),
                "Require exclusive should complete without an exception.");
    }

    @Test
    @DisplayName("locking mode 결과가 exclusive가 아니면 SQLException이다 — lockingModeRejectsNormal")
    void lockingModeRejectsNormal() {
        Assertions.assertThrows(
                SQLException.class,
                () -> SqliteStorage.requireExclusive("normal"),
                "Require exclusive should throw SQLException.");
    }

    @Test
    @DisplayName("locking mode 결과가 exclusive가 아니면 SQLException이다 — lockingModeRejectsNull")
    void lockingModeRejectsNull() {
        Assertions.assertThrows(
                SQLException.class,
                () -> SqliteStorage.requireExclusive(null),
                "Require exclusive should throw SQLException.");
    }

    @Test
    @DisplayName("연결을 열면 locking mode가 exclusive다")
    void whenAConnectionIsOpenedTheLockingModeIsExclusive() throws Exception {
        try (Connection c = SqliteStorage.connect(this.dir.resolve("x.db"), Durability.POWER_LOSS);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA locking_mode")) {
            if (!rs.next()) {
                throw new AssertionError("Expected a query result row");
            }
            Assertions.assertEquals("exclusive", rs.getString(1), "String should match the expected value.");
        }
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
