package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.NoDeadLetterQueueException;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueDeclarationConflictException;
import io.github.ppzxc.smq.QueueInUseException;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.storage.DeadLetters;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Extended;
import io.github.ppzxc.smq.storage.LeaseToken;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import io.github.ppzxc.smq.storage.Result;
import io.github.ppzxc.smq.storage.Storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * SQLite 파일 하나를 쓰는 저장소. 이 클래스에는 열기 순서, 큐 연산 SQL, row codec이 있다.
 * 한 JVM만 파일을 열 수 있다. 열 때 {@link StorageLock}으로 락 파일을 잡고 연결을 {@code locking_mode=EXCLUSIVE}로
 * 열며, 이미 누가 쥐고 있으면 기다리지 않고 {@code StorageLockedException}이다(ADR-0012).
 * 단일 연결과 단일 lock은 {@link TransactionExecutor}가 소유한다. 큐 연산은 모두 {@link #inTransaction}으로 그 실행기의
 * 쓰기 트랜잭션({@code BEGIN IMMEDIATE})을 거친다.
 * 읽기 경로는 따로 있다. 꺼내기 대기용 {@link #anyReady}는 같은 실행기의 autocommit 읽기를 써서 쓰기 락 없이
 * 보고, 확실하지 않으면 "있다"고 답해 쓰기 경로가 다시 확인하게 한다.
 * 열 때의 스키마 판정(생성, 거부, 업그레이드)은 상태 없는 {@link SqliteSchema}가 맡는다.
 */
public final class SqliteStorage implements Storage {

    private static final System.Logger LOG = System.getLogger(SqliteStorage.class.getName());

    /** 'SMQ1'. smq 파일임을 알리는 {@code application_id}. */
    public static final int APPLICATION_ID = SqliteSchema.APPLICATION_ID;

    public static final int SCHEMA_VERSION = 2;

    private static final String STORED_READY = "lease_deadline IS NULL AND available_at IS NULL";

    static final String PROMOTE_EXPIRED_LEASES =
            "UPDATE message SET lease_deadline = NULL WHERE queue_id = ? AND lease_deadline <= ?";
    static final String PROMOTE_DUE_DELAYS =
            "UPDATE message SET available_at = NULL WHERE queue_id = ? AND available_at <= ?";

    private static final String QUEUE_ID_BY_NAME = "(SELECT id FROM queue WHERE name = ?)";
    static final String ANY_READY = """
            SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = %1$s AND %2$s)
                OR EXISTS(SELECT 1 FROM message WHERE queue_id = %1$s AND lease_deadline <= ?)
                OR EXISTS(SELECT 1 FROM message WHERE queue_id = %1$s AND available_at <= ?),
                EXISTS(SELECT 1 FROM queue WHERE name = ?)
            """.replace("%1$s", QUEUE_ID_BY_NAME).replace("%2$s", STORED_READY);

    private static final String STORED_READY_FROM_WHERE = " FROM message WHERE queue_id = ? AND " + STORED_READY;
    private static final String PULLABLE_FROM_WHERE =
            STORED_READY_FROM_WHERE + " AND delivery_count < ? AND id <= ? ORDER BY id LIMIT ?";

    static final String SELECT_DELAY_BARRIER =
            "SELECT MIN(id) FROM message INDEXED BY message_delayed_id WHERE queue_id = ? AND available_at IS NOT NULL";

    static final String SELECT_PULLABLE = "SELECT id" + PULLABLE_FROM_WHERE;
    static final String SELECT_PULLABLE_STATE = "SELECT lease_token IS NOT NULL" + PULLABLE_FROM_WHERE;

    static final String SELECT_EXHAUSTED = "SELECT id, payload, headers, delivery_count, lease_token IS NOT NULL"
            + STORED_READY_FROM_WHERE
            + " AND delivery_count > 0 AND delivery_count >= ? ORDER BY delivery_count, id LIMIT ?";

    private static final SqliteSchema SCHEMA = new SqliteSchema(SCHEMA_VERSION, Map.of(1, SqliteSchema::upgradeV1ToV2));

    private final TransactionExecutor executor;
    private final Duration lockWaitFloor;
    private final StorageLock storageLock;
    private final Path path;
    private final QueueCounters counters;
    private volatile boolean closed;

    private SqliteStorage(
            Path path,
            TransactionExecutor executor,
            Duration lockWaitFloor,
            StorageLock storageLock,
            QueueCounters counters) {
        this.path = path;
        this.counters = counters;
        this.executor = executor;
        this.lockWaitFloor = lockWaitFloor;
        this.storageLock = storageLock;
    }

    public static SqliteStorage open(Path path, Durability durability) {
        return open(path, durability, Deadline.DEFAULT_LOCK_WAIT_FLOOR);
    }

    /**
     * 연산 lock 대기 하한을 정해 저장소 경합 중 빈 청크를 피한다.
     *
     * @param lockWaitFloor 꺼내기가 {@code wait}와 상관없이 저장소의 lock을 최소한 이만큼은 기다리는 시간(ADR-0016).
     *     0 이상이다. 공개 설정이 아니므로({@code Smq}의 빌더는 기본 100ms를 쓴다) 이 값을 바꿔 쓰는 곳은 테스트다.
     */
    public static SqliteStorage open(Path path, Durability durability, Duration lockWaitFloor) {
        return open(path, durability, lockWaitFloor, true);
    }

    public static SqliteStorage open(Path path, Durability durability, Duration lockWaitFloor, boolean autoVacuum) {
        return open(path, durability, lockWaitFloor, SqliteStorage::connect, autoVacuum);
    }

    @FunctionalInterface
    interface Connector {
        Connection connect(Path path, Durability durability) throws SQLException;
    }

    static SqliteStorage open(Path path, Durability durability, Duration lockWaitFloor, Connector connector) {
        return open(path, durability, lockWaitFloor, connector, true);
    }

    @PolicyException(
            rule = "PMD.PreserveStackTrace",
            reason = "A non-database file is reported as a domain error without exposing the SQLite cause.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.UseTryWithResources",
            reason = "The connection transfers to the storage object, which closes it when the storage closes.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.PreserveStackTrace"})
    private static SqliteStorage open(
            Path path, Durability durability, Duration lockWaitFloor, Connector connector, boolean autoVacuum) {
        if (lockWaitFloor.isNegative()) {
            throw new IllegalArgumentException("lockWaitFloor must not be negative: " + lockWaitFloor);
        }
        StorageLock storageLock = StorageLock.acquire(path);
        @Nullable Connection connection = null;
        boolean success = false;
        try {
            connection = StatementCachingConnection.wrap(connector.connect(path, durability));
            SqliteSchema.configureNewDatabase(connection, autoVacuum);
            TransactionExecutor executor = new TransactionExecutor(connection);
            executor.execute(c -> {
                int version = SCHEMA.establish(c);
                SqliteSchema.ensureDelayedIdIndex(c);
                return version;
            });

            executor.executeOutsideTransaction(() -> {
                try (Statement s = executor.connection().createStatement()) {
                    TransactionExecutor.configureWal(s);
                    return Boolean.TRUE;
                }
            });
            QueueCounters counters = new QueueCounters();
            executor.execute(c -> {
                loadCounters(c, counters);
                return Boolean.TRUE;
            });
            SqliteStorage storage = new SqliteStorage(path, executor, lockWaitFloor, storageLock, counters);
            success = true;
            return storage;
        } catch (SQLException | RuntimeException e) {
            if (isNotADatabase(e)) {
                throw SCHEMA.notSmq("not a SQLite file");
            }
            if (isBusyCause(e)) {
                throw StorageLock.locked(path, StorageLock.lockPathOf(path), e);
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new SmqStorageException("cannot open " + path, e);
        } finally {
            if (!success) {
                try {
                    closeQuietly(connection);
                } finally {
                    storageLock.close();
                }
            }
        }
    }

    @PolicyException(
            rule = "PMD.UseTryWithResources",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UseTryWithResources")
    static Connection connect(Path path, Durability durability) throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(0);
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties());
        boolean ok = false;
        try (Statement s = connection.createStatement()) {

            try (ResultSet rs = s.executeQuery("PRAGMA locking_mode = EXCLUSIVE")) {
                requireExclusive(rs.next() ? rs.getString(1) : null);
            }

            s.execute("PRAGMA synchronous = "
                    + switch (durability) {
                        case POWER_LOSS -> "FULL";
                        case PROCESS_CRASH -> "NORMAL";
                    });
            ok = true;
        } finally {
            if (!ok) {
                closeQuietly(connection);
            }
        }
        return connection;
    }

    static void requireExclusive(@Nullable String mode) throws SQLException {
        if (!"exclusive".equalsIgnoreCase(mode)) {
            throw new SQLException("locking_mode=EXCLUSIVE was not applied: " + mode);
        }
    }

    private static boolean isBusyCause(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException se && TransactionExecutor.isBusy(se)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotADatabase(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLiteException se && se.getResultCode() == SQLiteErrorCode.SQLITE_NOTADB) {
                return true;
            }
        }
        return false;
    }

    <T> T inTransaction(TransactionExecutor.TxBody<T> body) {
        return inTransaction(body, Deadline.NONE);
    }

    private <T> T inTransaction(TransactionExecutor.TxBody<T> body, Deadline deadline) {
        return executor.execute(body, deadline);
    }

    @FunctionalInterface
    private interface CountingBody<T> {
        T apply(Connection connection, QueueCounters.Tx tx) throws SQLException;
    }

    private <T> T inCountingTransaction(CountingBody<T> body) {
        return inCountingTransaction(body, Deadline.NONE);
    }

    private <T> T inCountingTransaction(CountingBody<T> body, Deadline deadline) {
        QueueCounters.Tx tx = counters.begin();
        return executor.execute(c -> body.apply(c, tx), deadline, tx::apply);
    }

    private static void loadCounters(Connection c, QueueCounters counters) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("""
                        SELECT q.id, q.name,
                               COALESCE(SUM(m.lease_token IS NULL AND m.available_at IS NULL), 0),
                               COALESCE(SUM(m.lease_token IS NOT NULL), 0),
                               COALESCE(SUM(m.lease_token IS NULL AND m.available_at IS NOT NULL), 0)
                        FROM queue q LEFT JOIN message m ON m.queue_id = q.id
                        GROUP BY q.id, q.name""")) {
            while (rs.next()) {
                counters.register(
                        rs.getString(2), rs.getLong(1), new QueueCounts(rs.getLong(3), rs.getLong(4), rs.getLong(5)));
            }
        }
    }

    @Override
    public void declare(String queue, QueueOptions options) {
        inCountingTransaction((c, tx) -> {
            if (findQueue(c, queue) != null) {
                QueueOptions existing = readOptions(c, queue);
                if (!existing.equals(options)) {
                    throw new QueueDeclarationConflictException(queue, existing, options);
                }
                return true;
            }
            Long dlqId = null;
            if (options.deadLetterQueue().isPresent()) {
                dlqId = requireQueue(c, options.deadLetterQueue().get());
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO queue(name, max_deliveries, dead_letter_queue_id, single_active_consumer)
                    VALUES (?, ?, ?, ?)""")) {
                ps.setString(1, queue);
                if (options.maxDeliveries().isPresent()) {
                    ps.setInt(2, options.maxDeliveries().getAsInt());
                } else {
                    ps.setNull(2, java.sql.Types.INTEGER);
                }
                if (dlqId != null) {
                    ps.setLong(3, dlqId);
                } else {
                    ps.setNull(3, java.sql.Types.INTEGER);
                }
                ps.setInt(4, options.singleActiveConsumer() ? 1 : 0);
                ps.executeUpdate();
            }
            tx.declared(queue, requireQueue(c, queue));
            return true;
        });
    }

    @Override
    public boolean delete(String queue) {
        return inCountingTransaction((c, tx) -> {
            Long queueId = findQueue(c, queue);
            if (queueId == null) {
                return false;
            }
            try (PreparedStatement ps =
                    c.prepareStatement("SELECT name FROM queue WHERE dead_letter_queue_id = ? ORDER BY id LIMIT 1")) {
                ps.setLong(1, queueId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new QueueInUseException(queue, rs.getString(1));
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM message WHERE queue_id = ?")) {
                ps.setLong(1, queueId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM queue WHERE id = ?")) {
                ps.setLong(1, queueId);
                ps.executeUpdate();
            }
            tx.deleted(queue, queueId);
            return true;
        });
    }

    @Override
    public boolean exists(String queue) {
        return executor.read(c -> findQueue(c, queue) != null);
    }

    @Override
    public Optional<QueueOptions> options(String queue) {
        return executor.read(
                c -> findQueue(c, queue) == null ? Optional.<QueueOptions>empty() : Optional.of(readOptions(c, queue)));
    }

    private static QueueOptions readOptions(Connection c, String queue) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT q.max_deliveries, d.name, q.single_active_consumer
                FROM queue q LEFT JOIN queue d ON d.id = q.dead_letter_queue_id
                WHERE q.name = ?""")) {
            ps.setString(1, queue);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new QueueNotFoundException(queue);
                }
                int max = rs.getInt(1);
                OptionalInt maxDeliveries = rs.wasNull() ? OptionalInt.empty() : OptionalInt.of(max);
                return new QueueOptions(maxDeliveries, Optional.ofNullable(rs.getString(2)), rs.getInt(3) != 0);
            }
        }
    }

    static final String SELECT_ACTIVE_LEASE = "SELECT 1 FROM message INDEXED BY message_lease_deadline"
            + " WHERE queue_id = ? AND lease_deadline > ? LIMIT 1";

    private static boolean hasActiveLease(Connection c, long queueId, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(SELECT_ACTIVE_LEASE)) {
            ps.setLong(1, queueId);
            ps.setLong(2, toNanos(now));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static @Nullable Long findQueue(Connection c, String queue) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT id FROM queue WHERE name = ?")) {
            ps.setString(1, queue);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    private static long requireQueue(Connection c, String queue) throws SQLException {
        Long id = findQueue(c, queue);
        if (id == null) {
            throw new QueueNotFoundException(queue);
        }
        return id;
    }

    /**
     * 메모리 개수를 읽는다. DB도 쓰기 락도 쓰지 않는다. 기한이 지난 리스와 끝난 지연은 다음에 꺼낼 때 반영된다(ADR-0018).
     */
    @Override
    public QueueCounts counts(String queue) {
        if (closed) {
            throw new IllegalStateException("storage is closed");
        }
        QueueCounts counts = counters.get(queue);
        if (counts == null) {
            throw new QueueNotFoundException(queue);
        }
        return counts;
    }

    @Override
    public List<MessageId> publish(String queue, List<NewMessage> batch, InstantSource clock) {
        return inCountingTransaction((c, tx) -> {
            Instant now = clock.instant();
            long queueId = requireQueue(c, queue);
            List<MessageId> ids = new ArrayList<>(batch.size());
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO message(queue_id, payload, headers, published_at, available_at) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                for (NewMessage m : batch) {
                    ps.setLong(1, queueId);
                    ps.setBytes(2, m.payload());
                    ps.setBytes(3, encodeHeaders(m.headers()));
                    ps.setLong(4, toNanos(now));
                    Instant availableAt = m.availableAt(now);
                    setInstantOrNull(ps, 5, availableAt);
                    ps.executeUpdate();
                    if (availableAt == null) {
                        tx.ready(queueId, 1);
                    } else {
                        tx.delayed(queueId, 1);
                    }
                    try (ResultSet keys = ps.getGeneratedKeys()) {
                        keys.next();
                        ids.add(new MessageId(keys.getLong(1)));
                    }
                }
            }
            return ids;
        });
    }

    private boolean anyReady(String queue, InstantSource clock, Deadline waitDeadline) {
        return executor.anyOrUncertain(
                connection -> {
                    Instant now = clock.instant();
                    long queueId = 0;
                    boolean queueExists = false;
                    boolean singleActiveConsumer = false;
                    try (PreparedStatement ps = connection.prepareStatement(
                            "SELECT id, single_active_consumer FROM queue WHERE name = ?")) {
                        ps.setString(1, queue);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                queueId = rs.getLong(1);
                                queueExists = true;
                                singleActiveConsumer = rs.getInt(2) != 0;
                            }
                        }
                    }
                    if (queueExists && singleActiveConsumer && hasActiveLease(connection, queueId, now)) {
                        return false;
                    }
                    try (PreparedStatement ps = connection.prepareStatement(ANY_READY)) {
                        long nowNanos = toNanos(now);
                        ps.setString(1, queue);
                        ps.setString(2, queue);
                        ps.setLong(3, nowNanos);
                        ps.setString(4, queue);
                        ps.setLong(5, nowNanos);
                        ps.setString(6, queue);
                        try (ResultSet rs = ps.executeQuery()) {

                            return !rs.next() || rs.getInt(1) == 1 || rs.getInt(2) == 0;
                        }
                    }
                },
                waitDeadline);
    }

    @Override
    public PullResult pull(String queue, int max, Duration lease, InstantSource clock) {
        return pull(queue, max, lease, clock, Deadline.NONE, new HashSet<>());
    }

    /** {@code wait} 안에 lock이나 쓰기 트랜잭션을 잡지 못하면 아무것도 반영하지 않고 빈 결과를 돌려준다(ADR-0016). */
    @Override
    public PullResult pull(String queue, int max, Duration lease, InstantSource clock, Duration wait)
            throws InterruptedException {
        return pull(queue, max, lease, clock, wait, _ -> {});
    }

    @Override
    @PolicyException(
            rule = "PMD.PreserveStackTrace",
            reason = "The original InterruptedException is rethrown; deadline expiry returns an empty result.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.PreserveStackTrace")
    public PullResult pull(
            String queue,
            int max,
            Duration lease,
            InstantSource clock,
            Duration wait,
            Consumer<Optional<String>> deadLetter)
            throws InterruptedException {
        try {
            Set<String> destinations = new HashSet<>();
            PullResult result = pull(queue, max, lease, clock, Deadline.after(wait, lockWaitFloor), destinations);
            destinations.forEach(name -> deadLetter.accept(Optional.of(name)));
            return result;
        } catch (Deadline.Interrupted e) {
            throw (InterruptedException) e.getCause();
        } catch (Deadline.Expired e) {
            return PullResult.empty();
        }
    }

    @Override
    public boolean supportsQueueWakeups() {
        return true;
    }

    @Override
    @PolicyException(
            rule = "PMD.PreserveStackTrace",
            reason = "The original InterruptedException is rethrown; deadline expiry returns no ready time.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.PreserveStackTrace")
    public Optional<Instant> nextReadyAt(String queue, InstantSource clock, Duration wait) throws InterruptedException {
        try {
            return executor.read(
                    c -> {
                        long now = toNanos(clock.instant());
                        try (PreparedStatement ps = c.prepareStatement("""
                        WITH candidates AS (
                          SELECT
                            q.single_active_consumer AS serial,
                            CASE WHEN q.single_active_consumer AND q.max_deliveries IS NOT NULL THEN
                              (SELECT MIN(available_at) FROM message INDEXED BY message_delayed_exhausted
                               WHERE queue_id = q.id AND available_at > ?1 AND delivery_count > 0
                                 AND delivery_count >= q.max_deliveries)
                            END AS exhausted_delay,
                            (SELECT MIN(lease_deadline) FROM message
                             WHERE queue_id = q.id AND lease_deadline > ?1) AS first_lease,
                            (SELECT MAX(lease_deadline) FROM message
                             WHERE queue_id = q.id AND lease_deadline > ?1) AS last_lease,
                            CASE WHEN q.single_active_consumer THEN
                              (SELECT available_at FROM message INDEXED BY message_delayed_id
                               WHERE queue_id = q.id AND available_at IS NOT NULL ORDER BY id LIMIT 1)
                            ELSE
                              (SELECT MIN(available_at) FROM message
                               WHERE queue_id = q.id AND available_at > ?1)
                            END AS delay
                          FROM queue q WHERE q.name = ?2
                        )
                        SELECT CASE
                          WHEN serial AND last_lease IS NOT NULL THEN last_lease
                          ELSE (SELECT MIN(at) FROM (
                            SELECT first_lease AS at UNION ALL SELECT delay UNION ALL SELECT exhausted_delay
                          ))
                        END FROM candidates""")) {
                            ps.setLong(1, now);
                            ps.setString(2, queue);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    return Optional.<Instant>empty();
                                }
                                long value = rs.getLong(1);
                                return rs.wasNull() ? Optional.<Instant>empty() : Optional.of(fromNanos(value));
                            }
                        }
                    },
                    Deadline.after(wait, Duration.ZERO));
        } catch (Deadline.Interrupted e) {
            throw (InterruptedException) e.getCause();
        } catch (Deadline.Expired e) {
            return Optional.empty();
        }
    }

    private PullResult pull(
            String queue,
            int max,
            Duration lease,
            InstantSource clock,
            Deadline waitDeadline,
            Set<String> destinations) {
        if (!anyReady(queue, clock, waitDeadline)) {
            return PullResult.empty();
        }
        return inCountingTransaction(
                (c, tx) -> {
                    Instant now = clock.instant();
                    long queueId = requireQueue(c, queue);
                    QueueOptions options = readOptions(c, queue);

                    if (options.singleActiveConsumer() && hasActiveLease(c, queueId, now)) {
                        return PullResult.empty();
                    }
                    promote(c, tx, queueId, toNanos(now));
                    int limit = options.maxDeliveries().orElse(Integer.MAX_VALUE);
                    MoveResult moved = options.maxDeliveries().isPresent()
                            ? moveExhausted(c, tx, queue, queueId, options, limit, now)
                            : new MoveResult(false, false);
                    if (moved.moved()) {
                        destinations.add(options.deadLetterQueue().orElseThrow());
                    }
                    long barrier = Long.MAX_VALUE;
                    if (options.singleActiveConsumer()) {
                        try (PreparedStatement ps = c.prepareStatement(SELECT_DELAY_BARRIER)) {
                            ps.setLong(1, queueId);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) {
                                    long id = rs.getLong(1);
                                    if (!rs.wasNull()) {
                                        barrier = id - 1;
                                    }
                                }
                            }
                        }
                    }

                    long fromReady = 0;
                    long picked = 0;
                    try (PreparedStatement peek = c.prepareStatement(SELECT_PULLABLE_STATE)) {
                        peek.setLong(1, queueId);
                        peek.setInt(2, limit);
                        peek.setLong(3, barrier);
                        peek.setInt(4, max);
                        try (ResultSet rs = peek.executeQuery()) {
                            while (rs.next()) {
                                picked++;

                                if (rs.getInt(1) == 0) {
                                    fromReady++;
                                }
                            }
                        }
                    }
                    if (picked == 0) {
                        return moved.remain()
                                ? PullResult.emptyDeadLettersRemain(moved.moved())
                                : PullResult.empty(moved.moved());
                    }
                    tx.ready(queueId, -fromReady);
                    tx.leased(queueId, fromReady);
                    long token;
                    try (Statement s = c.createStatement();
                            ResultSet rs = s.executeQuery(
                                    "UPDATE meta SET value = value + 1 WHERE key = 'lease_seq' RETURNING value")) {
                        if (!rs.next()) {
                            throw new SQLException("lease sequence row missing");
                        }
                        token = rs.getLong(1);
                    }
                    Instant deadline = now.plus(lease);
                    List<Message> messages = new ArrayList<>();
                    try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE message SET lease_token = ?, lease_deadline = ?, delivery_count = delivery_count + 1
                    WHERE id IN (""" + SELECT_PULLABLE + """
                    )
                    RETURNING id, payload, headers, delivery_count, published_at""")) {
                        ps.setLong(1, token);
                        ps.setLong(2, toNanos(deadline));
                        ps.setLong(3, queueId);
                        ps.setInt(4, limit);
                        ps.setLong(5, barrier);
                        ps.setInt(6, max);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                messages.add(new Message(
                                        new MessageId(rs.getLong(1)),
                                        rs.getBytes(2),
                                        decodeHeaders(rs.getBytes(3)),
                                        rs.getInt(4),
                                        fromNanos(rs.getLong(5))));
                            }
                        }
                    }
                    messages.sort((a, b) -> a.id().compareTo(b.id()));
                    return PullResult.of(new LeaseToken(token), deadline, messages, moved.moved());
                },
                waitDeadline);
    }

    private static void promote(Connection c, QueueCounters.Tx tx, long queueId, long nowNanos) throws SQLException {

        try (PreparedStatement ps = c.prepareStatement(PROMOTE_EXPIRED_LEASES)) {
            ps.setLong(1, queueId);
            ps.setLong(2, nowNanos);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement(PROMOTE_DUE_DELAYS)) {
            ps.setLong(1, queueId);
            ps.setLong(2, nowNanos);
            long promoted = ps.executeLargeUpdate();
            tx.delayed(queueId, -promoted);
            tx.ready(queueId, promoted);
        }
    }

    private static MoveResult moveExhausted(
            Connection c, QueueCounters.Tx tx, String queue, long queueId, QueueOptions options, int limit, Instant now)
            throws SQLException {
        long dlqId = requireQueue(c, options.deadLetterQueue().orElseThrow());
        List<Exhausted> found = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(SELECT_EXHAUSTED)) {
            ps.setLong(1, queueId);
            ps.setInt(2, limit);
            ps.setInt(3, DeadLetters.MAX_MOVES_PER_PULL + 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    found.add(new Exhausted(
                            rs.getLong(1),
                            rs.getBytes(2),
                            decodeHeaders(rs.getBytes(3)),
                            rs.getInt(4),
                            rs.getInt(5) != 0));
                }
            }
        }

        boolean deadLettersRemain = found.size() > DeadLetters.MAX_MOVES_PER_PULL;
        List<Exhausted> moving = found.subList(0, Math.min(found.size(), DeadLetters.MAX_MOVES_PER_PULL));
        try (PreparedStatement insert = c.prepareStatement("""
                INSERT INTO message(queue_id, payload, headers, delivery_count, published_at)
                VALUES (?, ?, ?, 0, ?)""");
                PreparedStatement delete = c.prepareStatement("DELETE FROM message WHERE id = ?")) {
            for (Exhausted e : moving) {
                insert.setLong(1, dlqId);
                insert.setBytes(2, e.payload());
                insert.setBytes(
                        3,
                        encodeHeaders(DeadLetters.headers(
                                new DeadLetters.Origin(queue, new MessageId(e.id()), e.headers(), e.deliveryCount()),
                                DeadLetters.Reason.MAX_DELIVERIES,
                                now)));
                insert.setLong(4, toNanos(now));
                insert.executeUpdate();
                delete.setLong(1, e.id());
                delete.executeUpdate();

                if (e.leased()) {
                    tx.leased(queueId, -1);
                } else {
                    tx.ready(queueId, -1);
                }
                tx.ready(dlqId, 1);
            }
        }
        return new MoveResult(deadLettersRemain, !moving.isEmpty());
    }

    private record MoveResult(boolean remain, boolean moved) {}

    @SuppressWarnings("ArrayRecordComponent") // This private record carries a database-owned payload.
    private record Exhausted(long id, byte[] payload, Map<String, String> headers, int deliveryCount, boolean leased) {}

    @Override
    public List<Result> settle(LeaseToken token, List<Disposition> items, InstantSource clock) {
        return settle(token, items, clock, _ -> {});
    }

    @Override
    @PolicyException(
            rule = "PMD.ExhaustiveSwitchHasDefault",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
    public List<Result> settle(
            LeaseToken token, List<Disposition> items, InstantSource clock, Consumer<Optional<String>> deadLetter) {
        Set<String> destinations = new HashSet<>();
        List<Result> settled = inCountingTransaction((c, tx) -> {
            Instant now = clock.instant();
            List<Result> results = new ArrayList<>(items.size());
            try (PreparedStatement ack = c.prepareStatement("DELETE FROM message WHERE id = ? AND lease_token = ?");
                    PreparedStatement ackFrom = c.prepareStatement(
                            "DELETE FROM message WHERE id = ? AND lease_token = ? RETURNING queue_id");
                    PreparedStatement find = c.prepareStatement("""
                            SELECT m.queue_id, q.name, q.dead_letter_queue_id, m.payload, m.headers, m.delivery_count, dlq.name
                            FROM message m JOIN queue q ON q.id = m.queue_id
                            LEFT JOIN queue dlq ON dlq.id = q.dead_letter_queue_id
                            WHERE m.id = ? AND m.lease_token = ?""");
                    PreparedStatement insert = c.prepareStatement("""
                            INSERT INTO message(queue_id, payload, headers, delivery_count, published_at)
                            VALUES (?, ?, ?, 0, ?)""");
                    PreparedStatement nack = c.prepareStatement("""
                            UPDATE message SET lease_token = NULL, lease_deadline = NULL, available_at = ?
                            WHERE id = ? AND lease_token = ?
                            RETURNING queue_id""")) {
                for (Disposition d : items) {
                    int updated;
                    switch (d.kind()) {
                        case ACK -> {
                            ackFrom.setLong(1, d.id().value());
                            ackFrom.setLong(2, token.value());
                            try (ResultSet rs = ackFrom.executeQuery()) {
                                updated = rs.next() ? 1 : 0;
                                if (updated == 1) {
                                    tx.leased(rs.getLong(1), -1);
                                }
                            }
                        }
                        case NACK -> {
                            Instant availableAt = d.availableAt(now);
                            setInstantOrNull(nack, 1, availableAt);
                            nack.setLong(2, d.id().value());
                            nack.setLong(3, token.value());
                            try (ResultSet rs = nack.executeQuery()) {
                                updated = rs.next() ? 1 : 0;
                                if (updated == 1) {
                                    long queueId = rs.getLong(1);
                                    tx.leased(queueId, -1);
                                    if (availableAt == null) {
                                        tx.ready(queueId, 1);
                                    } else {
                                        tx.delayed(queueId, 1);
                                    }
                                }
                            }
                        }
                        case REJECT -> updated = reject(find, insert, ack, tx, token, d, now, destinations);
                        default -> throw new IllegalStateException("unknown disposition: " + d.kind());
                    }
                    results.add(new Result(d.id(), updated == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return results;
        });
        destinations.forEach(name -> deadLetter.accept(Optional.of(name)));
        return settled;
    }

    private static int reject(
            PreparedStatement find,
            PreparedStatement insert,
            PreparedStatement delete,
            QueueCounters.Tx tx,
            LeaseToken token,
            Disposition d,
            Instant now,
            Set<String> destinations)
            throws SQLException {
        find.setLong(1, d.id().value());
        find.setLong(2, token.value());
        long sourceId;
        String queueName;
        String destination;
        long dlqId;
        byte[] payload;
        Map<String, String> headers;
        int deliveryCount;
        try (ResultSet rs = find.executeQuery()) {
            if (!rs.next()) {
                return 0;
            }
            sourceId = rs.getLong(1);
            queueName = rs.getString(2);
            dlqId = rs.getLong(3);
            if (rs.wasNull()) {
                throw new NoDeadLetterQueueException(queueName);
            }
            payload = rs.getBytes(4);
            headers = decodeHeaders(rs.getBytes(5));
            deliveryCount = rs.getInt(6);
            destination = rs.getString(7);
        }
        insert.setLong(1, dlqId);
        insert.setBytes(2, payload);
        insert.setBytes(
                3,
                encodeHeaders(DeadLetters.headers(
                        new DeadLetters.Origin(queueName, d.id(), headers, deliveryCount),
                        DeadLetters.Reason.REJECTED,
                        now)));
        insert.setLong(4, toNanos(now));
        insert.executeUpdate();
        delete.setLong(1, d.id().value());
        delete.setLong(2, token.value());
        int deleted = delete.executeUpdate();
        if (deleted == 1) {
            destinations.add(destination);
            tx.leased(sourceId, -1);
            tx.ready(dlqId, 1);
        }
        return deleted;
    }

    @Override
    public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, InstantSource clock) {
        return inTransaction(c -> {
            Instant now = clock.instant();
            Instant deadline = now.plus(d);
            List<Result> results = new ArrayList<>(ids.size());

            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE message SET lease_deadline = ?
                    WHERE id = ? AND lease_token = ?
                      AND (lease_deadline > ?
                           OR NOT EXISTS (SELECT 1 FROM queue q
                                          WHERE q.id = message.queue_id AND q.single_active_consumer = 1))""")) {
                for (MessageId id : ids) {
                    ps.setLong(1, toNanos(deadline));
                    ps.setLong(2, id.value());
                    ps.setLong(3, token.value());
                    ps.setLong(4, toNanos(now));
                    results.add(new Result(id, ps.executeUpdate() == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return new Extended(deadline, results);
        });
    }

    @Override
    @PolicyException(
            rule = "PMD.ExceptionAsFlowControl",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.ExceptionAsFlowControl")
    public void snapshot(Path target) {
        try {
            Path realTarget = realPathOf(target);
            Path realDb = realPathOf(path);
            if (realTarget.equals(realDb)
                    || realTarget.equals(realDb.resolveSibling(realDb.getFileName() + "-wal"))
                    || realTarget.equals(StorageLock.lockPathOf(realDb))) {
                throw new IllegalArgumentException("snapshot target overlaps the live database: " + target);
            }
            if (Files.exists(target)) {
                throw new FileAlreadyExistsException(target.toString());
            }
            Path parent = target.toAbsolutePath().getParent();
            if (parent == null) {
                throw new IllegalArgumentException("not a file path: " + target);
            }
            Path temp = Files.createTempFile(parent, "smq-snapshot-", ".tmp");
            try {
                executor.executeOutsideTransaction(() -> {
                    StatementCachingConnection.releaseStatements(executor.connection());
                    try (PreparedStatement s = executor.connection().prepareStatement("VACUUM INTO ?")) {
                        s.setString(1, temp.toString());
                        s.execute();
                        return Boolean.TRUE;
                    }
                });
                Files.createLink(target, temp);
            } catch (IOException | RuntimeException e) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }

            try {
                Files.deleteIfExists(temp);
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "failed to remove temporary snapshot file " + temp, e);
            }
        } catch (IOException e) {
            throw new SmqStorageException("cannot snapshot to " + target, e);
        }
    }

    private static Path realPathOf(Path file) throws IOException {
        Path absolute = file.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        Path name = absolute.getFileName();
        if (parent == null || name == null) {
            throw new IllegalArgumentException("not a file path: " + file);
        }
        return parent.toRealPath().resolve(name);
    }

    @Override
    @PolicyException(
            rule = "PMD.UseTryWithResources",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UseTryWithResources")
    public void close() {
        closed = true;
        try {
            executor.close();
        } finally {
            storageLock.close();
        }
    }

    private static void closeQuietly(@Nullable Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
                LOG.log(System.Logger.Level.WARNING, "failed to close SQLite connection during cleanup", e);
            }
        }
    }

    private static void setInstantOrNull(PreparedStatement ps, int index, @Nullable Instant t) throws SQLException {
        if (t == null) {
            ps.setNull(index, java.sql.Types.INTEGER);
        } else {
            ps.setLong(index, toNanos(t));
        }
    }

    private static long toNanos(Instant t) {
        return Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000_000L), t.getNano());
    }

    private static Instant fromNanos(long nanos) {
        return Instant.ofEpochSecond(Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
    }

    private static byte[] encodeHeaders(Map<String, String> headers) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(headers.size());
            for (Map.Entry<String, String> e : headers.entrySet()) {
                writeString(out, e.getKey());
                writeString(out, e.getValue());
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @PolicyException(
            rule = "PMD.ExceptionAsFlowControl",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.ExceptionAsFlowControl")
    private static Map<String, String> decodeHeaders(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int n = in.readInt();
            if (n < 0 || n > in.available() / 8) {
                throw new IOException("invalid header count");
            }
            Map<String, String> headers = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                headers.put(readString(in), readString(in));
            }
            return headers;
        } catch (IOException e) {
            throw new SmqStorageException("invalid message headers", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new IOException("invalid header string length");
        }
        byte[] b = new byte[length];
        in.readFully(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
}
