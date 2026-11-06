package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.NoDeadLetterQueueException;
import io.github.ppzxc.smq.Outcome;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
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

    /** 'SMQ1'. smq 파일임을 알리는 {@code application_id}. */
    public static final int APPLICATION_ID = SqliteSchema.APPLICATION_ID;

    public static final int SCHEMA_VERSION = 1;

    /** 꺼낼 수 있는 메시지의 시각 조건. 지연이 끝났고 리스가 없거나 만료됐다. 자리표시자 둘은 모두 현재 시각이다. */
    private static final String READY_TIME_CONDITION =
            "(available_at IS NULL OR available_at <= ?) AND (lease_token IS NULL OR lease_deadline <= ?)";

    /** 준비 조건: 큐, 지연 끝남, 리스 없음 또는 만료. 바인딩은 (queue_id, now, now) 순서다. */
    private static final String READY_FROM_WHERE = " FROM message WHERE queue_id = ? AND " + READY_TIME_CONDITION;

    /** 저장된 상태 두 칸: 리스 토큰이 있는지, 꺼낼 시각 값이 있는지. 개수를 옮길 원천 상태를 알아내는 데 쓴다(ADR-0018). */
    private static final String STORED_STATE = "lease_token IS NOT NULL, available_at IS NOT NULL";

    /** 최대 전달 횟수에 도달하지 않은 준비 메시지. 꺼내기가 실제로 고르는 대상이다. 바인딩: 준비 조건, 횟수 상한, LIMIT. */
    static final String SELECT_PULLABLE =
            "SELECT id" + READY_FROM_WHERE + " AND delivery_count < ? ORDER BY id LIMIT ?";

    /** {@link #SELECT_PULLABLE}과 같은 대상을 저장된 상태와 함께 읽는다. 바인딩도 같다. */
    private static final String SELECT_PULLABLE_STATE =
            "SELECT id, " + STORED_STATE + READY_FROM_WHERE + " AND delivery_count < ? ORDER BY id LIMIT ?";

    /** 최대 전달 횟수에 도달한 준비 메시지. dead letter 큐로 옮길 대상이다. 바인딩은 SELECT_PULLABLE과 같다. */
    static final String SELECT_EXHAUSTED = "SELECT id, payload, headers, delivery_count, " + STORED_STATE
            + READY_FROM_WHERE
            + " AND delivery_count >= ? ORDER BY id LIMIT ?";

    /** 운영 스키마: 목표 버전 1, 업그레이드 단계 없음. */
    private static final SqliteSchema SCHEMA = new SqliteSchema(SCHEMA_VERSION, Map.of());

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
        if (lockWaitFloor.isNegative()) {
            throw new IllegalArgumentException("lockWaitFloor must not be negative: " + lockWaitFloor);
        }
        StorageLock storageLock = StorageLock.acquire(path);
        @Nullable Connection connection = null;
        try {
            connection = StatementCachingConnection.wrap(connect(path, durability));
            TransactionExecutor executor = new TransactionExecutor(connection);
            executor.execute(SCHEMA::establish);
            // 표식을 확인한 뒤에야 파일의 저널 모드를 바꾼다.
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
            return new SqliteStorage(path, executor, lockWaitFloor, storageLock, counters);
        } catch (SQLException | RuntimeException e) {
            closeQuietly(connection);
            storageLock.close();
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
        }
    }

    /**
     * 연결을 열고 연결 설정을 건다. 설정 순서를 이 함수 하나에 모은다.
     *
     * <p>설정에는 {@code busy_timeout}만 넣고, {@code locking_mode}와 {@code synchronous}는 연결 직후 이 순서로 직접
     * 실행한다. 기다리지 않는다: 락 파일과 EXCLUSIVE 아래에서는 같은 파일을 쥔 연결이 이 연결뿐이라 BUSY가 나지 않고, 난다면
     * smq 밖의 도구가 쥔 것이라 기다려도 풀리지 않는다(ADR-0012). 열린 뒤에도 0을 그대로 쓴다.
     */
    static Connection connect(Path path, Durability durability) throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(0);
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path, config.toProperties());
        boolean ok = false;
        try (Statement s = connection.createStatement()) {
            // sqlite-jdbc는 설정 pragma의 실행 순서를 보장하지 않으므로, EXCLUSIVE를 첫 문장으로 직접 건다.
            // 두 문장의 순서가 곧 설정이다. 아래 결과 확인은 pragma가 무시된 경우만 잡고, 락을 실제로 잡았는지는 -shm 부재 테스트가 확인한다. 순서를 바꾸거나 SQLiteConfig로
            // 되돌리면 같은 고장이 다시 난다.
            // 이미 WAL인 파일을 다시 열 때도 WAL로 들어가기 전에 켜야 -shm 대신 heap memory wal-index를 쓴다.
            try (ResultSet rs = s.executeQuery("PRAGMA locking_mode = EXCLUSIVE")) {
                requireExclusive(rs.next() ? rs.getString(1) : null);
            }
            // POWER_LOSS는 synchronous=FULL, PROCESS_CRASH는 WAL과 함께 synchronous=NORMAL이다.
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

    /**
     * {@code PRAGMA locking_mode = EXCLUSIVE}가 돌려준 모드가 exclusive가 아니면 연결을 쓰지 않는다.
     *
     * <p>이 검사가 잡는 것은 pragma가 무시되거나 인식되지 않아 모드가 바뀌지 않은 경우뿐이다. pragma는 파일 락을 실제로
     * 잡기 전에도 'exclusive'를 돌려주므로, 락이 실제로 잡혔는지, WAL에서 {@code -shm} 없이 동작하는지는 이 검사로 알 수 없다.
     * 그 보증은 {@code -shm} 파일이 생기지 않음을 확인하는 테스트가 진다.
     */
    static void requireExclusive(@Nullable String mode) throws SQLException {
        if (!"exclusive".equalsIgnoreCase(mode)) {
            throw new SQLException("locking_mode=EXCLUSIVE was not applied: " + mode);
        }
    }

    private static boolean isBusyCause(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof SQLException se && TransactionExecutor.isBusy(se)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotADatabase(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof SQLiteException se && se.getResultCode() == SQLiteErrorCode.SQLITE_NOTADB) {
                return true;
            }
        }
        return false;
    }

    /**
     * 모든 큐 연산이 거치는 쓰기 트랜잭션 경로. 실행기의 쓰기 실행에 넘겨주기만 한다.
     * 스키마 버전은 열 때 한 번 판정하므로 트랜잭션마다 다시 확인하지 않고, {@code SQLITE_BUSY}도 재시도하지 않는다.
     */
    <T> T inTransaction(TransactionExecutor.TxBody<T> body) {
        return inTransaction(body, Deadline.NONE);
    }

    private <T> T inTransaction(TransactionExecutor.TxBody<T> body, Deadline deadline) {
        return executor.execute(body, deadline);
    }

    /** 개수를 바꾸는 쓰기 트랜잭션. 변화량을 모아 두었다가 커밋 직후 쓰기 락 안에서 메모리 개수에 반영한다. */
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

    /**
     * 저장된 상태로 개수를 채운다. 시각은 보지 않는다. 리스 토큰이 있으면 리스 중, 없고 꺼낼 시각 값이 있으면 지연됨,
     * 둘 다 없으면 준비다(ADR-0018).
     */
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
        return inTransaction(c -> findQueue(c, queue) != null);
    }

    @Override
    public Optional<QueueOptions> options(String queue) {
        return inTransaction(
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

    private static boolean hasActiveLease(Connection c, long queueId, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM message WHERE queue_id = ? AND lease_token IS NOT NULL AND lease_deadline > ? LIMIT 1")) {
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
    public List<MessageId> publish(String queue, List<NewMessage> batch, Instant now) {
        return inCountingTransaction((c, tx) -> {
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

    /**
     * 꺼낼 메시지가 있는지 autocommit으로 본다. 실행은 {@link TransactionExecutor#anyOrUncertain}이 맡아 쓰기 락 없이,
     * 읽기 트랜잭션을 남기지 않고 한다. 있다고 나와도 실제 꺼내기는 쓰기 트랜잭션이 다시 확인한다.
     * 확실하지 않으면 true를 돌려 쓰기 경로가 다시 확인하게 한다. 그런 경우는 큐가 없을 때(쓰기 경로가 오류를
     * 던진다)다.
     * SQL 오류는 true가 아니라 예외다.
     */
    private boolean anyReady(String queue, Instant now, Deadline waitDeadline) {
        return executor.anyOrUncertain(
                connection -> {
                    try (PreparedStatement ps = connection.prepareStatement(
                            "SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?)"
                                    + " AND " + READY_TIME_CONDITION + "), "
                                    + "EXISTS(SELECT 1 FROM queue WHERE name = ?)")) {
                        ps.setString(1, queue);
                        ps.setLong(2, toNanos(now));
                        ps.setLong(3, toNanos(now));
                        ps.setString(4, queue);
                        try (ResultSet rs = ps.executeQuery()) {
                            // 행이 없는 일은 없지만, 모르면 쓰기 경로가 확인하게 한다.
                            return !rs.next() || rs.getInt(1) == 1 || rs.getInt(2) == 0;
                        }
                    }
                },
                waitDeadline);
    }

    @Override
    public PullResult pull(String queue, int max, Duration lease, Instant now) {
        return pull(queue, max, lease, now, Deadline.NONE);
    }

    /** {@code wait} 안에 lock이나 쓰기 트랜잭션을 잡지 못하면 아무것도 반영하지 않고 빈 결과를 돌려준다(ADR-0016). */
    @Override
    public PullResult pull(String queue, int max, Duration lease, Instant now, Duration wait) {
        try {
            return pull(queue, max, lease, now, Deadline.after(wait, lockWaitFloor));
        } catch (Deadline.Expired e) {
            return PullResult.empty();
        }
    }

    private PullResult pull(String queue, int max, Duration lease, Instant now, Deadline waitDeadline) {
        if (!anyReady(queue, now, waitDeadline)) {
            return PullResult.empty();
        }
        return inCountingTransaction(
                (c, tx) -> {
                    long queueId = requireQueue(c, queue);
                    QueueOptions options = readOptions(c, queue);
                    // 꺼내기 트랜잭션 1단계: 단일 활성 컨슈머 큐에 리스 중인 청크가 있으면 빈 청크다.
                    if (options.singleActiveConsumer() && hasActiveLease(c, queueId, now)) {
                        return PullResult.empty();
                    }
                    int limit = options.maxDeliveries().orElse(Integer.MAX_VALUE);
                    boolean deadLettersRemain = options.maxDeliveries().isPresent()
                            && moveExhausted(c, tx, queue, queueId, options, limit, now);
                    // 꺼낼 대상을 저장된 상태와 함께 먼저 읽는다. 아래 UPDATE가 같은 트랜잭션에서 같은 행을 고른다.
                    long fromReady = 0;
                    long fromDelayed = 0;
                    long picked = 0;
                    try (PreparedStatement peek = c.prepareStatement(SELECT_PULLABLE_STATE)) {
                        peek.setLong(1, queueId);
                        peek.setLong(2, toNanos(now));
                        peek.setLong(3, toNanos(now));
                        peek.setInt(4, limit);
                        peek.setInt(5, max);
                        try (ResultSet rs = peek.executeQuery()) {
                            while (rs.next()) {
                                picked++;
                                if (rs.getInt(2) == 0) {
                                    // 리스 만료 행은 리스 중에서 리스 중으로 가므로 개수가 바뀌지 않는다.
                                    if (rs.getInt(3) == 0) {
                                        fromReady++;
                                    } else {
                                        fromDelayed++;
                                    }
                                }
                            }
                        }
                    }
                    if (picked == 0) {
                        return deadLettersRemain ? PullResult.emptyDeadLettersRemain() : PullResult.empty();
                    }
                    tx.ready(queueId, -fromReady);
                    tx.delayed(queueId, -fromDelayed);
                    tx.leased(queueId, fromReady + fromDelayed);
                    long token;
                    try (Statement s = c.createStatement();
                            ResultSet rs = s.executeQuery(
                                    "UPDATE meta SET value = value + 1 WHERE key = 'lease_seq' RETURNING value")) {
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
                        ps.setLong(4, toNanos(now));
                        ps.setLong(5, toNanos(now));
                        ps.setInt(6, limit);
                        ps.setInt(7, max);
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
                    return PullResult.of(new LeaseToken(token), deadline, messages);
                },
                waitDeadline);
    }

    /**
     * 최대 전달 횟수에 도달한 준비 메시지를 id 순서로 dead letter 큐에 새 메시지로 옮긴다(꺼내기 트랜잭션 안).
     * 한 번에 옮기는 개수에는 상한이 있다.
     *
     * @return 상한에 걸려 남은 도달 메시지가 더 있으면 true
     */
    private static boolean moveExhausted(
            Connection c, QueueCounters.Tx tx, String queue, long queueId, QueueOptions options, int limit, Instant now)
            throws SQLException {
        long dlqId = requireQueue(c, options.deadLetterQueue().orElseThrow());
        List<Exhausted> found = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(SELECT_EXHAUSTED)) {
            ps.setLong(1, queueId);
            ps.setLong(2, toNanos(now));
            ps.setLong(3, toNanos(now));
            ps.setInt(4, limit);
            ps.setInt(5, DeadLetters.MAX_MOVES_PER_PULL + 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    found.add(new Exhausted(
                            rs.getLong(1),
                            rs.getBytes(2),
                            decodeHeaders(rs.getBytes(3)),
                            rs.getInt(4),
                            rs.getInt(5) != 0,
                            rs.getInt(6) != 0));
                }
            }
        }
        // 상한보다 한 건 더 읽어 두었으므로, 더 있는지는 개수로 안다.
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
                // 원천은 저장된 상태로 정한다. 기한이 지난 리스가 남은 행은 리스 중에서 빠진다.
                if (e.leased()) {
                    tx.leased(queueId, -1);
                } else if (e.delayed()) {
                    tx.delayed(queueId, -1);
                } else {
                    tx.ready(queueId, -1);
                }
                tx.ready(dlqId, 1);
            }
        }
        return deadLettersRemain;
    }

    /** dead letter 큐로 옮길 도달 메시지 한 건. */
    // payload 배열은 JDBC에서 읽어 즉시 SQL 바인딩에만 사용한다.
    @SuppressWarnings("ArrayRecordComponent")
    private record Exhausted(
            long id, byte[] payload, Map<String, String> headers, int deliveryCount, boolean leased, boolean delayed) {}

    @Override
    public List<Result> settle(LeaseToken token, List<Disposition> items, Instant now) {
        return inCountingTransaction((c, tx) -> {
            List<Result> results = new ArrayList<>(items.size());
            try (PreparedStatement ack = c.prepareStatement("DELETE FROM message WHERE id = ? AND lease_token = ?");
                    PreparedStatement ackFrom = c.prepareStatement(
                            "DELETE FROM message WHERE id = ? AND lease_token = ? RETURNING queue_id");
                    PreparedStatement find = c.prepareStatement("""
                            SELECT m.queue_id, q.name, q.dead_letter_queue_id, m.payload, m.headers, m.delivery_count
                            FROM message m JOIN queue q ON q.id = m.queue_id
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
                        case REJECT -> updated = reject(find, insert, ack, tx, token, d, now);
                        default -> throw new IllegalStateException("unknown disposition: " + d.kind());
                    }
                    results.add(new Result(d.id(), updated == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return results;
        });
    }

    /** 이동은 dead letter 큐에 새 메시지를 넣고 원래 메시지를 지우는 일이다. 예외는 트랜잭션 전체를 되돌린다. */
    private static int reject(
            PreparedStatement find,
            PreparedStatement insert,
            PreparedStatement delete,
            QueueCounters.Tx tx,
            LeaseToken token,
            Disposition d,
            Instant now)
            throws SQLException {
        find.setLong(1, d.id().value());
        find.setLong(2, token.value());
        long sourceId;
        String queueName;
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
            tx.leased(sourceId, -1);
            tx.ready(dlqId, 1);
        }
        return deleted;
    }

    @Override
    public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, InstantSource clock) {
        return inTransaction(c -> {
            // 시각은 쓰기 트랜잭션을 잡은 뒤에 읽는다. 앞서 커밋된 다른 꺼내기와 시각 순서가 어긋나지 않게 한다.
            Instant now = clock.instant();
            Instant deadline = now.plus(d);
            List<Result> results = new ArrayList<>(ids.size());
            // 단일 활성 컨슈머 큐에서는 만료된 리스를 되살리지 않는다. 그 사이 다른 청크가 꺼냈을 수 있다.
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
            Path temp = Files.createTempFile(target.toAbsolutePath().getParent(), "smq-snapshot-", ".tmp");
            try {
                executor.executeOutsideTransaction(() -> {
                    StatementCachingConnection.releaseStatements(executor.connection());
                    try (Statement s = executor.connection().createStatement()) {
                        s.execute("VACUUM INTO '" + temp.toString().replace("'", "''") + "'");
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
            // 연결이 끝난 뒤라 대상은 이미 완전한 파일이다. 임시 파일을 못 지워도 실패로 보지 않는다.
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // 남은 임시 파일은 스냅샷의 결과에 영향이 없다.
            }
        } catch (IOException e) {
            throw new SmqStorageException("cannot snapshot to " + target, e);
        }
    }

    /** 부모 디렉터리의 실제 경로에 파일명을 붙인다. 파일 자체는 없어도 된다. */
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
            } catch (SQLException ignored) {
                // 닫기 실패는 무시한다.
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

    private static Map<String, String> decodeHeaders(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int n = in.readInt();
            Map<String, String> headers = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                headers.put(readString(in), readString(in));
            }
            return headers;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
}
