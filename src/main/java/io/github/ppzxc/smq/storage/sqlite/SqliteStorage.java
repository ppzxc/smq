package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * SQLite 파일 하나를 쓰는 저장소. 이 클래스에는 열기 순서, 큐 연산 SQL, row codec이 있다.
 * 연결, lock, 스키마 버전 확인, {@code SQLITE_BUSY} 재시도는 {@link TransactionExecutor}가 소유한다.
 * 큐 연산은 모두 {@link #inTransaction}으로 그 실행기의 확인하는 쓰기 트랜잭션을 거친다.
 * 꺼내기 대기용 {@link #anyReady}만 쓰기 락과 재시도 없는 autocommit 읽기로 따로 간다.
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
    private static final String READY_FROM_WHERE =
            " FROM message WHERE queue_id = ? AND " + READY_TIME_CONDITION;

    /** 최대 전달 횟수에 도달하지 않은 준비 메시지. 꺼내기가 실제로 고르는 대상이다. 바인딩: 준비 조건, 횟수 상한, LIMIT. */
    static final String SELECT_PULLABLE =
            "SELECT id" + READY_FROM_WHERE + " AND delivery_count < ? ORDER BY id LIMIT ?";

    /** 최대 전달 횟수에 도달한 준비 메시지. dead letter 큐로 옮길 대상이다. 바인딩은 SELECT_PULLABLE과 같다. */
    static final String SELECT_EXHAUSTED =
            "SELECT id, payload, headers, delivery_count" + READY_FROM_WHERE
                    + " AND delivery_count >= ? ORDER BY id LIMIT ?";

    private static final int BUSY_TIMEOUT_MILLIS = 5000;
    private static final int MAX_ATTEMPTS = 5;

    /** 운영 스키마: 목표 버전 1, 업그레이드 단계 없음. */
    private static final SqliteSchema SCHEMA = new SqliteSchema(SCHEMA_VERSION, Map.of());

    private final TransactionExecutor executor;

    private SqliteStorage(TransactionExecutor executor) {
        this.executor = executor;
    }

    public static SqliteStorage open(Path path, Durability durability) {
        return open(path, durability, BUSY_TIMEOUT_MILLIS, MAX_ATTEMPTS);
    }

    /** 테스트가 경합을 짧게 재현할 수 있게 busy timeout과 시도 횟수를 정한다. */
    static SqliteStorage open(Path path, Durability durability, int busyTimeoutMillis, int maxAttempts) {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(busyTimeoutMillis);
        // POWER_LOSS는 synchronous=FULL, PROCESS_CRASH는 WAL과 함께 synchronous=NORMAL이다.
        config.setSynchronous(durability == Durability.POWER_LOSS
                ? SQLiteConfig.SynchronousMode.FULL
                : SQLiteConfig.SynchronousMode.NORMAL);
        Connection connection = null;
        try {
            connection = StatementCachingConnection.wrap(
                    DriverManager.getConnection("jdbc:sqlite:" + path, config.toProperties()));
            TransactionExecutor executor = new TransactionExecutor(connection,
                    new TransactionExecutor.RetryPolicy(maxAttempts, attempt -> Thread.sleep(10L * attempt)));
            // 열기는 아직 기준 버전이 없으므로 확인 없이 판정하고, 그 결과가 기준 버전이 된다.
            executor.executeUnverified(SCHEMA::establish);
            // 표식을 확인한 뒤에야 파일의 저널 모드를 바꾼다.
            executor.retryOnBusy(() -> {
                try (Statement s = executor.connection().createStatement()) {
                    return s.execute("PRAGMA journal_mode = WAL");
                }
            });
            return new SqliteStorage(executor);
        } catch (SQLException | RuntimeException e) {
            closeQuietly(connection);
            if (isNotADatabase(e)) {
                throw SCHEMA.notSmq("not a SQLite file");
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new SmqStorageException("cannot open " + path, e);
        }
    }

    private static boolean isNotADatabase(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof SQLiteException se
                    && se.getResultCode() == SQLiteErrorCode.SQLITE_NOTADB) {
                return true;
            }
        }
        return false;
    }

    /**
     * 모든 큐 연산이 거치는 쓰기 트랜잭션 경로. 실행기의 확인하는 쓰기 실행에 넘겨주기만 한다.
     * 시작할 때마다 {@code user_version}을 다시 확인하고, {@code SQLITE_BUSY}면 재시도한다.
     */
    <T> T inTransaction(TransactionExecutor.TxBody<T> body) {
        return executor.execute(body);
    }

    @Override
    public void declare(String queue, QueueOptions options) {
        inTransaction(c -> {
            if (findQueue(c, queue) != null) {
                QueueOptions existing = readOptions(c, queue);
                if (!existing.equals(options)) {
                    throw new QueueDeclarationConflictException(queue, existing, options);
                }
                return null;
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
            return null;
        });
    }

    @Override
    public boolean delete(String queue) {
        return inTransaction(c -> {
            Long queueId = findQueue(c, queue);
            if (queueId == null) {
                return false;
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT name FROM queue WHERE dead_letter_queue_id = ? ORDER BY id LIMIT 1")) {
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
            return true;
        });
    }

    @Override
    public boolean exists(String queue) {
        return inTransaction(c -> findQueue(c, queue) != null);
    }

    @Override
    public Optional<QueueOptions> options(String queue) {
        return inTransaction(c -> findQueue(c, queue) == null ? Optional.<QueueOptions>empty()
                : Optional.of(readOptions(c, queue)));
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

    private static Long findQueue(Connection c, String queue) throws SQLException {
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

    @Override
    public QueueCounts counts(String queue, Instant now) {
        return inTransaction(c -> {
            long queueId = requireQueue(c, queue);
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT COALESCE(SUM(NOT delayed AND (lease_token IS NULL OR lease_deadline <= ?1)), 0),
                           COALESCE(SUM(NOT delayed AND lease_token IS NOT NULL AND lease_deadline > ?1), 0),
                           COALESCE(SUM(delayed), 0)
                    FROM (SELECT lease_token, lease_deadline, COALESCE(available_at > ?1, 0) AS delayed
                          FROM message WHERE queue_id = ?2)""")) {
                ps.setLong(1, toNanos(now));
                ps.setLong(2, queueId);
                try (ResultSet rs = ps.executeQuery()) {
                    return new QueueCounts(rs.getLong(1), rs.getLong(2), rs.getLong(3));
                }
            }
        });
    }

    @Override
    public List<MessageId> publish(String queue, List<NewMessage> batch, Instant now) {
        return inTransaction(c -> {
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
                    setInstantOrNull(ps, 5, m.availableAt(now));
                    ps.executeUpdate();
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
     * 꺼낼 메시지가 있는지 autocommit으로 본다. 쓰기 락을 잡지 않고, ResultSet과 Statement를 바로 닫아
     * 읽기 트랜잭션이 열린 채 남지 않는다. 폴링이 쓰기 경합을 만들거나 checkpoint를 굶기지 않게 하기 위해서다.
     * 있다고 나와도 실제 꺼내기는 쓰기 트랜잭션이 다시 확인한다. 큐가 없거나 스키마 버전이 바뀌었으면 true를 돌려 그쪽에서 오류가 나게 한다.
     */
    private boolean anyReady(String queue, Instant now) {
        Connection connection = executor.connection();
        executor.lock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?)"
                        + " AND " + READY_TIME_CONDITION + "), "
                        + "EXISTS(SELECT 1 FROM queue WHERE name = ?)")) {
            ps.setString(1, queue);
            ps.setLong(2, toNanos(now));
            ps.setLong(3, toNanos(now));
            ps.setString(4, queue);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return true; // 일어날 수 없지만, 모르면 쓰기 경로가 확인하게 한다.
                }
                if (rs.getInt(1) == 1 || rs.getInt(2) == 0) {
                    return true;
                }
            }
            // 비어 보여도 스키마 버전이 바뀌었으면 쓰기 경로가 확인하고 던지게 한다. 대기 중인 꺼내기가
            // 버전 변화를 놓치지 않기 위해서다.
            return executor.versionChanged(SqliteSchema.pragmaInt(connection, "user_version"));
        } catch (SQLException e) {
            if (TransactionExecutor.isBusy(e)) {
                return true; // 쓰기 트랜잭션 경로가 재시도한다.
            }
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            executor.lock().unlock();
        }
    }

    @Override
    public PullResult pull(String queue, int max, Duration lease, Instant now) {
        if (!anyReady(queue, now)) {
            return PullResult.empty();
        }
        return inTransaction(c -> {
            long queueId = requireQueue(c, queue);
            QueueOptions options = readOptions(c, queue);
            // 꺼내기 트랜잭션 1단계: 단일 활성 컨슈머 큐에 리스 중인 청크가 있으면 빈 청크다.
            if (options.singleActiveConsumer() && hasActiveLease(c, queueId, now)) {
                return PullResult.empty();
            }
            int limit = options.maxDeliveries().orElse(Integer.MAX_VALUE);
            boolean deadLettersRemain = options.maxDeliveries().isPresent() && moveExhausted(c, queue, queueId, options, limit, now);
            try (PreparedStatement peek = c.prepareStatement(SELECT_PULLABLE)) {
                peek.setLong(1, queueId);
                peek.setLong(2, toNanos(now));
                peek.setLong(3, toNanos(now));
                peek.setInt(4, limit);
                peek.setInt(5, 1);
                try (ResultSet rs = peek.executeQuery()) {
                    if (!rs.next()) {
                        return deadLettersRemain ? PullResult.emptyDeadLettersRemain() : PullResult.empty();
                    }
                }
            }
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
                        messages.add(new Message(new MessageId(rs.getLong(1)), rs.getBytes(2),
                                decodeHeaders(rs.getBytes(3)), rs.getInt(4), fromNanos(rs.getLong(5))));
                    }
                }
            }
            messages.sort((a, b) -> a.id().compareTo(b.id()));
            return PullResult.of(new LeaseToken(token), deadline, messages);
        });
    }

    /**
     * 최대 전달 횟수에 도달한 준비 메시지를 id 순서로 dead letter 큐에 새 메시지로 옮긴다(꺼내기 트랜잭션 안).
     * 한 번에 옮기는 개수에는 상한이 있다.
     *
     * @return 상한에 걸려 남은 도달 메시지가 더 있으면 true
     */
    private static boolean moveExhausted(Connection c, String queue, long queueId, QueueOptions options, int limit,
            Instant now) throws SQLException {
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
                    found.add(new Exhausted(rs.getLong(1), rs.getBytes(2), decodeHeaders(rs.getBytes(3)),
                            rs.getInt(4)));
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
                insert.setBytes(3, encodeHeaders(DeadLetters.headers(
                        new DeadLetters.Origin(queue, new MessageId(e.id()), e.headers(), e.deliveryCount()),
                        DeadLetters.Reason.MAX_DELIVERIES, now)));
                insert.setLong(4, toNanos(now));
                insert.executeUpdate();
                delete.setLong(1, e.id());
                delete.executeUpdate();
            }
        }
        return deadLettersRemain;
    }

    /** dead letter 큐로 옮길 도달 메시지 한 건. */
    private record Exhausted(long id, byte[] payload, Map<String, String> headers, int deliveryCount) {
    }

    @Override
    public List<Result> settle(LeaseToken token, List<Disposition> items, Instant now) {
        return inTransaction(c -> {
            List<Result> results = new ArrayList<>(items.size());
            try (PreparedStatement ack = c.prepareStatement("DELETE FROM message WHERE id = ? AND lease_token = ?");
                    PreparedStatement find = c.prepareStatement("""
                            SELECT m.queue_id, q.name, q.dead_letter_queue_id, m.payload, m.headers, m.delivery_count
                            FROM message m JOIN queue q ON q.id = m.queue_id
                            WHERE m.id = ? AND m.lease_token = ?""");
                    PreparedStatement insert = c.prepareStatement("""
                            INSERT INTO message(queue_id, payload, headers, delivery_count, published_at)
                            VALUES (?, ?, ?, 0, ?)""");
                    PreparedStatement nack = c.prepareStatement("""
                            UPDATE message SET lease_token = NULL, lease_deadline = NULL, available_at = ?
                            WHERE id = ? AND lease_token = ?""")) {
                for (Disposition d : items) {
                    int updated;
                    switch (d.kind()) {
                        case ACK -> {
                            ack.setLong(1, d.id().value());
                            ack.setLong(2, token.value());
                            updated = ack.executeUpdate();
                        }
                        case NACK -> {
                            setInstantOrNull(nack, 1, d.availableAt(now));
                            nack.setLong(2, d.id().value());
                            nack.setLong(3, token.value());
                            updated = nack.executeUpdate();
                        }
                        case REJECT -> updated = reject(find, insert, ack, token, d, now);
                        default -> throw new IllegalStateException("unknown disposition: " + d.kind());
                    }
                    results.add(new Result(d.id(), updated == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return results;
        });
    }

    /** 이동은 dead letter 큐에 새 메시지를 넣고 원래 메시지를 지우는 일이다. 예외는 트랜잭션 전체를 되돌린다. */
    private static int reject(PreparedStatement find, PreparedStatement insert, PreparedStatement delete,
            LeaseToken token, Disposition d, Instant now) throws SQLException {
        find.setLong(1, d.id().value());
        find.setLong(2, token.value());
        String queueName;
        long dlqId;
        byte[] payload;
        Map<String, String> headers;
        int deliveryCount;
        try (ResultSet rs = find.executeQuery()) {
            if (!rs.next()) {
                return 0;
            }
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
        insert.setBytes(3, encodeHeaders(DeadLetters.headers(
                new DeadLetters.Origin(queueName, d.id(), headers, deliveryCount), DeadLetters.Reason.REJECTED, now)));
        insert.setLong(4, toNanos(now));
        insert.executeUpdate();
        delete.setLong(1, d.id().value());
        delete.setLong(2, token.value());
        return delete.executeUpdate();
    }

    @Override
    public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, Instant now) {
        return inTransaction(c -> {
            Instant deadline = now.plus(d);
            List<Result> results = new ArrayList<>(ids.size());
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE message SET lease_deadline = ? WHERE id = ? AND lease_token = ?")) {
                for (MessageId id : ids) {
                    ps.setLong(1, toNanos(deadline));
                    ps.setLong(2, id.value());
                    ps.setLong(3, token.value());
                    results.add(new Result(id, ps.executeUpdate() == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return new Extended(deadline, results);
        });
    }

    @Override
    public void close() {
        executor.close();
    }

    private static void closeQuietly(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // 닫기 실패는 무시한다.
            }
        }
    }

    private static void setInstantOrNull(PreparedStatement ps, int index, Instant t) throws SQLException {
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
