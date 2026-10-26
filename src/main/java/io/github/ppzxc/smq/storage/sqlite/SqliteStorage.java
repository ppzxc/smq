package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueDeclarationConflictException;
import io.github.ppzxc.smq.QueueInUseException;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.SmqStorageException;
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
import java.util.concurrent.locks.ReentrantLock;
import org.sqlite.SQLiteConfig;

/**
 * SQLite 파일 하나를 쓰는 저장소. 모든 연산은 {@link #inTransaction}이라는 하나의 쓰기 트랜잭션 경로를 거친다.
 * 이후 트랜잭션마다 하는 일(스키마 버전 확인, {@code SQLITE_BUSY} 재시도)은 그 한 곳에 넣는다.
 */
public final class SqliteStorage implements Storage {

    /** 'SMQ1'. smq 파일임을 알리는 {@code application_id}. */
    public static final int APPLICATION_ID = 0x534D5131;
    public static final int SCHEMA_VERSION = 1;

    static final String SELECT_READY =
            "SELECT id FROM message WHERE queue_id = ? AND (lease_token IS NULL OR lease_deadline <= ?)"
                    + " ORDER BY id LIMIT ?";

    private static final int BUSY_TIMEOUT_MILLIS = 5000;

    @FunctionalInterface
    interface TxBody<T> {
        T apply(Connection connection) throws SQLException;
    }

    private final Connection connection;
    private final ReentrantLock lock = new ReentrantLock();

    private SqliteStorage(Connection connection) {
        this.connection = connection;
    }

    public static SqliteStorage open(Path path) {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(BUSY_TIMEOUT_MILLIS);
        config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + path, config.toProperties());
            SqliteStorage storage = new SqliteStorage(connection);
            storage.inTransaction(SqliteStorage::ensureSchema);
            // 표식을 확인한 뒤에야 파일의 저널 모드를 바꾼다.
            try (Statement s = connection.createStatement()) {
                s.execute("PRAGMA journal_mode = WAL");
            }
            return storage;
        } catch (SQLException | RuntimeException e) {
            closeQuietly(connection);
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new SmqStorageException("cannot open " + path, e);
        }
    }

    private static Void ensureSchema(Connection c) throws SQLException {
        int applicationId = pragmaInt(c, "application_id");
        int version = pragmaInt(c, "user_version");
        boolean empty;
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM sqlite_schema")) {
            empty = rs.getInt(1) == 0;
        }
        if (empty && applicationId == 0) {
            try (Statement s = c.createStatement()) {
                s.execute("""
                        CREATE TABLE queue (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          name TEXT NOT NULL UNIQUE,
                          max_deliveries INTEGER,
                          dead_letter_queue_id INTEGER REFERENCES queue(id),
                          single_active_consumer INTEGER NOT NULL DEFAULT 0
                        )""");
                s.execute("""
                        CREATE TABLE message (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          queue_id INTEGER NOT NULL REFERENCES queue(id),
                          payload BLOB NOT NULL,
                          headers BLOB NOT NULL,
                          delivery_count INTEGER NOT NULL DEFAULT 0,
                          published_at INTEGER NOT NULL,
                          lease_token INTEGER,
                          lease_deadline INTEGER
                        )""");
                s.execute("CREATE INDEX message_ready ON message(queue_id, id)");
                s.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)");
                s.execute("INSERT INTO meta(key, value) VALUES ('lease_seq', 0)");
                s.execute("PRAGMA application_id = " + APPLICATION_ID);
                s.execute("PRAGMA user_version = " + SCHEMA_VERSION);
            }
            return null;
        }
        if (applicationId != APPLICATION_ID) {
            throw new IncompatibleStorageException("not an smq database (application_id=" + applicationId + ")");
        }
        if (version != SCHEMA_VERSION) {
            throw new IncompatibleStorageException(
                    "unsupported schema version: found " + version + ", supported " + SCHEMA_VERSION);
        }
        return null;
    }

    private static int pragmaInt(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            return rs.getInt(1);
        }
    }

    /** 모든 연산이 거치는 유일한 쓰기 트랜잭션 경로. */
    <T> T inTransaction(TxBody<T> body) {
        lock.lock();
        try {
            try (Statement s = connection.createStatement()) {
                s.execute("BEGIN IMMEDIATE");
            }
            try {
                T result = body.apply(connection);
                try (Statement s = connection.createStatement()) {
                    s.execute("COMMIT");
                }
                return result;
            } catch (Throwable t) {
                rollbackQuietly();
                throw t;
            }
        } catch (SQLException e) {
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    private void rollbackQuietly() {
        try (Statement s = connection.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // 트랜잭션이 이미 끝났을 수 있다.
        }
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
                    SELECT COALESCE(SUM(lease_token IS NULL OR lease_deadline <= ?1), 0),
                           COALESCE(SUM(lease_token IS NOT NULL AND lease_deadline > ?1), 0)
                    FROM message WHERE queue_id = ?2""")) {
                ps.setLong(1, toNanos(now));
                ps.setLong(2, queueId);
                try (ResultSet rs = ps.executeQuery()) {
                    return new QueueCounts(rs.getLong(1), rs.getLong(2), 0);
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
                    "INSERT INTO message(queue_id, payload, headers, published_at) VALUES (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                for (NewMessage m : batch) {
                    ps.setLong(1, queueId);
                    ps.setBytes(2, m.payload());
                    ps.setBytes(3, encodeHeaders(m.headers()));
                    ps.setLong(4, toNanos(now));
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

    @Override
    public PullResult pull(String queue, int max, Duration lease, Instant now) {
        return inTransaction(c -> {
            long queueId = requireQueue(c, queue);
            try (PreparedStatement peek = c.prepareStatement(SELECT_READY)) {
                peek.setLong(1, queueId);
                peek.setLong(2, toNanos(now));
                peek.setInt(3, 1);
                try (ResultSet rs = peek.executeQuery()) {
                    if (!rs.next()) {
                        return PullResult.empty();
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
                    WHERE id IN (""" + SELECT_READY + """
                    )
                    RETURNING id, payload, headers, delivery_count, published_at""")) {
                ps.setLong(1, token);
                ps.setLong(2, toNanos(deadline));
                ps.setLong(3, queueId);
                ps.setLong(4, toNanos(now));
                ps.setInt(5, max);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        messages.add(new Message(new MessageId(rs.getLong(1)), rs.getBytes(2),
                                decodeHeaders(rs.getBytes(3)), rs.getInt(4), fromNanos(rs.getLong(5))));
                    }
                }
            }
            messages.sort((a, b) -> a.id().compareTo(b.id()));
            return new PullResult(new LeaseToken(token), deadline, messages);
        });
    }

    @Override
    public List<Result> settle(LeaseToken token, List<Disposition> items, Instant now) {
        return inTransaction(c -> {
            List<Result> results = new ArrayList<>(items.size());
            try (PreparedStatement ack = c.prepareStatement("DELETE FROM message WHERE id = ? AND lease_token = ?");
                    PreparedStatement nack = c.prepareStatement("""
                            UPDATE message SET lease_token = NULL, lease_deadline = NULL
                            WHERE id = ? AND lease_token = ?""")) {
                for (Disposition d : items) {
                    PreparedStatement ps = d.kind() == Disposition.Kind.ACK ? ack : nack;
                    ps.setLong(1, d.id().value());
                    ps.setLong(2, token.value());
                    results.add(new Result(d.id(), ps.executeUpdate() == 1 ? Outcome.OK : Outcome.STALE));
                }
            }
            return results;
        });
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
        lock.lock();
        try {
            closeQuietly(connection);
        } finally {
            lock.unlock();
        }
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
