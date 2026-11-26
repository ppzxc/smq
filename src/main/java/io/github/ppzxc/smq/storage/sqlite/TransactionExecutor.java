package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.SmqStorageException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

final class TransactionExecutor {

    private static final System.Logger LOG = System.getLogger(TransactionExecutor.class.getName());

    private static final int SQLITE_BUSY = 5;

    @FunctionalInterface
    interface TxBody<T> {
        T apply(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    interface SqlAction<T> {
        T run() throws SQLException;
    }

    static final int CHECKPOINT_CHECK_EVERY_COMMITS = 8;

    static final int CHECKPOINT_FAILURE_LOG_EVERY = 64;

    static final long CHECKPOINT_WAL_BYTES = 8L * 1024 * 1024;

    static final long WAL_FILE_KEEP_BYTES = 12L * 1024 * 1024;

    static void configureWal(Statement s) throws SQLException {
        s.execute("PRAGMA wal_autocheckpoint = 0");
        s.execute("PRAGMA journal_size_limit = " + WAL_FILE_KEEP_BYTES);
        s.execute("PRAGMA journal_mode = WAL");
    }

    private static final Path NO_WAL = Path.of("");

    private final Connection connection;

    private long commits;

    private Path walFile = NO_WAL;

    private boolean walFileLocated;

    private long walSizeAtRestart;

    private long consecutiveCheckpointFailures;

    private final WalSizeReader walSizeReader;

    private final ReentrantLock lock = new ReentrantLock();
    private @Nullable Throwable failure;

    @FunctionalInterface
    interface WalSizeReader {
        long size(Path walFile) throws IOException;
    }

    TransactionExecutor(Connection connection) {
        this(connection, Files::size);
    }

    TransactionExecutor(Connection connection, WalSizeReader walSizeReader) {
        this.connection = connection;
        this.walSizeReader = walSizeReader;
    }

    Connection connection() {
        return connection;
    }

    @FunctionalInterface
    interface ReadProbe {

        boolean apply(Connection connection) throws SQLException;
    }

    boolean anyOrUncertain(ReadProbe probe) {
        return anyOrUncertain(probe, Deadline.NONE);
    }

    boolean anyOrUncertain(ReadProbe probe, Deadline deadline) {
        return read(probe::apply, deadline);
    }

    <T> T read(TxBody<T> probe) {
        return read(probe, Deadline.NONE);
    }

    <T> T read(TxBody<T> probe, Deadline deadline) {
        acquire(deadline);
        try {
            checkFailure();
            return probe.apply(connection);
        } catch (SQLException e) {
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    <T> T execute(TxBody<T> body) {
        return execute(body, Deadline.NONE);
    }

    <T> T execute(TxBody<T> body, Deadline deadline) {
        return execute(body, deadline, () -> {});
    }

    <T> T execute(TxBody<T> body, Deadline deadline, Runnable afterCommit) {
        return runLocked(() -> runOnce(body, afterCommit), deadline);
    }

    void close() {
        lock.lock();
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.log(System.Logger.Level.WARNING, "failed to close SQLite connection", e);
        } finally {
            lock.unlock();
        }
    }

    private <T> T runOnce(TxBody<T> body, Runnable afterCommit) throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("BEGIN IMMEDIATE");
        }
        T result;
        try {
            result = body.apply(connection);
            try (Statement s = connection.createStatement()) {
                s.execute("COMMIT");
            }
        } catch (Throwable t) {
            try {
                rollback();
            } catch (Throwable rollbackFailure) {
                failure = t;
                t.addSuppressed(rollbackFailure);
            }
            throw t;
        }

        try {
            afterCommit.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "after-commit hook failed; in-memory state may lag the database", e);
        }
        checkpointIfWalIsLarge();
        return result;
    }

    private void checkpointIfWalIsLarge() {
        commits++;
        if (commits % CHECKPOINT_CHECK_EVERY_COMMITS != 0) {
            return;
        }
        try {
            checkWalSizeAndRewind();
        } catch (Throwable t) {

            walSizeAtRestart = 0;
            logCheckpointFailure(t, () -> LOG.log(System.Logger.Level.WARNING, CHECKPOINT_UNEXPECTED, t));
        }
    }

    @SuppressWarnings("ReferenceEquality") // Suppress self-suppression by comparing Throwable identity.
    static void logCheckpointFailure(Throwable failure, Runnable log) {
        try {
            log.run();
        } catch (Throwable loggingFailure) {
            if (loggingFailure != failure) {
                failure.addSuppressed(loggingFailure);
            }
        }
    }

    private void checkWalSizeAndRewind() {
        if (!walFileLocated) {
            try {
                walFile = locateWalFile();
                walFileLocated = true;
            } catch (SQLException | RuntimeException e) {

                checkpointFailed(WAL_LOCATE_FAILED, e);
                return;
            }
        }
        long size;
        if (walFile.equals(NO_WAL)) {
            size = 0;
        } else {
            try {
                size = walSizeReader.size(walFile);
            } catch (NoSuchFileException e) {

                size = 0;
            } catch (IOException | RuntimeException e) {

                checkpointFailed(WAL_SIZE_UNREADABLE, e);
                return;
            }
        }
        if (size < CHECKPOINT_WAL_BYTES) {
            consecutiveCheckpointFailures = 0;
            return;
        }
        if (size <= walSizeAtRestart) {

            return;
        }
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(RESTART)")) {

            if (rs.next() && rs.getInt(1) == 0) {
                consecutiveCheckpointFailures = 0;
                rememberSizeAfterRestart();
            } else {
                rewindFailed(null);
            }
        } catch (SQLException | RuntimeException e) {
            rewindFailed(e);
        }
    }

    private void rewindFailed(@Nullable Exception cause) {
        walSizeAtRestart = 0;
        checkpointFailed(CHECKPOINT_INCOMPLETE, cause);
    }

    private void rememberSizeAfterRestart() {
        try {
            walSizeAtRestart = Math.min(walSizeReader.size(walFile), WAL_FILE_KEEP_BYTES);
        } catch (IOException | RuntimeException e) {
            walSizeAtRestart = 0;
            LOG.log(
                    System.Logger.Level.WARNING,
                    "sqlite checkpoint rewound the WAL but its size could not be read; the next check may rewind again",
                    e);
        }
    }

    private static final String CHECKPOINT_INCOMPLETE =
            "sqlite checkpoint did not complete after commit; the next check will retry";
    private static final String WAL_LOCATE_FAILED =
            "sqlite WAL file could not be located, so the checkpoint check was skipped; the next check will retry";
    private static final String WAL_SIZE_UNREADABLE =
            "sqlite WAL size could not be read, so the checkpoint check was skipped; the next check will retry";
    private static final String CHECKPOINT_UNEXPECTED =
            "sqlite checkpoint check failed unexpectedly after commit (not rate-limited); the next check will retry";

    private void checkpointFailed(String what, @Nullable Throwable cause) {
        consecutiveCheckpointFailures++;
        long failures = consecutiveCheckpointFailures;
        if (failures != 1 && failures % CHECKPOINT_FAILURE_LOG_EVERY != 0) {
            return;
        }
        String message = what + " (consecutive failures: " + failures + ", the WAL may be growing)";
        if (cause == null) {
            LOG.log(System.Logger.Level.WARNING, message);
        } else {
            LOG.log(System.Logger.Level.WARNING, message, cause);
        }
    }

    private Path locateWalFile() throws SQLException {
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA database_list")) {
            while (rs.next()) {
                if ("main".equals(rs.getString(2))) {
                    String file = rs.getString(3);
                    return file == null || file.isEmpty() ? NO_WAL : Path.of(file + "-wal");
                }
            }
        }
        return NO_WAL;
    }

    private void rollback() throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("ROLLBACK");
        }
    }

    private void checkFailure() {
        if (failure != null) {
            throw new SmqStorageException("sqlite executor is unusable after rollback failure", failure);
        }
    }

    <T> T executeOutsideTransaction(SqlAction<T> action) {
        return runLocked(action, Deadline.NONE);
    }

    private <T> T runLocked(SqlAction<T> action, Deadline deadline) {
        acquire(deadline);
        try {
            checkFailure();
            return action.run();
        } catch (SQLException e) {
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    private void acquire(Deadline deadline) {
        if (!deadline.bounded()) {
            lock.lock();
            return;
        }
        try {
            if (!lock.tryLock(deadline.lockWaitNanos(), TimeUnit.NANOSECONDS)) {
                throw new Deadline.Expired();
            }
        } catch (InterruptedException e) {
            throw new Deadline.Interrupted(e);
        }
    }

    static boolean isBusy(SQLException e) {

        return (e.getErrorCode() & 0xFF) == SQLITE_BUSY;
    }
}
