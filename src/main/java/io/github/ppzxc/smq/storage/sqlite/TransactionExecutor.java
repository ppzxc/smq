package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.SmqStorageException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQLite 저장소의 쓰기 경로. 단일 연결, 단일 lock, 기준 스키마 버전, 영구 실패 상태, {@code SQLITE_BUSY} 판정과
 * 재시도 루프를 한 곳에서 소유한다(ADR-0004, ADR-0007). 꺼내기 대기용 autocommit 읽기 {@link #anyOrUncertain}도
 * 이 모듈을 거친다.
 *
 * <p>쓰기 실행은 두 가지다. {@link #executeUnverified}는 열 때만 쓰고 결과가 기준 버전이 된다. {@link #execute}는
 * 모든 큐 연산이 쓰고 {@code BEGIN IMMEDIATE} 안에서 트랜잭션마다 버전을 확인한다. 재시도 대기 중에도 lock을 쥔다.
 */
final class TransactionExecutor {

    /** SQLite 기본 결과 코드 {@code SQLITE_BUSY}. */
    private static final int SQLITE_BUSY = 5;

    /** 재시도 사이 대기. 시도 번호(1부터)를 받는다. */
    @FunctionalInterface
    interface Backoff {
        void await(int attempt) throws InterruptedException;
    }

    record RetryPolicy(int maxAttempts, Backoff backoff) {}

    @FunctionalInterface
    interface TxBody<T> {
        T apply(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    interface SqlAction<T> {
        T run() throws SQLException;
    }

    private final Connection connection;
    private final RetryPolicy retry;
    private final ReentrantLock lock = new ReentrantLock();
    /** 열 때 판정한 스키마 버전. {@link #execute}가 이 값과 파일의 버전을 비교한다. */
    private volatile int baselineVersion;
    /** 버전이 달라진 것을 한 번 보면 영구히 실패한 상태다. */
    private volatile boolean failed;

    TransactionExecutor(Connection connection, RetryPolicy retry) {
        this.connection = connection;
        this.retry = retry;
    }

    /** 연결. 같은 lock 아래에서만 쓴다. */
    Connection connection() {
        return connection;
    }

    /** autocommit 읽기. 연결에 한 번 묻고 그 결과를 돌려준다. */
    @FunctionalInterface
    interface ReadProbe {
        /** 꺼낼 것이 있거나 큐가 없으면 참. 읽기 statement는 이 안에서 닫는다. */
        boolean apply(Connection connection) throws SQLException;
    }

    /**
     * 꺼낼 것이 있는지 autocommit으로 본다. 쓰기 락과 재시도 없이 읽고 읽기 트랜잭션을 남기지 않는다.
     * 확실하지 않으면(비어 보이는데 버전이 바뀌었거나 영구 실패, BUSY) 참을 돌려 쓰기 경로가 다시 확인하고
     * 던지게 한다. BUSY가 아닌 오류는 예외다.
     */
    boolean anyOrUncertain(ReadProbe probe) {
        return anyOrUncertain(probe, Deadline.NONE);
    }

    /** 마감 안에 lock을 잡지 못하면 {@link Deadline.Expired}를 던진다. 아무것도 읽지 않았다. */
    boolean anyOrUncertain(ReadProbe probe, Deadline deadline) {
        acquire(deadline);
        try {
            if (probe.apply(connection)) {
                return true;
            }
            return failed || SqliteSchema.pragmaInt(connection, "user_version") != baselineVersion;
        } catch (SQLException e) {
            if (isBusy(e)) {
                return true;
            }
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    /** 확인 없는 쓰기 트랜잭션. 열 때만 쓴다. 돌려준 버전이 기준 버전이 된다. */
    int executeUnverified(TxBody<Integer> body) {
        int version = retryOnBusy(() -> runOnce(body, false));
        baselineVersion = version;
        return version;
    }

    /** 확인하는 쓰기 트랜잭션. BUSY면 되돌린 뒤 전체를 다시 시도한다. 그 밖의 SQL 오류는 효력 불명이다. */
    <T> T execute(TxBody<T> body) {
        return execute(body, Deadline.NONE);
    }

    /**
     * 마감이 있으면 lock 대기와 BUSY 재시도에 적용한다. 마감 안에 lock을 잡지 못하거나, BUSY로 실패한 뒤 마감이
     * 지났으면 {@link Deadline.Expired}를 던진다. 어느 쪽이든 아무것도 반영되지 않았다(BUSY 실패는 롤백된다).
     * driver 안의 {@code busy_timeout} 대기는 끊을 수 없으므로 최악은 마감에 한 번의 {@code busy_timeout}을 더한 값이다.
     */
    <T> T execute(TxBody<T> body, Deadline deadline) {
        return retryOnBusy(() -> runOnce(body, true), deadline);
    }

    void close() {
        lock.lock();
        try {
            connection.close();
        } catch (SQLException ignored) {
            // 닫기 실패는 무시한다.
        } finally {
            lock.unlock();
        }
    }

    private <T> T runOnce(TxBody<T> body, boolean verify) throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("BEGIN IMMEDIATE");
        }
        try {
            if (verify) {
                verifyVersion();
            }
            T result = body.apply(connection);
            try (Statement s = connection.createStatement()) {
                s.execute("COMMIT");
            }
            return result;
        } catch (Throwable t) {
            rollbackQuietly();
            throw t;
        }
    }

    private void verifyVersion() throws SQLException {
        int found = SqliteSchema.pragmaInt(connection, "user_version");
        if (failed) {
            throw SqliteSchema.versionMismatch(
                    "this instance stopped after the schema version changed", found, baselineVersion);
        }
        if (found != baselineVersion) {
            failed = true;
            throw SqliteSchema.versionMismatch("unsupported schema version", found, baselineVersion);
        }
    }

    private void rollbackQuietly() {
        try (Statement s = connection.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // 트랜잭션이 이미 끝났을 수 있다.
        }
    }

    <T> T retryOnBusy(SqlAction<T> action) {
        return retryOnBusy(action, Deadline.NONE);
    }

    <T> T retryOnBusy(SqlAction<T> action, Deadline deadline) {
        acquire(deadline);
        try {
            for (int attempt = 1;; attempt++) {
                try {
                    return action.run();
                } catch (SQLException e) {
                    if (!isBusy(e)) {
                        throw new SmqStorageException("sqlite operation failed", e);
                    }
                    if (deadline.passed()) {
                        throw new Deadline.Expired();
                    }
                    if (attempt >= retry.maxAttempts()) {
                        throw new SmqStorageException("sqlite operation failed", e);
                    }
                    try {
                        retry.backoff().await(attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SmqStorageException("interrupted while retrying sqlite operation", e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** lock을 잡는다. 마감이 있으면 남은 시간(하한 {@link Deadline#LOCK_WAIT_FLOOR_NANOS}) 안에서만 기다리고, 못 잡으면 {@link Deadline.Expired}를 던진다. */
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
            Thread.currentThread().interrupt();
            throw new SmqStorageException("interrupted while waiting for sqlite lock", e);
        }
    }

    static boolean isBusy(SQLException e) {
        // 확장 결과 코드의 아래 8비트가 기본 결과 코드다.
        return (e.getErrorCode() & 0xFF) == SQLITE_BUSY;
    }
}
