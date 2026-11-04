package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.SmqStorageException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQLite 저장소의 쓰기 경로. 단일 연결과 단일 lock, 그 lock 대기의 마감을 한 곳에서 소유한다(ADR-0007, ADR-0009).
 * 꺼내기 대기용 autocommit 읽기 {@link #anyOrUncertain}도 이 모듈을 거친다. 한 JVM만 파일을 열므로(ADR-0012)
 * 쓰기 경합을 재시도하지 않고, 트랜잭션마다 스키마 버전을 다시 확인하지도 않는다. 스키마는 열 때 한 번 판정한다.
 *
 * <p>모든 큐 연산은 {@link #execute}로 {@code BEGIN IMMEDIATE} 트랜잭션 하나를 lock 아래에서 실행한다.
 */
final class TransactionExecutor {

    /** SQLite 기본 결과 코드 {@code SQLITE_BUSY}. */
    private static final int SQLITE_BUSY = 5;

    @FunctionalInterface
    interface TxBody<T> {
        T apply(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    interface SqlAction<T> {
        T run() throws SQLException;
    }

    /** 커밋 몇 번마다 WAL 파일 크기를 확인할지. 확인은 파일 크기 조회 한 번이다. */
    static final int CHECKPOINT_CHECK_EVERY_COMMITS = 8;
    /** WAL 파일이 이 크기를 넘으면 되감는 체크포인트를 건다. 60초 부하에서 64MB 이하라는 목표보다 한참 작다. */
    static final long CHECKPOINT_WAL_BYTES = 8L * 1024 * 1024;
    /**
     * 되감은 WAL 파일을 이 크기까지만 남기고 더 큰 부분은 줄인다({@code journal_size_limit}). 한도의 1.5배이고, 파일은 이 아래에서는
     * 줄지 않는다.
     */
    static final long WAL_FILE_KEEP_BYTES = 12L * 1024 * 1024;
    /**
     * 되감기에 실패한 뒤 이 커밋 수만큼은 다시 시도하지 않는다. 방어 코드: 락 파일은 권고라서 이를 우회한 외부 도구가
     * 읽고 있으면 되감기가 막힐 수 있다.
     */
    private static final int CHECKPOINT_RETRY_AFTER_COMMITS = 8;

    // 실험(#139/#149): manual(현재), auto(자동 체크포인트, journal_size_limit 없음), auto-jsl(자동 + journal_size_limit),
    // auto-<N>(wal_autocheckpoint=N + journal_size_limit).
    static final String EXP_MODE = System.getenv().getOrDefault("SMQ_EXP_CHECKPOINT", "manual");

    /**
     * 저장소를 열 때 연결에 거는 WAL 설정. 자동 체크포인트를 끄고, 되감은 파일의 상한을 {@link #WAL_FILE_KEEP_BYTES}로 두고, 저널 모드를
     * WAL로 바꾼다. 이 순서로 건다. 열 때와 테스트가 같은 것을 쓴다.
     */
    static void configureWal(Statement s) throws SQLException {
        if (EXP_MODE.equals("manual")) {
            s.execute("PRAGMA wal_autocheckpoint = 0");
        } else if (EXP_MODE.startsWith("auto-") && !EXP_MODE.equals("auto-jsl")) {
            // 실험(#149): auto-<N>은 wal_autocheckpoint=N + journal_size_limit.
            s.execute("PRAGMA wal_autocheckpoint = " + Integer.parseInt(EXP_MODE.substring(5)));
        }
        if (!EXP_MODE.equals("auto")) {
            s.execute("PRAGMA journal_size_limit = " + WAL_FILE_KEEP_BYTES);
        }
        s.execute("PRAGMA journal_mode = WAL");
        try (ResultSet a = s.executeQuery("PRAGMA wal_autocheckpoint")) {
            a.next();
            int ac = a.getInt(1);
            try (ResultSet j = s.executeQuery("PRAGMA journal_size_limit")) {
                j.next();
                System.err.println(
                        "[exp] mode=" + EXP_MODE + ", autocheckpoint=" + ac + ", journal_size_limit=" + j.getLong(1));
            }
        }
    }

    private static final Path NO_WAL = Path.of("");

    // 실험(#139): lock을 쥔 시간의 히스토그램. 0.1ms 칸, 1초 이상은 마지막 칸.
    private final ExpHist EXP_TX = new ExpHist();
    private final ExpHist EXP_COMMIT = new ExpHist();
    private final ExpHist EXP_RESTART = new ExpHist();

    static final class ExpHist {
        private final long[] buckets = new long[10_001];
        private long count;
        private long max;

        void add(long nanos) {
            int b = (int) Math.min(nanos / 100_000L, 10_000L);
            buckets[b]++;
            count++;
            max = Math.max(max, nanos);
        }

        private double pct(double q) {
            long target = (long) Math.ceil(count * q);
            long seen = 0;
            for (int i = 0; i < buckets.length; i++) {
                seen += buckets[i];
                if (seen >= target) {
                    return (i + 1) / 10.0;
                }
            }
            return max / 1e6;
        }

        String summary() {
            if (count == 0) {
                return "n=0";
            }
            long over100 = 0;
            long over50 = 0;
            for (int i = 500; i < buckets.length; i++) {
                if (i >= 1000) {
                    over100 += buckets[i];
                }
                over50 += buckets[i];
            }
            return String.format(
                    java.util.Locale.ROOT,
                    "n=%d p50<=%.1fms p99<=%.1fms p99.9<=%.1fms max=%.2fms over50ms=%d over100ms=%d",
                    count, pct(0.5), pct(0.99), pct(0.999), max / 1e6, over50, over100);
        }
    }

    private final Connection connection;
    /** 커밋 횟수. lock 아래에서만 바뀐다. */
    private long commits;
    /** 마지막 확인에서 WAL이 한도를 넘었고 아직 되감지 못했다. {@link #checkpointRetryAtCommit}부터 다시 시도한다. */
    private boolean walOverLimit;
    /** {@link #walOverLimit}일 때 다시 시도할 커밋 횟수. */
    private long checkpointRetryAtCommit;
    /** 처음 확인할 때 찾는다. */
    private Path walFile = NO_WAL;
    /** 마지막으로 되감은 직후의 WAL 파일 크기(최대 {@link #WAL_FILE_KEEP_BYTES}). 파일이 이보다 커져야 다시 되감는다. */
    private long walSizeAtRestart;

    private final ReentrantLock lock = new ReentrantLock();

    TransactionExecutor(Connection connection) {
        this.connection = connection;
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
     * 꺼낼 것이 있는지 autocommit으로 본다. 쓰기 락 없이 읽고 읽기 트랜잭션을 남기지 않는다.
     * 확실하지 않으면(BUSY) 참을 돌려 쓰기 경로가 다시 확인하게 한다. BUSY가 아닌 오류는 예외다.
     */
    boolean anyOrUncertain(ReadProbe probe) {
        return anyOrUncertain(probe, Deadline.NONE);
    }

    /** 마감 안에 lock을 잡지 못하면 {@link Deadline.Expired}를 던진다. 아무것도 읽지 않았다. */
    boolean anyOrUncertain(ReadProbe probe, Deadline deadline) {
        acquire(deadline);
        try {
            return probe.apply(connection);
        } catch (SQLException e) {
            // 방어 코드: 락 파일은 권고라서 이를 우회해 파일을 쥔 외부 도구가 있으면 BUSY가 날 수 있다.
            if (isBusy(e)) {
                return true;
            }
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    /** 쓰기 트랜잭션 하나를 lock 아래에서 실행한다. SQL 오류는 효력 불명이므로 {@link SmqStorageException}이다. */
    <T> T execute(TxBody<T> body) {
        return execute(body, Deadline.NONE);
    }

    /**
     * 마감이 있으면 lock 대기에 적용한다. 마감 안에 lock을 잡지 못하면 {@link Deadline.Expired}를 던진다.
     * 아무것도 반영되지 않았다.
     */
    <T> T execute(TxBody<T> body, Deadline deadline) {
        return runLocked(() -> runOnce(body), deadline);
    }

    void close() {
        lock.lock();
        try {
            System.err.println("[exp-hold] mode=" + EXP_MODE + " tx " + EXP_TX.summary());
            System.err.println("[exp-hold] mode=" + EXP_MODE + " commit " + EXP_COMMIT.summary());
            System.err.println("[exp-hold] mode=" + EXP_MODE + " restart " + EXP_RESTART.summary());
            connection.close();
        } catch (SQLException ignored) {
            // 닫기 실패는 무시한다.
        } finally {
            lock.unlock();
        }
    }

    private <T> T runOnce(TxBody<T> body) throws SQLException {
        long txStart = System.nanoTime();
        try (Statement s = connection.createStatement()) {
            s.execute("BEGIN IMMEDIATE");
        }
        try {
            T result = body.apply(connection);
            long commitStart = System.nanoTime();
            try (Statement s = connection.createStatement()) {
                s.execute("COMMIT");
            }
            long commitEnd = System.nanoTime();
            EXP_COMMIT.add(commitEnd - commitStart);
            checkpointIfWalIsLarge();
            EXP_TX.add(System.nanoTime() - txStart);
            return result;
        } catch (Throwable t) {
            rollbackQuietly();
            throw t;
        }
    }

    /**
     * SQLite의 자동 체크포인트는 WAL이 정산량과 무관하게 자라는 것을 막지 못한다(ADR-0010).
     * 그래서 자동 체크포인트를 끄고(열 때 {@code wal_autocheckpoint = 0}), 커밋 몇 번마다 WAL 파일 크기를 보다가 한도를 넘으면
     * {@code RESTART} 체크포인트로 되감는다. 파일은 줄이지 않는다. 줄이면 이후 쓰기마다 파일이 다시 늘면서
     * 크기 변경이 fsync에 실려 단건 청크 처리량이 떨어진다. {@code RESTART}는 되감은 뒤에도 파일이 한도 이상이므로 크기만 보면 다시
     * 되감을 필요가 없을 때도 계속 건다. 그래서 되감은 직후의 크기를 기억해 파일이 그보다 더 커졌을 때(되감은 구간을 다 채웠을 때)만 다시 건다.
     * 못 되감으면 {@link #CHECKPOINT_RETRY_AFTER_COMMITS}번 커밋 동안은 다시 시도하지 않고 그 뒤에 다시 한다. 이미 커밋된 쓰기의
     * 결과를 바꾸지 않으므로 BUSY는 삼킨다. BUSY가 아닌 오류는 숨기지 않고 {@link SmqStorageException}으로 던진다(이미 커밋됐으므로
     * 호출자에게는 효력 불명이다).
     */
    private void checkpointIfWalIsLarge() {
        if (!EXP_MODE.equals("manual")) {
            return;
        }
        commits++;
        boolean due = walOverLimit ? commits >= checkpointRetryAtCommit : commits % CHECKPOINT_CHECK_EVERY_COMMITS == 0;
        if (!due) {
            return;
        }
        try {
            if (walFile.equals(NO_WAL)) {
                walFile = locateWalFile();
            }
            long size = walFile.equals(NO_WAL) ? 0 : walSize();
            walOverLimit = size >= CHECKPOINT_WAL_BYTES && size > walSizeAtRestart;
            if (!walOverLimit) {
                return;
            }
            try (Statement s = connection.createStatement()) {
                long restartStart = System.nanoTime();
                try (ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(RESTART)")) {
                    // 첫 열이 0이면 WAL을 끝까지 되감았다.
                    walOverLimit = !rs.next() || rs.getInt(1) != 0;
                }
                EXP_RESTART.add(System.nanoTime() - restartStart);
            }
            if (walOverLimit) {
                checkpointRetryAtCommit = commits + CHECKPOINT_RETRY_AFTER_COMMITS;
            } else {
                walSizeAtRestart = Math.min(walSize(), WAL_FILE_KEEP_BYTES);
            }
        } catch (SQLException e) {
            if (!isBusy(e)) {
                throw new SmqStorageException("sqlite checkpoint failed after commit", e);
            }
            // 방어 코드: 락 파일은 권고라서 이를 우회해 파일을 쥔 외부 도구가 있으면 BUSY가 날 수 있다.
            walOverLimit = true;
            checkpointRetryAtCommit = commits + CHECKPOINT_RETRY_AFTER_COMMITS;
        }
    }

    /** 주 데이터베이스 파일의 WAL 경로. 파일이 없는 데이터베이스(메모리, 임시)면 {@link #NO_WAL}. */
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

    private long walSize() {
        try {
            return Files.size(walFile);
        } catch (IOException e) {
            return 0;
        }
    }

    private void rollbackQuietly() {
        try (Statement s = connection.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // 트랜잭션이 이미 끝났을 수 있다.
        }
    }

    /**
     * 트랜잭션 밖에서 하는 열기 단계(저널 모드 전환 등)를 lock 아래에서 마감 없이 한 번 실행한다. 락 파일을 잡은 뒤의 BUSY는
     * smq 밖의 도구가 DB를 쥔 것이라 기다려도 풀리지 않는다(ADR-0012). BUSY도 다른 오류도 {@link SmqStorageException}으로
     * 올리고, 호출자가 원인 사슬의 {@link #isBusy}로 구분한다.
     */
    <T> T executeOutsideTransaction(SqlAction<T> action) {
        return runLocked(action, Deadline.NONE);
    }

    /** lock을 잡고(마감 규칙은 {@link #acquire}) 한 번 실행한다. SQL 오류는 {@link SmqStorageException}이다. */
    private <T> T runLocked(SqlAction<T> action, Deadline deadline) {
        acquire(deadline);
        try {
            return action.run();
        } catch (SQLException e) {
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    /** lock을 잡는다. 마감이 있으면 남은 시간(열 때 정한 하한) 안에서만 기다리고, 못 잡으면 {@link Deadline.Expired}를 던진다. */
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
