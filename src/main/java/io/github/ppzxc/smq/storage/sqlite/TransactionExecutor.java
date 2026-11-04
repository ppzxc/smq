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

/**
 * SQLite 저장소의 쓰기 경로. 단일 연결과 단일 lock, 그 lock 대기의 마감을 한 곳에서 소유한다(ADR-0007, ADR-0016).
 * 꺼내기 대기용 autocommit 읽기 {@link #anyOrUncertain}도 이 모듈을 거친다. 한 JVM만 파일을 열므로(ADR-0012)
 * 쓰기 경합을 재시도하지 않고, 트랜잭션마다 스키마 버전을 다시 확인하지도 않는다. 스키마는 열 때 한 번 판정한다.
 *
 * <p>모든 큐 연산은 {@link #execute}로 {@code BEGIN IMMEDIATE} 트랜잭션 하나를 lock 아래에서 실행한다.
 */
final class TransactionExecutor {

    private static final System.Logger LOG = System.getLogger(TransactionExecutor.class.getName());

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
    /** 연속 실패 중 첫 번째와 이 횟수의 배수번째 실패마다 WARNING을 남긴다. */
    static final int CHECKPOINT_FAILURE_LOG_EVERY = 64;
    /** WAL 파일이 이 크기를 넘으면 되감는 체크포인트를 건다. 60초 부하에서 64MB 이하라는 목표보다 한참 작다. */
    static final long CHECKPOINT_WAL_BYTES = 8L * 1024 * 1024;
    /**
     * 되감은 WAL 파일을 이 크기까지만 남기고 더 큰 부분은 줄인다({@code journal_size_limit}). 한도의 1.5배이고, 파일은 이 아래에서는
     * 줄지 않는다.
     */
    static final long WAL_FILE_KEEP_BYTES = 12L * 1024 * 1024;
    /**
     * 저장소를 열 때 연결에 거는 WAL 설정. 자동 체크포인트를 끄고, 되감은 파일의 상한을 {@link #WAL_FILE_KEEP_BYTES}로 두고, 저널 모드를
     * WAL로 바꾼다. 이 순서로 건다. 열 때와 테스트가 같은 것을 쓴다.
     */
    static void configureWal(Statement s) throws SQLException {
        s.execute("PRAGMA wal_autocheckpoint = 0");
        s.execute("PRAGMA journal_size_limit = " + WAL_FILE_KEEP_BYTES);
        s.execute("PRAGMA journal_mode = WAL");
    }

    private static final Path NO_WAL = Path.of("");

    private final Connection connection;
    /** 커밋 횟수. lock 아래에서만 바뀐다. */
    private long commits;
    /** 처음 확인할 때 찾는다. 파일 없는 데이터베이스면 {@link #NO_WAL}로 남는다. */
    private Path walFile = NO_WAL;
    /** WAL 위치를 확인했는지. {@link #NO_WAL}도 확인한 결과이므로 다시 묻지 않는다. */
    private boolean walFileLocated;
    /** 마지막으로 되감은 직후의 WAL 파일 크기(최대 {@link #WAL_FILE_KEEP_BYTES}). 파일이 이보다 커져야 다시 되감는다. */
    private long walSizeAtRestart;
    /** 체크포인트 확인 실패(되감기 실패, 크기 읽기 실패) 연속 횟수. 연속 실패 로그를 줄이려고 둔다. 로그용이며 재시도에는 쓰지 않는다. */
    private long consecutiveCheckpointFailures;

    private final WalSizeReader walSizeReader;

    private final ReentrantLock lock = new ReentrantLock();

    /** WAL 파일 크기 읽기. 테스트가 실패를 주입할 수 있게 갈라 둔다. */
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
     * 확실하지 않으면(큐가 없으면) 참을 돌려 쓰기 경로가 다시 확인하게 한다. SQL 오류는 BUSY도 예외다.
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
            throw new SmqStorageException("sqlite operation failed", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 쓰기 트랜잭션 하나를 lock 아래에서 실행한다. 커밋까지의 SQL 오류는 효력 불명이므로 {@link SmqStorageException}이다.
     * 커밋 뒤 체크포인트의 오류는 삼키므로 이미 커밋된 결과를 그대로 돌려준다.
     */
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
            connection.close();
        } catch (SQLException ignored) {
            // 닫기 실패는 무시한다.
        } finally {
            lock.unlock();
        }
    }

    private <T> T runOnce(TxBody<T> body) throws SQLException {
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
            rollbackQuietly();
            throw t;
        }
        // 커밋이 확정됐다. 이 아래의 어떤 실패도 롤백이나 호출자의 실패로 이어지면 안 된다.
        checkpointIfWalIsLarge();
        return result;
    }

    /**
     * SQLite의 자동 체크포인트는 WAL이 정산량과 무관하게 자라는 것을 막지 못한다(ADR-0017).
     * 그래서 자동 체크포인트를 끄고(열 때 {@code wal_autocheckpoint = 0}), 커밋 몇 번마다 WAL 파일 크기를 보다가 한도를 넘으면
     * {@code RESTART} 체크포인트로 되감는다. 파일은 줄이지 않는다. 줄이면 이후 쓰기마다 파일이 다시 늘면서
     * 크기 변경이 fsync에 실려 단건 청크 처리량이 떨어진다. {@code RESTART}는 되감은 뒤에도 파일이 한도 이상이므로 크기만 보면 다시
     * 되감을 필요가 없을 때도 계속 건다. 그래서 되감은 직후의 크기를 기억해 파일이 그보다 더 커졌을 때(되감은 구간을 다 채웠을 때)만 다시 건다.
     * 되감지 못했으면(결과 행 첫 열이 0이 아니거나 예외) 기억값을 0으로 되돌려, 낡은 기억값 때문에 다음 확인이 시도를 건너뛰지 않게 한다. 되감기에 성공한 뒤 크기를 못 읽으면
     * 되감기는 실패가 아니다. 기억값을 0(다음 확인에서 다시 걸 수 있는 쪽)으로 두고 따로 로그한다. SQL 오류든 런타임 예외든
     * (예: {@code InvalidPathException}) 심지어 {@code Error}든 삼키고 로그만 남긴다. 이미 커밋된 연산을 실패로 올리면 호출자가 재시도해 중복이 생기기
     * 때문이다. 재시도 상태는 두지 않고 다음 정규 확인(커밋 8번 뒤)이 다시 시도한다. 계속 실패해도 로그가 쏟아지지 않게, 실패(예외와
     * 되감지 못함)가 이어지는 동안은 첫 번째와 {@value #CHECKPOINT_FAILURE_LOG_EVERY}번째마다(연속 실패 횟수 포함) WARNING으로
     * 남기고, 되감기에 성공하거나 WAL이 한도 아래라 확인이 통과하면 횟수를 지운다.
     */
    private void checkpointIfWalIsLarge() {
        commits++;
        if (commits % CHECKPOINT_CHECK_EVERY_COMMITS != 0) {
            return;
        }
        try {
            checkWalSizeAndRewind();
        } catch (Throwable t) {
            // 커밋은 이미 확정됐다. Error(LinkageError, StackOverflowError, OutOfMemoryError 등)를 올려도 JVM을 살리지도,
            // 커밋을 되돌리지도 못하고 호출자가 성공한 쓰기를 실패로 보고 재시도해 중복만 만든다. VirtualMachineError는 복구할 수
            // 없지만 같은 이유로 여기서 삼킨다. 진짜로 망가진 JVM이면 다음 할당이나 호출에서 호출자에게 다시 드러난다.
            // ThreadDeath도 같다(Thread.stop은 폐기됐다). 로그조차 못 남기는 상태일 수 있어 로그 실패는 무시한다.
            walSizeAtRestart = 0;
            try {
                checkpointFailed(CHECKPOINT_UNEXPECTED, t);
            } catch (Throwable ignored) {
                // 로그를 남기지 못해도 커밋 결과에는 영향이 없다.
            }
        }
    }

    private void checkWalSizeAndRewind() {
        if (!walFileLocated) {
            try {
                walFile = locateWalFile();
                walFileLocated = true;
            } catch (SQLException | RuntimeException e) {
                // 위치를 모르면 크기도 볼 수 없다. 확인하지 못했을 뿐 체크포인트를 시도한 것은 아니다.
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
                // -wal이 아직 없거나 지워졌다. 오류가 아니라 크기 0(한도 아래)이다.
                size = 0;
            } catch (IOException | RuntimeException e) {
                // 크기를 모르면 "한도 아래"로 볼 수 없다. 연속 실패 상태를 지우지 않고 다음 확인에 맡긴다.
                checkpointFailed(WAL_SIZE_UNREADABLE, e);
                return;
            }
        }
        if (size < CHECKPOINT_WAL_BYTES) {
            consecutiveCheckpointFailures = 0;
            return;
        }
        if (size <= walSizeAtRestart) {
            // 되감은 구간을 아직 다 채우지 못했다. 되감기 성공 때 이미 횟수를 지웠고, 실패하면 기억값을 0으로 되돌리므로
            // 여기서 실패 상태를 지울 일은 없다.
            return;
        }
        try (Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA wal_checkpoint(RESTART)")) {
            // 첫 열이 0이면 WAL을 끝까지 되감았다.
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

    /** 되감기 실패. 낡은 기억 크기가 다음 시도를 막지 않도록 한도 아래(0)로 되돌린다. 재시도 상태는 아니다. */
    private void rewindFailed(@Nullable Exception cause) {
        walSizeAtRestart = 0;
        checkpointFailed(CHECKPOINT_INCOMPLETE, cause);
    }

    /** 되감기에 성공한 직후의 크기를 기억한다. 못 읽으면 0으로 두어(다음 확인에서 다시 걸 수 있는 쪽) 낡은 값이 남지 않게 한다. */
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
            "sqlite checkpoint check failed unexpectedly after commit; the next check will retry";

    /**
     * 확인이 끝까지 가지 못한 실패(WAL 위치 확인, 크기 읽기, 되감기 실패)를 센다. 연속 실패 중 첫 번째와
     * {@link #CHECKPOINT_FAILURE_LOG_EVERY}번째마다 WARNING으로 남긴다. 단계마다 메시지만 다르다.
     */
    private void checkpointFailed(String what, @Nullable Throwable cause) {
        long failures = ++consecutiveCheckpointFailures;
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
