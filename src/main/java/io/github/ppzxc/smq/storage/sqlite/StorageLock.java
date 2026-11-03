package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.SmqStorageException;
import io.github.ppzxc.smq.StorageLockedException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.jspecify.annotations.Nullable;

/**
 * DB 옆의 전용 락 파일에 거는 OS 배타 락. 한 JVM만 저장소를 열게 한다(ADR-0012).
 *
 * <p>락은 DB 파일이 아니라 별도 파일에 건다. POSIX 락은 같은 프로세스가 그 파일의 다른 핸들을 닫으면 풀리기
 * 때문이다. 락 파일은 닫은 뒤에도 남기며, 존재가 아니라 OS 락으로 판단한다. 프로세스가 죽으면 OS가 락을 푼다.
 */
final class StorageLock implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;

    private StorageLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static Path lockPathOf(Path db) {
        return db.resolveSibling(db.getFileName() + ".lock");
    }

    /**
     * 락을 잡는다.
     *
     * @throws StorageLockedException 다른 프로세스나 같은 JVM이 이미 쥐고 있을 때
     * @throws SmqStorageException 락 파일을 만들거나 잠그지 못했을 때(권한, 경로 없음 등)
     */
    static StorageLock acquire(Path db) {
        Path lockPath = lockPathOf(db);
        FileChannel channel;
        try {
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new SmqStorageException("cannot open lock file " + lockPath, e);
        }
        try {
            FileLock lock = channel.tryLock();
            if (lock == null) {
                closeQuietly(channel);
                throw locked(db, lockPath, null);
            }
            return new StorageLock(channel, lock);
        } catch (OverlappingFileLockException e) {
            closeQuietly(channel);
            throw locked(db, lockPath, e);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new SmqStorageException("cannot lock " + lockPath, e);
        }
    }

    static StorageLockedException locked(Path db, Path lockPath, @Nullable Throwable cause) {
        String message = "storage is already in use by another opener (smq allows a single JVM): db=" + db
                + ", lock file=" + lockPath;
        return cause == null ? new StorageLockedException(message) : new StorageLockedException(message, cause);
    }

    @Override
    public void close() {
        try {
            lock.release();
        } catch (IOException ignored) {
            // 채널을 닫으면 락도 풀린다.
        }
        closeQuietly(channel);
    }

    private static void closeQuietly(FileChannel c) {
        try {
            c.close();
        } catch (IOException ignored) {
            // 닫기 실패는 무시한다.
        }
    }
}
