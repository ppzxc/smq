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

final class StorageLock implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(StorageLock.class.getName());

    private final FileChannel channel;
    private final FileLock lock;

    private StorageLock(final FileChannel channel, final FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static Path lockPathOf(final Path db) {
        return db.resolveSibling(db.getFileName() + ".lock");
    }

    static StorageLock acquire(final Path db) {
        final FileChannel channel;
        final Path lockPath;
        lockPath = lockPathOf(db);

        try {
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new SmqStorageException("cannot open lock file " + lockPath, e);
        }
        try {
            final FileLock lock;
            lock = channel.tryLock();
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

    static StorageLockedException locked(final Path db, final Path lockPath, final @Nullable Throwable cause) {
        final String message;
        message = "storage is already in use by another opener (smq allows a single JVM): db=" + db + ", lock file="
                + lockPath;
        return cause == null ? new StorageLockedException(message) : new StorageLockedException(message, cause);
    }

    @Override
    public void close() {
        try {
            lock.release();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "failed to release storage lock", e);
        }
        closeQuietly(channel);
    }

    private static void closeQuietly(final FileChannel c) {
        try {
            c.close();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "failed to close storage lock channel", e);
        }
    }
}
