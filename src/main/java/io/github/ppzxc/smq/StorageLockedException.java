package io.github.ppzxc.smq;

public class StorageLockedException extends SmqException {

    public StorageLockedException(final String message) {
        super(message);
    }

    public StorageLockedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
