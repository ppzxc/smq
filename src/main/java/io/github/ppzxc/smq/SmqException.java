package io.github.ppzxc.smq;

public class SmqException extends RuntimeException {

    public SmqException(final String message) {
        super(message);
    }

    public SmqException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
