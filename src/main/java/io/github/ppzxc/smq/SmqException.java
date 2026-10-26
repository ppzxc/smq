package io.github.ppzxc.smq;

/** smq가 던지는 모든 예외의 뿌리. 인자 오류는 {@link IllegalArgumentException}을 쓴다. */
public class SmqException extends RuntimeException {

    public SmqException(String message) {
        super(message);
    }

    public SmqException(String message, Throwable cause) {
        super(message, cause);
    }
}
