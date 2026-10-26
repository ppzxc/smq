package io.github.ppzxc.smq;

/** 저장소 연산의 효력을 알 수 없을 때 던진다. 발행은 다시 해도 되고, 정산은 같은 값으로 다시 하면 된다. */
public class SmqStorageException extends SmqException {

    public SmqStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
