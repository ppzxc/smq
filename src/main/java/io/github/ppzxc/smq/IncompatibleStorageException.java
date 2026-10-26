package io.github.ppzxc.smq;

/** 저장소 파일이 smq 파일이 아니거나 스키마 버전이 맞지 않을 때 던진다. */
public class IncompatibleStorageException extends SmqException {

    public IncompatibleStorageException(String message) {
        super(message);
    }
}
