package io.github.ppzxc.smq;

/**
 * SQLite 저장소 파일을 이미 다른 곳이 쥐고 있어 열 수 없을 때 던진다.
 *
 * <p>smq의 SQLite 저장소는 한 JVM만 연다(ADR-0012). 같은 파일을 다른 프로세스가 열었거나, 같은 JVM이 이미 열었거나,
 * 외부 SQLite 도구가 파일을 쥐고 있으면 기다리지 않고 바로 이 예외가 올라온다. 메시지에 DB 경로와 락 파일 경로가
 * 있다. 아무것도 변경되지 않았다.
 */
public class StorageLockedException extends SmqException {

    public StorageLockedException(String message) {
        super(message);
    }

    public StorageLockedException(String message, Throwable cause) {
        super(message, cause);
    }
}
