package kr.nanoit.smq.prototype;

/**
 * smq가 던지는 예외의 뿌리. 전부 unchecked다.
 *
 * <p>인자 자체가 잘못된 경우(음수 개수, null, 빈 발행 배치)는 smq 고유 타입을 만들지 않고 JDK의 {@link
 * IllegalArgumentException}을 쓴다. 이 계층은 큐의 상태 때문에 생기는 오류와 저장소 오류만 담는다.
 *
 * <p>QUESTION: sealed로 닫을지. 닫으면 호출자가 switch로 빠짐없이 다룰 수 있지만 v1 뒤에 예외를 추가하기 어렵다.
 */
public abstract class SmqException extends RuntimeException {

    protected SmqException(String message, Throwable cause) {
        super(message, cause);
    }
}
