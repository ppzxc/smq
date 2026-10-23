package kr.nanoit.smq.prototype;

/**
 * 저장소가 연산을 끝까지 확인해 주지 못했다. <b>효력이 불명</b>이다: 반영됐을 수도, 안 됐을 수도 있다.
 *
 * <p>발행에서 나면 다시 발행해도 된다(at-least-once라 중복은 컨슈머가 걸러 낸다). 정산에서 나면 같은 정산을 다시 하면
 * 된다. 이미 반영됐다면 그 메시지들은 {@link Outcome#STALE}로 돌아온다.
 *
 * <p>SQLite의 {@code SQLITE_BUSY}가 바쁨 대기 한도를 넘긴 경우, 디스크 오류, 파일 손상 등을 감싼다.
 */
public final class SmqStorageException extends SmqException {

    public SmqStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
