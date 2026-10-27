package io.github.ppzxc.smq;

/**
 * 저장소 연산의 효력을 알 수 없을 때 던진다. 반영됐을 수도, 안 됐을 수도 있다.
 *
 * <p>SQLite 저장소는 쓰기 경합({@code SQLITE_BUSY})을 내부에서 트랜잭션 전체 단위로 다시 시도한다.
 * 이 예외는 그 재시도로도 풀리지 않았거나 그 밖의 저장소 오류일 때 올라온다. 다른 프로세스가 쓰기 락을 오래 쥐면
 * 재시도가 끝나기까지 같은 프로세스의 호출이 서로 직렬로 막혀 최대 약 25초(기본값 기준)가 걸린다. 꺼내기만
 * {@code wait}로 한정되고 빈 청크로 돌아온다({@link Queue#pull(int, java.time.Duration)}).
 *
 * <p>롤백이 확실한 실패(예: {@code SQLITE_BUSY} 재시도 소진)도 이 예외로 올라온다. 호출자는 롤백 여부를 구분할
 * 수 없지만, 아래 규칙대로 다시 해도 안전하다.
 *
 * <p>재시도 규칙:
 * <ul>
 *   <li>발행은 다시 해도 된다. 앞선 발행이 반영됐다면 메시지가 중복될 수 있고, 이는 at-least-once 범위 안이다.</li>
 *   <li>정산은 같은 {@code Settlement} 값으로 다시 하면 된다. 이미 반영됐다면 해당 메시지는 {@code STALE}로 나온다.</li>
 * </ul>
 */
public class SmqStorageException extends SmqException {

    public SmqStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
