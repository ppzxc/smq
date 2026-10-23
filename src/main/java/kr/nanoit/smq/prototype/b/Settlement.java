package kr.nanoit.smq.prototype.b;

import java.time.Duration;
import kr.nanoit.smq.prototype.MessageId;

/**
 * 변형 B: 정산 하나를 나타내는 불변 값. 리스 토큰과 메시지별 처리 결과(ACK, NACK+지연, reject)를 담는다. {@link
 * Queue#settle(Settlement)}로 반영하기 전까지는 아무 일도 없다.
 *
 * <p>"나머지"는 이 정산 값을 만드는 동안 담지 않은 청크의 메시지다. 이전에 따로 반영한 정산은 모른다. 그래서 여러 번
 * 나눠 정산한 뒤 {@code ackRest()}를 하면 앞서 정산한 메시지가 다시 담기고 STALE로 돌아온다.
 *
 * <p>정산이 {@code SmqStorageException}으로 끝나면 같은 값을 그대로 다시 {@code settle}하면 된다. 값이 불변이라
 * "무엇을 정산하려 했는지"가 사라지지 않는다.
 */
public interface Settlement {

    interface Builder {
        Builder ack(MessageId id);

        Builder nack(MessageId id);

        Builder nack(MessageId id, Duration delay);

        Builder reject(MessageId id);

        /** 여기까지 담지 않은 청크의 나머지 메시지 전부를 ACK로 담고 완성한다. */
        Settlement ackRest();

        Settlement nackRest(Duration delay);

        /** 담은 것만으로 완성한다. */
        Settlement build();
    }
}
