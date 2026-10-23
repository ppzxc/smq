package kr.nanoit.smq.prototype.b;

import java.time.Duration;
import java.util.List;
import kr.nanoit.smq.prototype.MessageId;
import kr.nanoit.smq.prototype.OutgoingMessage;
import kr.nanoit.smq.prototype.Outcomes;
import kr.nanoit.smq.prototype.QueueCounts;
import kr.nanoit.smq.prototype.QueueOptions;

/**
 * 변형 B의 큐 핸들. 스레드 안전하다. 정산과 연장도 큐의 연산이다. 저장소 SPI(declare/delete/counts/publish/pull/
 * settle/extend)와 거의 일대일로 대응한다.
 */
public interface Queue {

    String name();

    QueueOptions options();

    QueueCounts counts();

    List<MessageId> publish(List<OutgoingMessage> batch);

    default MessageId publish(OutgoingMessage message) {
        return publish(List.of(message)).getFirst();
    }

    /** 변형 A와 같다. 하나도 없이 {@code wait}가 지나면 빈 청크. */
    Chunk pull(int max, Duration wait) throws InterruptedException;

    Chunk pull(int max, Duration wait, Duration leaseDuration) throws InterruptedException;

    /**
     * 정산 하나를 트랜잭션 하나로 반영한다. 정산 값에 리스 토큰이 들어 있다.
     *
     * @throws kr.nanoit.smq.prototype.NoDeadLetterQueueException reject가 있는데 dead letter 큐가 없다. 정산 전체가
     *     반영되지 않는다.
     */
    Outcomes settle(Settlement settlement);

    /**
     * 청크의 리스 기한을 지금부터 {@code leaseDuration} 뒤로 옮긴다. 청크는 불변이라 새 기한은 결과로 돌려준다.
     *
     * <p>QUESTION: 이미 정산한 메시지를 B는 모른다. 그 메시지들은 큐에 없거나 다른 리스라 STALE로 돌아온다. 호출자가
     * 혼동하지 않을지.
     */
    Extension extend(Chunk chunk, Duration leaseDuration);

    record Extension(java.time.Instant leaseDeadline, Outcomes outcomes) {}
}
