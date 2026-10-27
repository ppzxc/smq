package io.github.ppzxc.smq;

import java.time.Duration;
import java.util.List;

public interface Queue {

    String name();

    QueueOptions options();

    /** 준비 메시지 개수 등. */
    QueueCounts counts();

    /** 배치를 원자적으로 발행하고 메시지 id를 입력 순서대로 돌려준다. */
    List<MessageId> publish(List<OutgoingMessage> batch);

    MessageId publish(OutgoingMessage message);

    /** 메시지가 하나라도 있으면 바로, 없으면 {@code wait}까지 기다려 최대 max개를 id 순서로 돌려준다. 없으면 빈 청크다. */
    Chunk pull(int max, Duration wait) throws InterruptedException;

    Chunk pull(int max, Duration wait, Duration leaseDuration) throws InterruptedException;

    /** 이 큐에서 꺼낸 청크로 만든 정산만 반영한다. 다른 큐의 청크로 만든 정산은 {@code IllegalArgumentException}이다. */
    Outcomes settle(Settlement settlement);

    /**
     * 청크의 리스를 "지금부터 {@code leaseDuration} 뒤"로 바꾼다. 같은 연장을 다시 해도 안전하다. 기한이 지났어도 아무도 다시 꺼내지
     * 않았다면 {@code OK}이고, 다른 리스가 가져갔거나 이미 정산된 메시지는 효력 없이 {@code STALE}이다.
     * 다른 큐의 청크는 {@code IllegalArgumentException}이다.
     */
    Extension extend(Chunk chunk, Duration leaseDuration);
}
