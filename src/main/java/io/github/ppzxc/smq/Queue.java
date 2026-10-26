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

    Outcomes settle(Settlement settlement);
}
