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

    /**
     * 메시지가 하나라도 있으면 바로, 없으면 {@code wait}까지 기다려 최대 max개를 id 순서로 돌려준다. 없으면 빈 청크다.
     *
     * <p>SQLite 저장소에서 {@code wait}는 저장소의 lock을 기다리는 시간도 한정한다.
     * 같은 {@code Smq}의 다른 연산이 오래 걸리면 {@code wait} 안에 꺼내지 못하고 빈 청크로 돌아온다.
     * 정상적인 짧은 직렬화는 {@code wait}가 0이어도 기다린다(lock 대기 하한, 100ms).
     * 여기에 쓰기 커밋 뒤 WAL을 되감는 체크포인트 시간(ADR-0010)이 {@code wait}와 별개로 더 붙을 수 있다.
     * 이 한도는 꺼내기에만 있다. 선언, 발행, 정산, 연장, {@link Smq#close()}는 같은 프로세스의 다른 호출이 락을 쥐고 있는 동안
     * 직렬로 막힐 수 있다.
     */
    Chunk pull(int max, Duration wait) throws InterruptedException;

    Chunk pull(int max, Duration wait, Duration leaseDuration) throws InterruptedException;

    /** 이 큐에서 꺼낸 청크로 만든 정산만 반영한다. 다른 큐의 청크로 만든 정산은 {@code IllegalArgumentException}이다. */
    Outcomes settle(Settlement settlement);

    /**
     * 청크의 리스를 "지금부터 {@code leaseDuration} 뒤"로 바꾼다. 같은 연장을 다시 해도 안전하다. 기한이 지났어도 아무도 다시 꺼내지
     * 않았다면 {@code OK}이고, 다른 리스가 가져갔거나 이미 정산된 메시지는 효력 없이 {@code STALE}이다. 단, 단일 활성
     * 컨슈머 큐에서는 기한이 지난 리스를 연장할 수 없다. 그 사이 다른 청크가 꺼냈을 수 있어, 되살리면 리스가 둘이 되기
     * 때문이며 아무도 다시 꺼내지 않았어도 {@code STALE}이다. 기한 판정 시각은 저장소가 연장을 처리하는 시점이다.
     * 다른 큐의 청크는 {@code IllegalArgumentException}이다.
     */
    Extension extend(Chunk chunk, Duration leaseDuration);
}
