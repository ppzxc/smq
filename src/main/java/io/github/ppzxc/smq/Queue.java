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
     * <p>SQLite 저장소에서 {@code wait}는 저장소의 lock과 쓰기 경합({@code SQLITE_BUSY})을 기다리는 시간도 한정한다.
     * 다른 프로세스나 같은 프로세스의 다른 호출이 쓰기 락을 오래 쥐면 {@code wait} 안에 꺼내지 못하고 빈 청크로 돌아온다.
     * 정상적인 짧은 직렬화는 {@code wait}가 0이어도 기다린다(lock 대기 하한, 기본 100ms, {@code SqliteBuilder.lockWaitFloor}로 바꾼다). SQLite가 한 번 기다리기 시작한
     * 쓰기 락 대기({@code busy_timeout}, 기본 5초)는 끊을 수 없어, 최악은 {@code wait}에 그 한 번을 더한 시간이다.
     * 여기에 쓰기 커밋 뒤 WAL을 되감는 체크포인트가 읽는 연결을 기다리는 시간(최대 50ms, ADR-0010)이 {@code wait}와 별개로 더 붙을 수 있다.
     * 이 한도는 꺼내기에만 있다. 선언, 발행, 정산, 연장, {@link Smq#close()}는 다른 프로세스가 쓰기 락을 오래 쥘 때
     * 재시도가 끝날 때까지(기본값 기준 최대 약 25초) 같은 프로세스의 다른 호출과 함께 직렬로 막힐 수 있다.
     */
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
