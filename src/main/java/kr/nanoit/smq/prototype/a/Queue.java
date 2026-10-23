package kr.nanoit.smq.prototype.a;

import java.time.Duration;
import java.util.List;
import kr.nanoit.smq.prototype.MessageId;
import kr.nanoit.smq.prototype.OutgoingMessage;
import kr.nanoit.smq.prototype.QueueCounts;
import kr.nanoit.smq.prototype.QueueOptions;

/** 변형 A의 큐 핸들. 스레드 안전하다. 정산은 큐가 아니라 {@link Chunk}에 건다. */
public interface Queue {

    String name();

    /** 이 핸들을 얻을 때 저장소에서 읽은 선언 속성. */
    QueueOptions options();

    QueueCounts counts();

    /**
     * 발행 배치. 전부 발행되거나 하나도 발행되지 않는다.
     *
     * @return 배치 순서대로 붙은 메시지 id
     * @throws IllegalArgumentException 배치가 비었다
     */
    List<MessageId> publish(List<OutgoingMessage> batch);

    /** 메시지 하나짜리 발행 배치. */
    default MessageId publish(OutgoingMessage message) {
        return publish(List.of(message)).getFirst();
    }

    /**
     * 준비 상태의 메시지를 최대 {@code max}개 꺼낸다. 하나도 없으면 {@code wait}까지 기다린다. 기다려도 없으면 빈 청크를
     * 돌려준다(null도 Optional도 아니다).
     *
     * <p>리스 기한은 큐 선언 속성의 기본값을 쓴다.
     *
     * <p>QUESTION: 하나라도 오면 바로 돌려줄지(지금), {@code max}개가 찰 때까지 조금 더 기다릴지. 후자는 RabbitMQ에 없고
     * 배치 효율에 좋지만 인자가 하나 더 는다.
     *
     * @param wait {@link Duration#ZERO}면 기다리지 않는다. 이름이 {@code delay}가 아닌 것은 용어집의 지연과 구분하려고.
     * @throws InterruptedException 기다리는 동안 인터럽트됐다. 아무것도 꺼내지 않은 상태다.
     */
    Chunk pull(int max, Duration wait) throws InterruptedException;

    /** 리스 기한을 이번 꺼내기에만 따로 준다. */
    Chunk pull(int max, Duration wait, Duration leaseDuration) throws InterruptedException;
}
