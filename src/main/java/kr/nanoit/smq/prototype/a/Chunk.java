package kr.nanoit.smq.prototype.a;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import kr.nanoit.smq.prototype.Message;
import kr.nanoit.smq.prototype.MessageId;
import kr.nanoit.smq.prototype.Outcome;
import kr.nanoit.smq.prototype.Outcomes;

/**
 * 변형 A: 꺼낸 청크이자 그 리스의 핸들. <b>상태가 있다.</b> 어떤 메시지를 이미 정산했는지 기억해서 "나머지 전부"를
 * 계산한다. 리스 토큰은 이 안에 숨어 있고 공개되지 않는다.
 *
 * <p>각 메서드가 저장소 트랜잭션 하나다. SQLite에서는 쓰기 트랜잭션이 프로세스 전체에서 하나뿐이므로, 메시지마다 {@link
 * #ack(MessageId)}를 부르면 청크 크기만큼 트랜잭션이 생긴다. 여러 메시지를 한 번에 정산하려면 {@link #settle()}을 쓴다.
 *
 * <p>QUESTION: 스레드 안전하게 만들지. 청크를 여러 스레드에 나눠 처리하고 각자 ACK하는 사용 방식을 지원할지. 지원하면
 * "나머지" 계산과 정산이 경합한다.
 *
 * <p>QUESTION: 정산이 {@code SmqStorageException}으로 끝나면 그 메시지를 "정산함"으로 기억할지 "아직"으로 기억할지.
 * 효력이 불명이라 어느 쪽도 맞지 않다. 지금 생각은 "아직"으로 두어 다시 정산하게 하고, 이미 반영됐다면 STALE로 돌아온다.
 */
public interface Chunk {

    /** 꺼낸 순서(FIFO)대로. 빈 청크면 빈 리스트. */
    List<Message> messages();

    boolean isEmpty();

    /** 리스 기한. 연장하면 바뀐다. 빈 청크면 의미 없다. */
    Instant leaseDeadline();

    /** 아직 이 핸들로 정산하지 않은 메시지. */
    List<MessageId> unsettled();

    // ---- 개별 정산: 메시지 하나에 트랜잭션 하나

    Outcome ack(MessageId id);

    /** 지연 없이 NACK. 메시지는 큐 맨 앞으로 간다. */
    Outcome nack(MessageId id);

    Outcome nack(MessageId id, Duration delay);

    /** @throws kr.nanoit.smq.prototype.NoDeadLetterQueueException 큐에 dead letter 큐가 없다 */
    Outcome reject(MessageId id);

    // ---- 청크 전체/나머지 정산: 트랜잭션 하나

    /** 아직 정산하지 않은 메시지 전부를 ACK. 아무것도 정산하지 않았다면 청크 전체다. */
    Outcomes ackRest();

    Outcomes nackRest();

    Outcomes nackRest(Duration delay);

    // ---- 섞인 정산: 트랜잭션 하나

    /** 여러 메시지에 서로 다른 처리 결과를 담아 한 번에 반영한다. */
    SettleBuilder settle();

    interface SettleBuilder {
        SettleBuilder ack(MessageId id);

        SettleBuilder nack(MessageId id, Duration delay);

        SettleBuilder reject(MessageId id);

        /** 여기까지 담지 않은 나머지 전부를 ACK하고 반영한다. */
        Outcomes ackRest();

        /** 여기까지 담지 않은 나머지 전부를 NACK하고 반영한다. */
        Outcomes nackRest(Duration delay);

        /** 담은 것만 반영한다. 나머지는 리스 중으로 남는다. */
        Outcomes apply();
    }

    // ---- 연장

    /**
     * 리스 기한을 지금부터 {@code leaseDuration} 뒤로 옮긴다. 이미 정산한 메시지는 대상이 아니다.
     *
     * <p>QUESTION: "지금부터 d"(현재 생각)인지 "현재 기한에 d를 더함"인지. 앞쪽이 멱등이라 재시도에 안전하다.
     *
     * @return 메시지별 결과. 이미 다른 리스로 넘어간 메시지는 STALE.
     */
    Outcomes extend(Duration leaseDuration);
}
