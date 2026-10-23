package kr.nanoit.smq.prototype.b;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import kr.nanoit.smq.prototype.Message;
import kr.nanoit.smq.prototype.MessageId;

/**
 * 변형 B: 꺼낸 청크. <b>불변 값</b>이라 여러 스레드에 그대로 넘겨도 된다. 정산 상태를 기억하지 않는다.
 *
 * <p>리스 토큰은 공개 필드가 아니다. {@link #settle()}로 만든 {@link Settlement}가 안에 품고 다닌다.
 *
 * <p>QUESTION: 리스 토큰을 공개할지. 공개하면 청크를 직렬화해 다른 스레드나 작업 큐로 넘긴 뒤 토큰과 메시지 id만으로
 * 정산할 수 있다. 같은 프로세스 밖으로는 의미가 없으니 v1에서는 숨긴다는 쪽이 지금 생각.
 */
public record Chunk(String queue, List<Message> messages, Instant leaseDeadline) {

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /** 이 청크의 리스로 정산 값을 만들기 시작한다. */
    public Settlement.Builder settle() {
        throw new UnsupportedOperationException("prototype");
    }

    /** 청크 전체 ACK. */
    public Settlement ackAll() {
        return settle().ackRest();
    }

    /** 청크 전체 NACK. */
    public Settlement nackAll(Duration delay) {
        return settle().nackRest(delay);
    }

    /** 메시지 하나만 ACK. 나머지는 리스 중으로 남는다. */
    public Settlement ack(MessageId id) {
        return settle().ack(id).build();
    }
}
