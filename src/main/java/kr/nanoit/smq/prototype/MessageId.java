package kr.nanoit.smq.prototype;

/**
 * 메시지가 큐에 들어갈 때 큐가 붙이는 식별자. 재전달되어도 바뀌지 않으므로 컨슈머의 중복 제거 키가 된다.
 *
 * <p>FIFO 키이기도 해서 같은 큐 안에서는 비교할 수 있다.
 *
 * <p>QUESTION: long을 감싼 record로 드러낼지, 내부 표현을 숨긴 불투명 문자열({@code toString()}만 공개)로 둘지.
 * PostgreSQL이 bigserial이면 long으로 통일된다. 큐 이름 없이 id만으로 전역 유일하다고 약속하지는 않는다.
 */
public record MessageId(long value) implements Comparable<MessageId> {

    @Override
    public int compareTo(MessageId other) {
        return Long.compare(value, other.value);
    }
}
