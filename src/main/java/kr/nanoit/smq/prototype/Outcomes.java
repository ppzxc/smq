package kr.nanoit.smq.prototype;

import java.util.Map;
import java.util.Set;

/**
 * 한 번의 정산이나 연장에 담긴 메시지들의 결과. 낡은 연산은 예외가 아니라 여기서 {@link Outcome#STALE}로 보인다.
 *
 * <p>QUESTION: 대부분의 호출자는 "전부 OK인가"만 본다. {@code Map} 하나로 충분한지, {@code stale()} 같은 편의
 * 메서드가 필요한지.
 */
public record Outcomes(Map<MessageId, Outcome> byMessage) {

    public boolean allOk() {
        return byMessage.values().stream().allMatch(o -> o == Outcome.OK);
    }

    public Set<MessageId> stale() {
        throw new UnsupportedOperationException("prototype");
    }
}
