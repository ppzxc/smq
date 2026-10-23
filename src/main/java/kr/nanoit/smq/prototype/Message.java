package kr.nanoit.smq.prototype;

import java.time.Instant;
import java.util.Map;

/**
 * 꺼낸 청크에 담긴 메시지. payload, 헤더, 시스템 필드로 이루어진다.
 *
 * @param deliveryCount 이번 전달을 포함한 전달 횟수. 1이면 처음 전달이다.
 * @param publishedAt 발행 시각(코어가 주입한 {@code InstantSource} 기준).
 */
public record Message(
        MessageId id, byte[] payload, Map<String, String> headers, int deliveryCount, Instant publishedAt) {

    /** 1보다 크면 이전에 한 번 이상 전달된 적이 있다. 중복 처리를 의심할 근거. */
    public boolean isRedelivered() {
        return deliveryCount > 1;
    }
}
