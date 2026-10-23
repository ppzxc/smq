package kr.nanoit.smq.prototype;

import java.time.Duration;
import java.util.Map;

/**
 * 발행 배치에 담을 메시지. 아직 메시지 id가 없다.
 *
 * <p>QUESTION: 이름. 발행하는 쪽 값이라 {@code Message}와 구분해야 한다. 후보는 {@code OutgoingMessage}, {@code
 * NewMessage}, {@code Publish.Entry}.
 *
 * @param delay {@link Duration#ZERO}면 바로 준비 상태가 된다. 양수면 지연됨 상태로 들어간다.
 */
public record OutgoingMessage(byte[] payload, Map<String, String> headers, Duration delay) {

    public static OutgoingMessage of(byte[] payload) {
        return new OutgoingMessage(payload, Map.of(), Duration.ZERO);
    }

    public OutgoingMessage withHeaders(Map<String, String> headers) {
        return new OutgoingMessage(payload, Map.copyOf(headers), delay);
    }

    public OutgoingMessage withDelay(Duration delay) {
        return new OutgoingMessage(payload, headers, delay);
    }
}
