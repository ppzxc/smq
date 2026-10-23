package kr.nanoit.smq.prototype;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * 큐를 선언할 때 주는 속성. 같은 속성으로 다시 선언하면 아무 일도 없고, 다르면 {@link
 * QueueDeclarationConflictException}.
 *
 * <p>최대 전달 횟수는 기본 무제한이고, 한도를 주려면 dead letter 큐가 반드시 있어야 한다(ADR 0001). 그래서 둘을 한
 * 메서드 {@link #maxDeliveries(int, String)}로만 함께 설정할 수 있게 했다. 한도 없이 dead letter 큐만 두는 것은
 * reject를 위해 허용한다.
 *
 * @param leaseDuration 꺼내기에서 리스 기한을 따로 주지 않을 때 쓰는 기본값.
 */
public record QueueOptions(
        OptionalInt maxDeliveries,
        Optional<String> deadLetterQueue,
        boolean singleActiveConsumer,
        Duration leaseDuration) {

    /**
     * 무제한 전달, dead letter 큐 없음, 단일 활성 컨슈머 아님, 리스 30초.
     *
     * <p>QUESTION: 리스 기본 30초가 맞는지. 리스 기한을 큐 속성(선언에 포함, 다르면 충돌)으로 둘지, 꺼내기 인자로만 둘지.
     * 지금은 큐에 기본값을 두고 꺼내기에서 덮어쓸 수 있게 했다. 큐 속성이면 다른 프로세스가 다른 리스 기한으로 선언할 때
     * 충돌 오류가 난다.
     */
    public static QueueOptions defaults() {
        return new QueueOptions(OptionalInt.empty(), Optional.empty(), false, Duration.ofSeconds(30));
    }

    public QueueOptions maxDeliveries(int limit, String deadLetterQueue) {
        return new QueueOptions(OptionalInt.of(limit), Optional.of(deadLetterQueue), singleActiveConsumer, leaseDuration);
    }

    public QueueOptions deadLetterQueue(String deadLetterQueue) {
        return new QueueOptions(maxDeliveries, Optional.of(deadLetterQueue), singleActiveConsumer, leaseDuration);
    }

    public QueueOptions singleActiveConsumer(boolean singleActiveConsumer) {
        return new QueueOptions(maxDeliveries, deadLetterQueue, singleActiveConsumer, leaseDuration);
    }

    public QueueOptions leaseDuration(Duration leaseDuration) {
        return new QueueOptions(maxDeliveries, deadLetterQueue, singleActiveConsumer, leaseDuration);
    }
}
