package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 큐 연산 단위의 저장소 SPI(내부 계약). 각 연산의 원자성은 저장소가 책임지고, 시각은 저장소가 연산의 lock 안에서 시계로 읽는다. 인스턴스 하나는 스레드 안전하다.
 */
public interface Storage extends AutoCloseable {

    /** 멱등. 이미 있으면 그대로 둔다. */
    void declare(String queue, QueueOptions options);

    boolean exists(String queue);

    /** 큐를 메시지째 지운다. 지웠으면 true, 없었으면 false. */
    boolean delete(String queue);

    /** 선언된 큐의 옵션. 큐가 없으면 비어 있다. */
    Optional<QueueOptions> options(String queue);

    /**
     * 기록된 상태 전환을 따라 센 근사값이다. 시각을 보지 않는다: 리스 토큰이 남은 메시지는 기한이 지났어도 리스 중이고, 꺼낼 시각이
     * 남은 메시지는 그 시각이 지났어도 꺼내기 전까지 지연됨이다(ADR-0018).
     *
     * @throws io.github.ppzxc.smq.QueueNotFoundException 큐가 없을 때
     */
    QueueCounts counts(String queue);

    /** 전부 아니면 전무. */
    List<MessageId> publish(String queue, List<NewMessage> batch, InstantSource clock);

    /** 기다리지 않는다. 준비 메시지가 없으면 빈 결과다. */
    PullResult pull(String queue, int max, Duration lease, InstantSource clock);

    /**
     * {@code wait} 안에서만 저장소의 lock과 쓰기 경합을 기다린다. 그 안에 꺼내지 못하면 아무것도 반영하지 않고 빈
     * 결과를 돌려준다. interrupt에 반응하는 구현은 lock 대기 중 interrupt되면 아무것도 반영하지 않고 {@link InterruptedException}을 던진다.
     * 기본 구현은 한도를 두지 않는 {@link #pull(String, int, Duration, InstantSource)}다.
     */
    default PullResult pull(String queue, int max, Duration lease, InstantSource clock, Duration wait)
            throws InterruptedException {
        return pull(queue, max, lease, clock);
    }

    /** 꺼내기 중 이동한 dead letter의 목적지를 알린다. 목적지를 모르면 빈 값을 알린다. */
    default PullResult pull(
            String queue,
            int max,
            Duration lease,
            InstantSource clock,
            Duration wait,
            Consumer<Optional<String>> deadLetter)
            throws InterruptedException {
        PullResult result = pull(queue, max, lease, clock, wait);
        if (result.deadLettersMoved()) {
            deadLetter.accept(Optional.empty());
        }
        return result;
    }

    /** 코어가 프로세스 내부 변경 알림을 큐별로 보낼 수 있는지 나타낸다. */
    default boolean supportsQueueWakeups() {
        return false;
    }

    /** 빈 꺼내기 뒤 시간 경과로 결과가 달라질 수 있는 시각. 알 수 없으면 빈 값이다. */
    default Optional<Instant> nextReadyAt(String queue, InstantSource clock, Duration wait)
            throws InterruptedException {
        return Optional.empty();
    }

    /** 한 트랜잭션으로 반영하고 메시지별 결과를 돌려준다. */
    List<Result> settle(LeaseToken token, List<Disposition> items, InstantSource clock);

    /** 정산 중 이동한 dead letter의 목적지를 알린다. 목적지를 모르면 빈 값을 알린다. */
    default List<Result> settle(
            LeaseToken token, List<Disposition> items, InstantSource clock, Consumer<Optional<String>> deadLetter) {
        List<Result> results = settle(token, items, clock);
        if (items.stream().anyMatch(item -> item.kind() == Disposition.Kind.REJECT)) {
            deadLetter.accept(Optional.empty());
        }
        return results;
    }

    /**
     * 리스를 "처리하는 시각 + {@code d}"로 바꾼다. 정산과 같은 토큰 일치 기준으로 메시지별 결과를 돌려준다.
     *
     * <p>처리하는 시각은 호출자가 미리 재 두지 않고 저장소가 연산의 lock(트랜잭션)을 잡은 뒤 {@code clock}에서 한 번 읽는다.
     * 호출자가 lock 대기 앞에서 잰 시각을 쓰면, 그 사이 다른 청크가 이 리스의 만료 뒤에 꺼내 간 일을 놓치고 만료된 리스를
     * 되살리기 때문이다. 단일 활성 컨슈머 큐에서는 그 시각에 이미 만료된 리스의 연장을 {@code STALE}로 처리한다. 그 밖의
     * 큐는 만료 뒤라도 아무도 다시 꺼내지 않았다면 연장할 수 있다. 반환하는 {@code leaseDeadline}은 계산한 새 기한이며, 모든
     * 결과가 {@code STALE}이어도 같은 값이다(반영된 것은 {@code OK}인 메시지뿐이다).
     */
    Extended extend(LeaseToken token, List<MessageId> ids, Duration d, InstantSource clock);

    /**
     * 저장소의 일관된 사본을 {@code target}에 만든다. 대상이 있으면 덮어쓰지 않고 실패한다. 지원하지 않는 저장소는
     * {@link UnsupportedOperationException}이다(계약은 {@code Smq.snapshot}).
     */
    void snapshot(Path target);

    /** 변경이 있을 때 대기자를 깨우는 선택적 힌트. 없으면 코어가 폴링 간격을 늘려 가며 폴링한다. */
    default Optional<ChangeHint> changeHint() {
        return Optional.empty();
    }

    @Override
    void close();
}
