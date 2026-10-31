package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;

/**
 * 큐 연산 단위의 저장소 SPI(내부 계약). 각 연산의 원자성은 저장소가 책임지고, 시각은 코어가 {@code now}로 넘긴다. 예외는
 * {@link #extend}로, 저장소가 lock 안에서 시각을 읽는다. 인스턴스 하나는 스레드 안전하다.
 */
public interface Storage extends AutoCloseable {

    /** 멱등. 이미 있으면 그대로 둔다. */
    void declare(String queue, QueueOptions options);

    boolean exists(String queue);

    /** 큐를 메시지째 지운다. 지웠으면 true, 없었으면 false. */
    boolean delete(String queue);

    /** 선언된 큐의 옵션. 큐가 없으면 비어 있다. */
    Optional<QueueOptions> options(String queue);

    QueueCounts counts(String queue, Instant now);

    /** 전부 아니면 전무. */
    List<MessageId> publish(String queue, List<NewMessage> batch, Instant now);

    /** 기다리지 않는다. 준비 메시지가 없으면 빈 결과다. */
    PullResult pull(String queue, int max, Duration lease, Instant now);

    /**
     * {@code wait} 안에서만 저장소의 lock과 쓰기 경합을 기다린다. 그 안에 꺼내지 못하면 아무것도 반영하지 않고 빈
     * 결과를 돌려준다. 기본 구현은 한도를 두지 않는 {@link #pull(String, int, Duration, Instant)}다.
     */
    default PullResult pull(String queue, int max, Duration lease, Instant now, Duration wait) {
        return pull(queue, max, lease, now);
    }

    /** 한 트랜잭션으로 반영하고 메시지별 결과를 돌려준다. */
    List<Result> settle(LeaseToken token, List<Disposition> items, Instant now);

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

    /** 변경이 있을 때 대기자를 깨우는 선택적 힌트. 없으면 코어가 폴링 간격을 늘려 가며 폴링한다. */
    default Optional<ChangeHint> changeHint() {
        return Optional.empty();
    }

    @Override
    void close();
}
