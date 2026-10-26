package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 큐 연산 단위의 저장소 SPI(내부 계약). 각 연산의 원자성은 저장소가 책임지고, 시각은 코어가 {@code now}로 넘긴다.
 * 인스턴스 하나는 스레드 안전하다.
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

    /** 한 트랜잭션으로 반영하고 메시지별 결과를 돌려준다. */
    List<Result> settle(LeaseToken token, List<Disposition> items, Instant now);

    /** 리스를 {@code now + d}로 바꾼다. 정산과 같은 토큰 일치 기준으로 메시지별 결과를 돌려준다. */
    Extended extend(LeaseToken token, List<MessageId> ids, Duration d, Instant now);

    @Override
    void close();
}
