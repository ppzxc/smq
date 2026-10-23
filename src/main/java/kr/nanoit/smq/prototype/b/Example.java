package kr.nanoit.smq.prototype.b;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import kr.nanoit.smq.prototype.Message;
import kr.nanoit.smq.prototype.Outcome;
import kr.nanoit.smq.prototype.Outcomes;
import kr.nanoit.smq.prototype.OutgoingMessage;
import kr.nanoit.smq.prototype.QueueCounts;
import kr.nanoit.smq.prototype.QueueOptions;
import kr.nanoit.smq.prototype.Smq;
import kr.nanoit.smq.prototype.SmqStorageException;

/** 변형 B로 쓴 같은 애플리케이션 흐름. 변형 A의 {@code Example}과 나란히 본다. */
final class Example {

    void run() throws InterruptedException {
        try (Smq<Queue> smq = Smq.sqlite(Path.of("/var/lib/app/smq.db"))
                .onClose(Smq.CloseMode.NACK_UNSETTLED)
                .open()) {

            smq.declare("orders.dead", QueueOptions.defaults());
            Queue orders = smq.declare("orders", QueueOptions.defaults()
                    .maxDeliveries(5, "orders.dead")
                    .leaseDuration(Duration.ofMinutes(1)));

            orders.publish(List.of(
                    OutgoingMessage.of("o-1".getBytes()),
                    OutgoingMessage.of("o-2".getBytes()).withHeaders(Map.of("tenant", "acme")),
                    OutgoingMessage.of("o-3".getBytes()).withDelay(Duration.ofMinutes(10))));

            QueueCounts counts = orders.counts();

            while (!Thread.currentThread().isInterrupted()) {
                Chunk chunk = orders.pull(100, Duration.ofSeconds(5));
                if (chunk.isEmpty()) {
                    continue;
                }
                handle(orders, chunk);
            }
        }
    }

    private void handle(Queue orders, Chunk chunk) {
        Queue.Extension extended = orders.extend(chunk, Duration.ofMinutes(5));

        Settlement.Builder settle = chunk.settle();
        for (Message m : chunk.messages()) {
            switch (classify(m)) {
                case RETRY -> settle.nack(m.id(), Duration.ofSeconds(30));
                case POISON -> settle.reject(m.id());
                case DONE -> { }
            }
        }
        Settlement settlement = settle.ackRest();

        Outcomes outcomes;
        try {
            outcomes = orders.settle(settlement);
        } catch (SmqStorageException e) {
            // 효력 불명. 같은 값을 한 번 더 반영한다. 이미 반영됐다면 STALE로 돌아온다.
            outcomes = orders.settle(settlement);
        }
        outcomes.byMessage().forEach((id, outcome) -> {
            if (outcome == Outcome.STALE) {
                // 다른 리스로 넘어갔다.
            }
        });
    }

    /** 가장 흔한 경우: 청크 전체 ACK. */
    private void handleAllOk(Queue orders, Chunk chunk) {
        orders.settle(chunk.ackAll());
    }

    private enum Kind { DONE, RETRY, POISON }

    private Kind classify(Message m) {
        return Kind.DONE;
    }
}
