package kr.nanoit.smq.prototype.a;

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

/** 변형 A로 쓴 애플리케이션 흐름. 반응할 대상은 이 파일이다. */
final class Example {

    void run() throws InterruptedException {
        // ---- 열기
        try (Smq<Queue> smq = Smq.sqlite(Path.of("/var/lib/app/smq.db"))
                .onClose(Smq.CloseMode.NACK_UNSETTLED)
                .open()) {

            // ---- 선언: dead letter 큐 먼저
            smq.declare("orders.dead", QueueOptions.defaults());
            Queue orders = smq.declare("orders", QueueOptions.defaults()
                    .maxDeliveries(5, "orders.dead")
                    .leaseDuration(Duration.ofMinutes(1)));

            // ---- 발행 배치: 하나는 지연
            orders.publish(List.of(
                    OutgoingMessage.of("o-1".getBytes()),
                    OutgoingMessage.of("o-2".getBytes()).withHeaders(Map.of("tenant", "acme")),
                    OutgoingMessage.of("o-3".getBytes()).withDelay(Duration.ofMinutes(10))));

            QueueCounts counts = orders.counts(); // ready=2, leased=0, delayed=1

            // ---- 꺼내기 루프
            while (!Thread.currentThread().isInterrupted()) {
                Chunk chunk = orders.pull(100, Duration.ofSeconds(5));
                if (chunk.isEmpty()) {
                    continue;
                }
                try {
                    handle(chunk);
                } catch (SmqStorageException e) {
                    // 효력 불명. 정산하지 못한 메시지는 리스 만료 뒤 다시 온다. 여기서는 로그만 남긴다.
                }
            }
        }
    }

    private void handle(Chunk chunk) {
        // 오래 걸리는 처리 전에 연장
        Outcomes extended = chunk.extend(Duration.ofMinutes(5));
        if (!extended.allOk()) {
            // STALE인 메시지는 이미 다른 컨슈머에게 넘어갔다. 처리하지 않는다.
        }

        // 섞인 정산: 일부는 개별 판단, 나머지는 ACK. 트랜잭션 하나.
        Chunk.SettleBuilder settle = chunk.settle();
        for (Message m : chunk.messages()) {
            switch (classify(m)) {
                case RETRY -> settle.nack(m.id(), Duration.ofSeconds(30));
                case POISON -> settle.reject(m.id());
                case DONE -> { /* 나머지로 ACK */ }
            }
        }
        Outcomes outcomes = settle.ackRest();
        outcomes.byMessage().forEach((id, outcome) -> {
            if (outcome == Outcome.STALE) {
                // 리스가 만료돼 다른 리스로 넘어갔다. 처리 결과는 버려지고 그쪽에서 다시 처리된다.
            }
        });
    }

    /** 개별 정산 방식: 메시지마다 트랜잭션 하나. */
    private void handleOneByOne(Chunk chunk) {
        for (Message m : chunk.messages()) {
            if (classify(m) == Kind.RETRY) {
                chunk.nack(m.id(), Duration.ofSeconds(30));
            }
        }
        chunk.ackRest(); // 앞에서 NACK한 메시지는 핸들이 기억하므로 빠진다
    }

    private enum Kind { DONE, RETRY, POISON }

    private Kind classify(Message m) {
        return Kind.DONE;
    }
}
