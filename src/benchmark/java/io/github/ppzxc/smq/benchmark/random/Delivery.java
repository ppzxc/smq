package io.github.ppzxc.smq.benchmark.random;

/**
 * 컨슈머의 전달 기록. 꺼낸 메시지마다 하나.
 *
 * @param queue 꺼낸 큐
 * @param tag 헤더에서 읽은 태그. dead letter 큐에서도 원래 태그다
 * @param messageId 큐 안의 메시지 id
 * @param content 전달받은 payload 체크섬과 헤더 전체(dead letter 이동 시 붙는 {@code smq-} 헤더 포함)
 */
record Delivery(QueueKind queue, Tag tag, long messageId, int deliveryCount, Content content, Pull pull,
        Settle settle) {

    /**
     * 꺼내기 한 번의 정보. 같은 청크의 전달은 같은 {@code chunk}를 가진다.
     *
     * @param chunk 모든 프로세스에서 유일한 청크 번호
     * @param position 청크 안에서의 위치(0부터)
     * @param atMillis 꺼낸 시각(epoch ms)
     * @param leaseDeadlineMillis 연장을 모두 반영한 최종 리스 기한(epoch ms)
     */
    record Pull(long chunk, int position, long atMillis, long leaseDeadlineMillis) {
    }

    enum Action {
        ACK,
        NACK,
        REJECT,
        /** 정산을 시도하지 않았다. */
        NONE
    }

    /**
     * 정산 결과.
     *
     * @param applied 정산이 반영됐다(OK). 낡은 연산(STALE)이면 false. {@code NONE}이면 false
     * @param atMillis 정산 호출 직전의 벽시계 시각(epoch ms). {@code NONE}이면 0
     * @param nackDelayMillis NACK 지연. NACK이 아니면 0
     */
    record Settle(Action action, boolean applied, long atMillis, long nackDelayMillis) {

        static final Settle NONE = new Settle(Action.NONE, false, 0, 0);

        static Settle ack(long atMillis) {
            return new Settle(Action.ACK, true, atMillis, 0);
        }

        static Settle nack(long atMillis, long delayMillis) {
            return new Settle(Action.NACK, true, atMillis, delayMillis);
        }

        static Settle reject(long atMillis) {
            return new Settle(Action.REJECT, true, atMillis, 0);
        }

        boolean applied(Action a) {
            return applied && action == a;
        }
    }
}
