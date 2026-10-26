package io.github.ppzxc.smq;

/** dead letter 큐가 없는 큐에서 메시지를 reject했다. 섞인 정산 전체가 반영되지 않는다. */
public class NoDeadLetterQueueException extends SmqException {

    private final String queue;

    public NoDeadLetterQueueException(String queue) {
        super("queue has no dead letter queue: " + queue);
        this.queue = queue;
    }

    public String queue() {
        return queue;
    }
}
