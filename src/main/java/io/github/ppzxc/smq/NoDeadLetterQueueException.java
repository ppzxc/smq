package io.github.ppzxc.smq;

public class NoDeadLetterQueueException extends SmqException {

    private final String queue;

    public NoDeadLetterQueueException(final String queue) {
        super("queue has no dead letter queue: " + queue);
        this.queue = queue;
    }

    public String queue() {
        return queue;
    }
}
