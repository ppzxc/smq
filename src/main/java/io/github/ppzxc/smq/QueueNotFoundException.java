package io.github.ppzxc.smq;

public class QueueNotFoundException extends SmqException {

    private final String queue;

    public QueueNotFoundException(String queue) {
        super("queue not found: " + queue);
        this.queue = queue;
    }

    public String queue() {
        return queue;
    }
}
