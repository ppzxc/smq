package io.github.ppzxc.smq;

public class QueueInUseException extends SmqException {

    private final String queue;
    private final String referencedBy;

    public QueueInUseException(final String queue, final String referencedBy) {
        super("queue is in use as the dead letter queue of " + referencedBy + ": " + queue);
        this.queue = queue;
        this.referencedBy = referencedBy;
    }

    public String queue() {
        return queue;
    }

    public String referencedBy() {
        return referencedBy;
    }
}
