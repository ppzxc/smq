package io.github.ppzxc.smq;

public class QueueDeclarationConflictException extends SmqException {

    private final String queue;
    private final QueueOptions existing;
    private final QueueOptions requested;

    public QueueDeclarationConflictException(
            final String queue, final QueueOptions existing, final QueueOptions requested) {
        super("queue already declared with different options: " + queue + " (existing=" + existing + ", requested="
                + requested + ")");
        this.queue = queue;
        this.existing = existing;
        this.requested = requested;
    }

    public String queue() {
        return queue;
    }

    public QueueOptions existing() {
        return existing;
    }

    public QueueOptions requested() {
        return requested;
    }
}
