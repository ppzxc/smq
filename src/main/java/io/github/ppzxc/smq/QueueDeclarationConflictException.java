package io.github.ppzxc.smq;

/** 이미 있는 큐를 다른 옵션으로 선언했다. */
public class QueueDeclarationConflictException extends SmqException {

    private final String queue;
    private final QueueOptions existing;
    private final QueueOptions requested;

    public QueueDeclarationConflictException(String queue, QueueOptions existing, QueueOptions requested) {
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
