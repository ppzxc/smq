package io.github.ppzxc.smq;

/** 다른 큐가 dead letter 큐로 참조 중인 큐를 지우려 했다. */
public class QueueInUseException extends SmqException {

    private final String queue;
    private final String referencedBy;

    public QueueInUseException(String queue, String referencedBy) {
        super("queue is in use as the dead letter queue of " + referencedBy + ": " + queue);
        this.queue = queue;
        this.referencedBy = referencedBy;
    }

    public String queue() {
        return queue;
    }

    /** 이 큐를 dead letter 큐로 지정한 큐 가운데 하나. */
    public String referencedBy() {
        return referencedBy;
    }
}
