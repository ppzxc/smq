package kr.nanoit.smq.prototype;

/** 이미 있는 큐를 다른 속성으로 선언했다. 저장소에 있는 속성과 요청한 속성을 함께 담는다. */
public final class QueueDeclarationConflictException extends SmqException {

    private final QueueOptions existing;
    private final QueueOptions requested;

    public QueueDeclarationConflictException(String queue, QueueOptions existing, QueueOptions requested) {
        super("queue " + queue + " already declared with " + existing + ", requested " + requested, null);
        this.existing = existing;
        this.requested = requested;
    }

    public QueueOptions existing() {
        return existing;
    }

    public QueueOptions requested() {
        return requested;
    }
}
