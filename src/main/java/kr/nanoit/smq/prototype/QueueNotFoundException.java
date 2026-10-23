package kr.nanoit.smq.prototype;

/** 선언되지 않았거나 삭제된 큐에 연산했다. 다른 프로세스가 큐를 삭제한 경우에도 난다. */
public final class QueueNotFoundException extends SmqException {

    public QueueNotFoundException(String queue) {
        super("queue not found: " + queue, null);
    }
}
