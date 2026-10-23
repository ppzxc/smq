package kr.nanoit.smq.prototype;

/**
 * dead letter 큐가 없는 큐에서 reject했다. 섞인 정산이면 정산 전체가 반영되지 않는다.
 *
 * <p>QUESTION: 큐 핸들이 아는 선언 속성으로 저장소에 가기 전에 코어에서 던질지, 저장소에서 확인할지. 다른 프로세스가 큐를
 * 지우고 다른 속성으로 다시 선언할 수 있으니 저장소가 최종 판정해야 한다. 코어 검사는 빠른 실패용일 뿐이다.
 */
public final class NoDeadLetterQueueException extends SmqException {

    public NoDeadLetterQueueException(String queue) {
        super("queue " + queue + " has no dead letter queue; reject is not allowed", null);
    }
}
