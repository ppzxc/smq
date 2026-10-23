package kr.nanoit.smq.prototype;

import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;

/**
 * smq의 진입점. 저장소 하나를 열어 그 위의 큐를 다룬다. 스레드 안전하며 프로세스당 저장소 하나에 하나를 두고 공유한다.
 *
 * <p>티켓의 "브로커"는 용어집에 없는 말이라 타입으로 만들지 않았다. 진입 타입이 곧 {@code Smq}다.
 *
 * <p>QUESTION: 변형 A와 B가 {@code Queue} 타입만 다르다. 실제 API에서는 이 인터페이스가 변형의 {@code Queue}를
 * 돌려준다. 프로토타입에서는 제네릭으로 둘을 함께 표현했다.
 */
public interface Smq<Q> extends AutoCloseable {

    /** SQLite 파일을 저장소로 쓴다. 같은 호스트의 여러 프로세스가 같은 파일을 열 수 있다. */
    static Builder sqlite(Path file) {
        throw new UnsupportedOperationException("prototype");
    }

    /** 프로세스 메모리를 저장소로 쓴다. 닫으면 전부 사라진다. 테스트와 단일 프로세스용. */
    static Builder inMemory() {
        throw new UnsupportedOperationException("prototype");
    }

    /**
     * 큐를 주어진 속성으로 존재하게 하고 핸들을 돌려준다. dead letter 큐를 가리키면 그 큐가 먼저 선언돼 있어야 한다.
     *
     * @throws QueueDeclarationConflictException 같은 이름의 큐가 다른 속성으로 있다
     * @throws QueueNotFoundException 가리킨 dead letter 큐가 없다
     */
    Q declare(String name, QueueOptions options);

    /**
     * 이미 선언된 큐의 핸들. 선언하지 않는다.
     *
     * <p>QUESTION: 여기서 존재를 확인할지(저장소 조회 한 번), 핸들만 만들고 첫 연산에서 {@link QueueNotFoundException}을
     * 낼지. 지금은 확인한다.
     *
     * @throws QueueNotFoundException 큐가 없다
     */
    Q queue(String name);

    /**
     * 큐와 그 안의 메시지를 지운다. 리스 중인 메시지도 지워지고, 그 리스에 대한 정산은 전부 STALE이 된다.
     *
     * <p>QUESTION: 다른 큐의 dead letter 큐로 지정된 큐를 지우는 것을 막을지.
     *
     * @return 큐가 있어서 지웠으면 true
     */
    boolean delete(String name);

    /**
     * 저장소를 닫는다. 꺼내기에서 기다리던 스레드는 깨어나 빈 청크를 받는다.
     *
     * <p>아직 정산하지 않은 리스를 어떻게 할지는 {@link Builder#onClose(CloseMode)}가 정한다.
     */
    @Override
    void close();

    /** 닫을 때 이 {@code Smq}에서 꺼낸 청크 중 아직 정산하지 않은 메시지를 어떻게 할지. */
    enum CloseMode {
        /**
         * 그대로 둔다. 리스 기한이 지나야 다시 꺼낼 수 있다. 프로세스가 죽은 경우와 같은 동작이다.
         *
         * <p>QUESTION: 기본값을 이쪽으로 할지. 장점은 smq가 살아 있는 리스를 추적하지 않아도 된다는 것.
         */
        LEAVE_LEASED,
        /**
         * 지연 없이 NACK한다. 메시지가 바로 맨 앞으로 가서 다른 컨슈머가 기다리지 않는다. 전달 횟수는 이미 늘었으므로
         * 되돌리지 않는다.
         *
         * <p>이렇게 하려면 smq가 꺼낸 청크를 전부 기억해야 한다(변형 A든 B든 내부에서 약한 참조로 추적).
         */
        NACK_UNSETTLED
    }

    interface Builder {

        /** 시각의 출처. 기본은 {@link InstantSource#system()}. 지연과 리스 기한을 테스트에서 돌리기 위해 쓴다. */
        Builder clock(InstantSource clock);

        /**
         * 꺼내기에서 메시지를 기다릴 때의 폴링 간격. 프로세스 간 알림이 없으므로 폴링한다. 기본 1ms에서 250ms까지 늘린다.
         *
         * <p>QUESTION: v1에서 이것을 공개할지, 내부 상수로 둘지.
         */
        Builder pollInterval(Duration min, Duration max);

        /** 기본값은 QUESTION. {@link CloseMode} 참고. */
        Builder onClose(CloseMode mode);

        <Q> Smq<Q> open();
    }
}
