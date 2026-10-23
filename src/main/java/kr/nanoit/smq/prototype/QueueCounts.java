package kr.nanoit.smq.prototype;

/**
 * 큐에 있는 메시지를 상태별로 센 값. 한 시점의 스냅숏이다.
 *
 * <p>리스가 만료된 메시지는 다시 꺼낼 수 있으므로 {@code ready}로 센다. 지연 시각이 지난 메시지도 {@code ready}다.
 */
public record QueueCounts(long ready, long leased, long delayed) {

    public long total() {
        return ready + leased + delayed;
    }
}
