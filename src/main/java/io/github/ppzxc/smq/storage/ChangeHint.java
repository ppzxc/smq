package io.github.ppzxc.smq.storage;

import java.time.Duration;

/**
 * 저장소가 내는 선택적 변경 힌트. 꺼내기 대기자가 바쁜 폴링 없이 기다리는 데 쓴다.
 * 거짓 깨움과 누락을 모두 허용하므로, 깨어난 쪽은 반드시 다시 꺼내 보고 기다리는 쪽은 기다림에 상한을 둔다.
 */
public interface ChangeHint {

    /** 변경이 있을 때마다 늘어나는 버전. 기다리기 전에 읽어 두면 그 사이의 변경을 놓치지 않는다. */
    long version();

    /** 버전이 {@code seen}에서 바뀌었거나 {@link #wake()}가 불렸거나 {@code timeout}이 지나면 돌아온다. */
    void await(long seen, Duration timeout) throws InterruptedException;

    /** 기다리는 쪽을 모두 깨운다. */
    void wake();
}
