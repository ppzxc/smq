package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.ChangeHint;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * 꺼내기 대기 루프. 저장소 {@code pull}은 기다리지 않으므로 코어가 여기서 기다린다.
 * 결과가 비면 변경 힌트를 기다리되 1ms에서 250ms까지 늘어나는 상한을 둔다(힌트는 거짓 깨움과 누락을 허용하고,
 * 지연된 메시지가 준비로 바뀌는 시점은 폴링이 잡는다). {@code wait}와 백오프는 주입한 시계가 아니라 단조 시계로 잰다.
 */
final class PullWaiter {

    private static final long MIN_BACKOFF_NANOS = Duration.ofMillis(1).toNanos();
    private static final long MAX_BACKOFF_NANOS = Duration.ofMillis(250).toNanos();

    private final ChangeHint hint;
    private final AtomicBoolean closed = new AtomicBoolean();

    PullWaiter(ChangeHint hint) {
        this.hint = hint;
    }

    /** 같은 프로세스 안의 변경을 알려 기다리던 꺼내기를 깨운다. */
    void wake() {
        hint.wake();
    }

    /** 닫히면 대기자는 빈 결과로 깨어난다. */
    void close() {
        closed.set(true);
        hint.wake();
    }

    /**
     * @param attempt 한 번 꺼내 본다. 비었으면 null
     * @param empty 시간이 다했거나 닫혔을 때의 빈 결과
     */
    <T> T await(Duration wait, Supplier<T> attempt, Supplier<T> empty) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        long backoff = MIN_BACKOFF_NANOS;
        while (true) {
            if (closed.get()) {
                return empty.get();
            }
            // 꺼내기 전에 버전을 읽어 두면, 꺼내고 기다리기 사이의 변경을 놓치지 않는다.
            long seen = hint.version();
            T result = attempt.get();
            if (result != null) {
                return result;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return empty.get();
            }
            hint.await(seen, Duration.ofNanos(Math.min(backoff, remaining)));
            backoff = Math.min(backoff * 2, MAX_BACKOFF_NANOS);
        }
    }
}
