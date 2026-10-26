package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.ChangeHint;
import io.github.ppzxc.smq.storage.ConditionChangeHint;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * 꺼내기 대기 루프. 저장소 {@code pull}은 기다리지 않으므로 코어가 여기서 기다린다.
 * 결과가 비면 두 갈래로 기다린다.
 * <ul>
 *   <li>저장소가 변경 힌트를 내면 힌트를 기다린다. 힌트는 거짓 깨움과 누락을 허용하고 지연됨에서 준비로 바뀌는
 *       시점은 알리지 못하므로, 기다림에는 폴링 간격 상한({@link #MAX_POLL_INTERVAL_NANOS})을 둔다.</li>
 *   <li>힌트가 없으면 1ms에서 250ms까지 늘어나는 폴링 간격으로 폴링한다. 같은 프로세스 안의 변경은
 *       코어가 만든 지역 신호로 간격을 기다리는 중에도 바로 깨운다.</li>
 * </ul>
 * {@code wait}와 폴링 간격은 주입한 시계가 아니라 단조 시계로 잰다.
 */
final class PullWaiter {

    private static final long MIN_POLL_INTERVAL_NANOS = Duration.ofMillis(1).toNanos();
    private static final long MAX_POLL_INTERVAL_NANOS = Duration.ofMillis(250).toNanos();

    private final ChangeHint hint;
    private final boolean storageHint;
    private final AtomicBoolean closed = new AtomicBoolean();

    PullWaiter(Optional<ChangeHint> storageHint) {
        this(storageHint.orElseGet(ConditionChangeHint::new), storageHint.isPresent());
    }

    PullWaiter(ChangeHint hint, boolean storageHint) {
        this.hint = hint;
        this.storageHint = storageHint;
    }

    /** 같은 프로세스 안의 변경을 알려 기다리던 꺼내기를 깨운다. 저장소와 상관없이 코어가 부른다. */
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
        return await(wait, attempt, empty, () -> false);
    }

    /**
     * @param retryNow 비어 돌아온 직후 true이면 기다리지 않고 바로 다시 꺼낸다. 그 사이에도 닫힘과 interrupt를 확인한다.
     */
    <T> T await(Duration wait, Supplier<T> attempt, Supplier<T> empty, BooleanSupplier retryNow)
            throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        long pollInterval = storageHint ? MAX_POLL_INTERVAL_NANOS : MIN_POLL_INTERVAL_NANOS;
        while (true) {
            // 버전을 먼저 읽고 그다음 닫힘을 본다. 닫힘 확인 뒤에 close()가 끝나도 버전이 이미 올라가 있어
            // 읽은 버전이 낡지 않으므로, 아래 기다림이 그 깨움을 놓치지 않는다. 꺼내기 전에 읽는 것은
            // 꺼내고 기다리기 사이의 변경을 놓치지 않기 위해서이기도 하다.
            long seen = hint.version();
            if (closed.get()) {
                return empty.get();
            }
            T result;
            try {
                result = attempt.get();
            } catch (RuntimeException e) {
                // 닫는 중에 저장소가 먼저 닫혀 꺼내기가 실패한 경우다. 닫힌 뒤의 꺼내기는 빈 결과다.
                if (closed.get()) {
                    return empty.get();
                }
                throw e;
            }
            if (result != null) {
                return result;
            }
            if (retryNow.getAsBoolean()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                continue;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return empty.get();
            }
            hint.await(seen, Duration.ofNanos(Math.min(pollInterval, remaining)));
            if (!storageHint) {
                pollInterval = Math.min(pollInterval * 2, MAX_POLL_INTERVAL_NANOS);
            }
        }
    }
}
