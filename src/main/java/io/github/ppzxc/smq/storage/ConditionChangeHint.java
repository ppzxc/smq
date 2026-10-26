package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** 락과 {@link Condition}으로 구현한 변경 힌트. */
public final class ConditionChangeHint implements ChangeHint {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private long version;

    @Override
    public long version() {
        lock.lock();
        try {
            return version;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void await(long seen, Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lockInterruptibly();
        try {
            while (version == seen && nanos > 0) {
                nanos = changed.awaitNanos(nanos);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void wake() {
        lock.lock();
        try {
            version++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
