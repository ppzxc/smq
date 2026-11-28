package io.github.ppzxc.smq.storage;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    public void await(final long seen, final Duration timeout) throws InterruptedException {
        long nanos;
        nanos = timeout.toNanos();
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
