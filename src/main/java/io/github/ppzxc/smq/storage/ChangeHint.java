package io.github.ppzxc.smq.storage;

import java.time.Duration;

public interface ChangeHint {

    long version();

    void await(long seen, Duration timeout) throws InterruptedException;

    void wake();
}
