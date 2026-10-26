package io.github.ppzxc.smq;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;

final class MutableClock implements InstantSource {

    private volatile Instant now;

    MutableClock(Instant now) {
        this.now = now;
    }

    @Override
    public Instant instant() {
        return now;
    }

    void advance(Duration d) {
        now = now.plus(d);
    }
}
