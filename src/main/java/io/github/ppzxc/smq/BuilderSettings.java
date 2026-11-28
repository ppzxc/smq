package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

final class BuilderSettings {

    private InstantSource clock = InstantSource.system();
    private Duration leaseDuration = Duration.ofSeconds(30);

    void clock(final InstantSource clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    void leaseDuration(final Duration leaseDuration) {
        this.leaseDuration = Validation.positive(leaseDuration, "leaseDuration");
    }

    Smq open(final Storage storage) {
        return new DefaultSmq(storage, clock, leaseDuration);
    }
}
