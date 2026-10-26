package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Storage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

/** 저장소와 상관없이 모든 빌더가 공유하는 설정. */
final class BuilderSettings {

    private InstantSource clock = InstantSource.system();
    private Duration leaseDuration = Duration.ofSeconds(30);

    void clock(InstantSource clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    void leaseDuration(Duration leaseDuration) {
        this.leaseDuration = Validation.positive(leaseDuration, "leaseDuration");
    }

    Smq open(Storage storage) {
        return new DefaultSmq(storage, clock, leaseDuration);
    }
}
