package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.Storage;
import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;
import java.util.function.Supplier;

/** smq 진입점. 스레드 안전하고, 닫을 때 정산하지 않은 리스는 그대로 둔다. */
public interface Smq extends AutoCloseable {

    static Builder sqlite(Path path) {
        Objects.requireNonNull(path, "path");
        return new Builder(() -> SqliteStorage.open(path));
    }

    static Builder inMemory() {
        return new Builder(MemoryStorage::new);
    }

    /** 큐를 선언하고(이미 있으면 그대로) 돌려준다. */
    Queue declare(String name, QueueOptions options);

    /** 선언된 큐를 돌려준다. 없으면 {@link QueueNotFoundException}. */
    Queue queue(String name);

    @Override
    void close();

    final class Builder {

        private final Supplier<Storage> storage;
        private InstantSource clock = InstantSource.system();
        private Duration leaseDuration = Duration.ofSeconds(30);

        Builder(Supplier<Storage> storage) {
            this.storage = storage;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder leaseDuration(Duration leaseDuration) {
            this.leaseDuration = Validation.positive(leaseDuration, "leaseDuration");
            return this;
        }

        public Smq open() {
            return new DefaultSmq(storage.get(), clock, leaseDuration);
        }
    }
}
