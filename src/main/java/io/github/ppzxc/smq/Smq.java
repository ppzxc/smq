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

    /**
     * 큐를 선언하고 돌려준다. 같은 옵션으로 다시 선언하면 그대로 돌려주고, 다른 옵션이면
     * {@link QueueDeclarationConflictException}을 던진다. 지정한 dead letter 큐는 이미 있어야 한다.
     */
    Queue declare(String name, QueueOptions options);

    /** 선언된 큐를 돌려준다. 없으면 {@link QueueNotFoundException}. */
    Queue queue(String name);

    /** 큐를 메시지째 지운다. 지웠으면 true, 없었으면 false. */
    boolean delete(String name);

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
