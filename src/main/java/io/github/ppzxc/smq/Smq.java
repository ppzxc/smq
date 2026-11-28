package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

public interface Smq extends AutoCloseable {

    static SqliteBuilder sqlite(final Path path) {
        Objects.requireNonNull(path, "path");
        return new SqliteBuilder(path);
    }

    static Builder inMemory() {
        return new Builder();
    }

    Queue declare(String name, QueueOptions options);

    Queue queue(String name);

    boolean delete(String name);

    void snapshot(Path target);

    @Override
    void close();

    final class Builder {

        private final BuilderSettings settings = new BuilderSettings();

        Builder() {}

        public Builder clock(final InstantSource clock) {
            settings.clock(clock);
            return this;
        }

        public Builder leaseDuration(final Duration leaseDuration) {
            settings.leaseDuration(leaseDuration);
            return this;
        }

        public Smq open() {
            return settings.open(new MemoryStorage());
        }
    }

    final class SqliteBuilder {

        private final BuilderSettings settings = new BuilderSettings();
        private final Path path;
        private Durability durability = Durability.PROCESS_CRASH;
        private boolean autoVacuum = true;
        private Duration lockWaitFloor = Duration.ofMillis(100);

        SqliteBuilder(final Path path) {
            this.path = path;
        }

        public SqliteBuilder durability(final Durability durability) {
            this.durability = Objects.requireNonNull(durability, "durability");
            return this;
        }

        public SqliteBuilder autoVacuum(final boolean enabled) {
            this.autoVacuum = enabled;
            return this;
        }

        Durability durability() {
            return durability;
        }

        SqliteBuilder lockWaitFloor(final Duration lockWaitFloor) {
            Objects.requireNonNull(lockWaitFloor, "lockWaitFloor");
            if (lockWaitFloor.isNegative()) {
                throw new IllegalArgumentException("lockWaitFloor must not be negative: " + lockWaitFloor);
            }
            this.lockWaitFloor = lockWaitFloor;
            return this;
        }

        public SqliteBuilder clock(final InstantSource clock) {
            settings.clock(clock);
            return this;
        }

        public SqliteBuilder leaseDuration(final Duration leaseDuration) {
            settings.leaseDuration(leaseDuration);
            return this;
        }

        public Smq open() {
            return settings.open(SqliteStorage.open(path, durability, lockWaitFloor, autoVacuum));
        }
    }
}
