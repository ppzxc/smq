package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;

/** smq 진입점. 스레드 안전하고, 닫을 때 정산하지 않은 리스는 그대로 둔다. */
public interface Smq extends AutoCloseable {

    static SqliteBuilder sqlite(Path path) {
        Objects.requireNonNull(path, "path");
        return new SqliteBuilder(path);
    }

    static Builder inMemory() {
        return new Builder();
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

    /** 인메모리 저장소용 빌더. */
    final class Builder {

        private final BuilderSettings settings = new BuilderSettings();

        Builder() {}

        public Builder clock(InstantSource clock) {
            settings.clock(clock);
            return this;
        }

        public Builder leaseDuration(Duration leaseDuration) {
            settings.leaseDuration(leaseDuration);
            return this;
        }

        public Smq open() {
            return settings.open(new MemoryStorage());
        }
    }

    /** SQLite 저장소용 빌더. 내구성 수준은 여기에만 있다. */
    final class SqliteBuilder {

        private final BuilderSettings settings = new BuilderSettings();
        private final Path path;
        private Durability durability = Durability.POWER_LOSS;

        SqliteBuilder(Path path) {
            this.path = path;
        }

        /** 내구성 수준. 기본은 {@link Durability#POWER_LOSS}. */
        public SqliteBuilder durability(Durability durability) {
            this.durability = Objects.requireNonNull(durability, "durability");
            return this;
        }

        public SqliteBuilder clock(InstantSource clock) {
            settings.clock(clock);
            return this;
        }

        public SqliteBuilder leaseDuration(Duration leaseDuration) {
            settings.leaseDuration(leaseDuration);
            return this;
        }

        public Smq open() {
            return settings.open(SqliteStorage.open(path, durability));
        }
    }
}
