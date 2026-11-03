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

    /**
     * 실행 중인 SQLite 저장소의 일관된 사본을 {@code target}에 만든다. 끝날 때까지 기다리고, 외부 SQLite 도구로 열 수 있는
     * 파일을 남긴다.
     *
     * <ul>
     *   <li>SQLite 전용이다. 인메모리 저장소는 {@link UnsupportedOperationException}을 던진다.
     *   <li>{@code VACUUM INTO}가 도는 동안 저장소의 lock을 쥐므로 {@code publish}, {@code pull}, {@code settle},
     *       {@code extend} 같은 다른 연산이 DB 크기에 비례해 멈춘다.
     *   <li>같은 디렉터리에 임시 파일을 만들었다가 하드 링크로 연결하므로, {@code target}의 디렉터리에 DB 크기만큼의 여유
     *       공간이 있어야 한다. 사전 검사는 하지 않는다.
     *   <li>{@code target}의 파일시스템이 하드 링크를 지원해야 한다. 지원하지 않으면 예외로 실패하고 대체 경로는 없다.
     *   <li>{@code target}이 이미 있으면 덮어쓰지 않고 예외를 던진다. 부모 디렉터리가 없어도 예외이며 만들지 않는다.
     *   <li>실패하면 임시 파일을 지우고 {@code target}을 만들지 않는다.
     * </ul>
     *
     * @throws IllegalArgumentException {@code target}이 실행 중인 DB, 그 {@code -wal} 파일, 락 파일과 같은 파일일 때
     * @throws SmqStorageException 대상이 이미 있거나 부모 디렉터리가 없거나, 복사나 연결에 실패했을 때
     * @throws UnsupportedOperationException 저장소가 스냅샷을 지원하지 않을 때
     */
    void snapshot(Path target);

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

    /**
     * SQLite 저장소용 빌더. 내구성 수준은 여기에만 있다. SQLite 파일은 한 JVM만 열 수 있다. 같은 파일을 이미 다른 곳이 쥐고
     * 있으면 {@link #open()}이 기다리지 않고 {@link StorageLockedException}을 던진다.
     */
    final class SqliteBuilder {

        private final BuilderSettings settings = new BuilderSettings();
        private final Path path;
        private Durability durability = Durability.POWER_LOSS;
        private Duration lockWaitFloor = Duration.ofMillis(100);

        SqliteBuilder(Path path) {
            this.path = path;
        }

        /** 내구성 수준. 기본은 {@link Durability#POWER_LOSS}. */
        public SqliteBuilder durability(Durability durability) {
            this.durability = Objects.requireNonNull(durability, "durability");
            return this;
        }

        /**
         * 꺼내기가 {@code wait}와 상관없이 저장소의 lock을 최소한 이만큼은 기다리는 시간. 기본은 100ms다.
         * 같은 프로세스의 다른 호출이 쓰기 트랜잭션 하나를 끝내는 시간보다 짧으면, 경합 중에 {@code wait}가 짧은 꺼내기가
         * 메시지가 있는데도 빈 청크로 돌아온다. 쓰기 트랜잭션 하나가 100ms를 넘는 느린 저장 장치에서 늘린다. 0 이상이다.
         */
        public SqliteBuilder lockWaitFloor(Duration lockWaitFloor) {
            Objects.requireNonNull(lockWaitFloor, "lockWaitFloor");
            if (lockWaitFloor.isNegative()) {
                throw new IllegalArgumentException("lockWaitFloor must not be negative: " + lockWaitFloor);
            }
            this.lockWaitFloor = lockWaitFloor;
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

        /**
         * 저장소를 연다.
         *
         * @throws StorageLockedException 파일을 다른 프로세스, 같은 JVM의 다른 인스턴스, 외부 SQLite 도구가 쥐고 있을 때
         */
        public Smq open() {
            return settings.open(SqliteStorage.open(path, durability, lockWaitFloor));
        }
    }
}
