package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import java.nio.file.Path;

/** 다른 패키지의 테스트가 busy timeout과 시도 횟수를 짧게 정해 경합을 재현하도록 여는 통로. */
public final class SqliteStorageTestSupport {

    private SqliteStorageTestSupport() {}

    public static SqliteStorage open(Path path, int busyTimeoutMillis, int maxAttempts) {
        return SqliteStorage.open(path, Durability.PROCESS_CRASH, busyTimeoutMillis, maxAttempts);
    }
}
