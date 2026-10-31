package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.IncompatibleStorageException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * 열 때의 스키마 판정. 빈 파일이면 만들고, smq 파일이 아니면 거부하고, 더 높은 버전이면 거부하고,
 * 낮은 버전이면 한 단계씩 올린다. 첫 생성 DDL과 {@code application_id}도 여기 있다. 상태가 없다.
 * 목표 버전과 업그레이드 단계 목록은 주입받는다. 운영 코드는 목표 버전 1과 빈 목록을 쓴다.
 */
final class SqliteSchema {

    /** 'SMQ1'. smq 파일임을 알리는 {@code application_id}. */
    static final int APPLICATION_ID = 0x534D5131;

    /** 한 단계 낮은 버전의 스키마를 한 단계 올린다. 호출한 쪽의 쓰기 트랜잭션 안에서 돈다. */
    @FunctionalInterface
    interface UpgradeStep {
        void apply(Connection connection) throws SQLException;
    }

    private final int targetVersion;
    /** 키는 올리기 전의 버전이다. */
    private final Map<Integer, UpgradeStep> upgrades;

    SqliteSchema(int targetVersion, Map<Integer, UpgradeStep> upgrades) {
        this.targetVersion = targetVersion;
        this.upgrades = Map.copyOf(upgrades);
    }

    /**
     * 호출한 쪽의 쓰기 트랜잭션 안에서 판정한다. 생성이나 업그레이드를 했으면 그 뒤의 버전을, 그대로 열면
     * 파일의 버전을 돌려준다. DDL, 데이터 변환, 버전 올리기가 모두 호출한 쪽의 트랜잭션 하나에 들어간다.
     */
    int establish(Connection c) throws SQLException {
        int applicationId = pragmaInt(c, "application_id");
        int version = pragmaInt(c, "user_version");
        boolean empty;
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM sqlite_schema")) {
            empty = rs.getInt(1) == 0;
        }
        if (empty && applicationId == 0) {
            create(c);
            return targetVersion;
        }
        if (applicationId != APPLICATION_ID) {
            throw notSmq("application_id=" + applicationId);
        }
        if (version > targetVersion) {
            throw unsupported(version);
        }
        if (version < targetVersion) {
            upgrade(c, version);
        }
        return targetVersion;
    }

    private void create(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE queue (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT NOT NULL UNIQUE,
                      max_deliveries INTEGER,
                      dead_letter_queue_id INTEGER REFERENCES queue(id),
                      single_active_consumer INTEGER NOT NULL DEFAULT 0
                    )""");
            s.execute("""
                    CREATE TABLE message (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      queue_id INTEGER NOT NULL REFERENCES queue(id),
                      payload BLOB NOT NULL,
                      headers BLOB NOT NULL,
                      delivery_count INTEGER NOT NULL DEFAULT 0,
                      published_at INTEGER NOT NULL,
                      lease_token INTEGER,
                      lease_deadline INTEGER,
                      available_at INTEGER
                    )""");
            s.execute("CREATE INDEX message_ready ON message(queue_id, id)");
            s.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)");
            s.execute("INSERT INTO meta(key, value) VALUES ('lease_seq', 0)");
            s.execute("PRAGMA application_id = " + APPLICATION_ID);
            s.execute("PRAGMA user_version = " + targetVersion);
        }
    }

    /** 한 단계씩 올린다. */
    private void upgrade(Connection c, int from) throws SQLException {
        for (int version = from; version < targetVersion; version++) {
            UpgradeStep step = upgrades.get(version);
            if (step == null) {
                throw unsupported(from);
            }
            step.apply(c);
            try (Statement s = c.createStatement()) {
                s.execute("PRAGMA user_version = " + (version + 1));
            }
        }
    }

    private IncompatibleStorageException unsupported(int found) {
        return versionMismatch("unsupported schema version", found, targetVersion);
    }

    /** 버전 불일치 예외의 유일한 메시지 형식. 열 때의 판정과 트랜잭션마다의 확인이 함께 쓴다. */
    static IncompatibleStorageException versionMismatch(String what, int found, int supported) {
        return new IncompatibleStorageException(what + ": found " + found + ", supported " + supported);
    }

    IncompatibleStorageException notSmq(String reason) {
        return new IncompatibleStorageException("not an smq database (" + reason + "), supported " + targetVersion);
    }

    static int pragmaInt(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            return rs.getInt(1);
        }
    }
}
