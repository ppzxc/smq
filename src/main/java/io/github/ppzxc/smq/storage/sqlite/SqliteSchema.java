package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.IncompatibleStorageException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

final class SqliteSchema {

    static final int APPLICATION_ID = 0x534D5131;

    @FunctionalInterface
    interface UpgradeStep {
        void apply(Connection connection) throws SQLException;
    }

    private final int targetVersion;

    private final Map<Integer, UpgradeStep> upgrades;

    SqliteSchema(int targetVersion, Map<Integer, UpgradeStep> upgrades) {
        this.targetVersion = targetVersion;
        this.upgrades = Map.copyOf(upgrades);
    }

    int establish(Connection c) throws SQLException {
        int applicationId = pragmaInt(c, "application_id");
        int version = pragmaInt(c, "user_version");
        boolean empty = schemaIsEmpty(c);
        if (empty && applicationId == 0) {
            createV1(c);
            upgrade(c, 1);
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

    private static boolean schemaIsEmpty(Connection c) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM sqlite_schema")) {
            if (!rs.next()) {
                throw new SQLException("schema count row missing");
            }
            return rs.getInt(1) == 0;
        }
    }

    static void configureNewDatabase(Connection c, boolean autoVacuum) throws SQLException {
        if (pragmaInt(c, "application_id") != 0) {
            return;
        }
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM sqlite_schema")) {
            if (!rs.next()) {
                throw new SQLException("schema count row missing");
            }
            if (rs.getInt(1) != 0) {
                return;
            }
        }
        try (Statement s = c.createStatement()) {
            s.execute("PRAGMA auto_vacuum = " + (autoVacuum ? "FULL" : "NONE"));
        }
    }

    private static void createV1(Connection c) throws SQLException {
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
            s.execute("PRAGMA user_version = 1");
        }
    }

    static void upgradeV1ToV2(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("""
                    CREATE INDEX message_stored_ready ON message(queue_id, id)
                      WHERE lease_deadline IS NULL AND available_at IS NULL""");
            s.execute("""
                    CREATE INDEX message_lease_deadline ON message(queue_id, lease_deadline)
                      WHERE lease_deadline IS NOT NULL""");
            s.execute("""
                    CREATE INDEX message_available_at ON message(queue_id, available_at)
                      WHERE available_at IS NOT NULL""");
            s.execute("""
                    CREATE INDEX message_redelivered_ready ON message(queue_id, delivery_count, id)
                      WHERE lease_deadline IS NULL AND available_at IS NULL AND delivery_count > 0""");
            s.execute("""
                    UPDATE message SET available_at = NULL
                      WHERE lease_token IS NOT NULL AND available_at IS NOT NULL""");
        }
    }

    static void ensureDelayedIdIndex(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("""
                    CREATE INDEX IF NOT EXISTS message_delayed_id ON message(queue_id, id)
                      WHERE available_at IS NOT NULL""");
            s.execute("""
                    CREATE INDEX IF NOT EXISTS message_delayed_exhausted
                      ON message(queue_id, delivery_count, available_at)
                      WHERE available_at IS NOT NULL AND delivery_count > 0""");
        }
    }

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

    static IncompatibleStorageException versionMismatch(String what, int found, int supported) {
        return new IncompatibleStorageException(what + ": found " + found + ", supported " + supported);
    }

    IncompatibleStorageException notSmq(String reason) {
        return new IncompatibleStorageException("not an smq database (" + reason + "), supported " + targetVersion);
    }

    static int pragmaInt(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA " + name)) {
            if (!rs.next()) {
                throw new SQLException("pragma row missing: " + name);
            }
            return rs.getInt(1);
        }
    }
}
