package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.QueueCounts;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;

final class ExactCounts {

    private ExactCounts() {}

    static QueueCounts stored(SqliteStorage storage, String queue) {
        return storage.inTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT COALESCE(SUM(lease_token IS NULL AND available_at IS NULL), 0),
                           COALESCE(SUM(lease_token IS NOT NULL), 0),
                           COALESCE(SUM(lease_token IS NULL AND available_at IS NOT NULL), 0)
                    FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?)""")) {
                ps.setString(1, queue);
                try (ResultSet rs = ps.executeQuery()) {
                    return new QueueCounts(rs.getLong(1), rs.getLong(2), rs.getLong(3));
                }
            }
        });
    }

    static QueueCounts at(SqliteStorage storage, String queue, Instant now) {
        long nanos = Math.addExact(Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L), now.getNano());
        return storage.inTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT COALESCE(SUM(NOT delayed AND (lease_token IS NULL OR lease_deadline <= ?1)), 0),
                           COALESCE(SUM(NOT delayed AND lease_token IS NOT NULL AND lease_deadline > ?1), 0),
                           COALESCE(SUM(delayed), 0)
                    FROM (SELECT lease_token, lease_deadline, COALESCE(available_at > ?1, 0) AS delayed
                          FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?2))""")) {
                ps.setLong(1, nanos);
                ps.setString(2, queue);
                try (ResultSet rs = ps.executeQuery()) {
                    return new QueueCounts(rs.getLong(1), rs.getLong(2), rs.getLong(3));
                }
            }
        });
    }
}
