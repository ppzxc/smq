package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.QueueCounts;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;

/**
 * 테스트 전용 SQL 집계. 운영 코드의 {@code counts()}는 메모리 개수를 읽으므로(ADR-0018), DB의 실제 행을 직접 세어 비교할 때 쓴다.
 * 큐를 훑으므로 깊은 큐에서 부르지 않는다.
 */
final class ExactCounts {

    private ExactCounts() {}

    /** 저장된 상태 기준: 리스 토큰이 있으면 리스 중, 없고 꺼낼 시각 값이 있으면 지연됨, 나머지는 준비. 시각은 보지 않는다. */
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

    /** 시각 기준: {@code now}에 꺼낼 수 있으면 준비, 리스가 살아 있으면 리스 중, 지연이 안 끝났으면 지연됨. */
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
