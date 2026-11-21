import re, sys, os
keep_full = os.environ.get("KEEP_FULL", "1") == "1"
p = "src/io/github/ppzxc/smq/storage/sqlite/"
s = open(p + "SqliteSchema.java").read()
full = 's.execute("CREATE INDEX message_ready ON message(queue_id, id)");\n' if keep_full else 's.execute("CREATE INDEX message_queue ON message(queue_id)");\n'
idx = full + '''            s.execute("CREATE INDEX message_stored_ready ON message(queue_id, id) WHERE lease_deadline IS NULL AND available_at IS NULL");
            s.execute("CREATE INDEX message_lease_deadline ON message(queue_id, lease_deadline) WHERE lease_deadline IS NOT NULL");
            s.execute("CREATE INDEX message_available_at ON message(queue_id, available_at) WHERE available_at IS NOT NULL");
            s.execute("CREATE INDEX message_redelivered_ready ON message(queue_id, delivery_count, id) WHERE lease_deadline IS NULL AND available_at IS NULL AND delivery_count > 0");
'''
assert 's.execute("CREATE INDEX message_ready ON message(queue_id, id)");\n' in s
s = s.replace('s.execute("CREATE INDEX message_ready ON message(queue_id, id)");\n', idx)
open(p + "SqliteSchema.java", "w").write(s)

s = open(p + "SqliteStorage.java").read()
def rep(a, b, count=1):
    global s
    assert a in s, a
    s = s.replace(a, b, count)
# stored-ready condition without time parameters
rep('''"(available_at IS NULL OR available_at <= ?) AND (lease_token IS NULL OR lease_deadline <= ?)";''',
    '''"lease_deadline IS NULL AND available_at IS NULL";''')
rep('''"SELECT id, payload, headers, delivery_count, " + STORED_STATE
            + READY_FROM_WHERE
            + " AND delivery_count >= ? ORDER BY id LIMIT ?";''',
    '''"SELECT id, payload, headers, delivery_count, " + STORED_STATE
            + READY_FROM_WHERE
            + " AND delivery_count > 0 AND delivery_count >= ? ORDER BY delivery_count, id LIMIT ?";''')
# anyReady
rep('''"SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?)"
                                    + " AND " + READY_TIME_CONDITION + "), "
                                    + "EXISTS(SELECT 1 FROM queue WHERE name = ?)")) {
                        ps.setString(1, queue);
                        ps.setLong(2, toNanos(now));
                        ps.setLong(3, toNanos(now));
                        ps.setString(4, queue);''',
    '''"SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?1)"
                                    + " AND " + READY_TIME_CONDITION + ")"
                                    + " OR EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?1) AND lease_deadline <= ?2)"
                                    + " OR EXISTS(SELECT 1 FROM message WHERE queue_id = (SELECT id FROM queue WHERE name = ?1) AND available_at <= ?2), "
                                    + "EXISTS(SELECT 1 FROM queue WHERE name = ?1)")) {
                        ps.setString(1, queue);
                        ps.setLong(2, toNanos(now));''')
rep('"SELECT 1 FROM message WHERE queue_id = ? AND lease_token IS NOT NULL AND lease_deadline > ? LIMIT 1"',
    '"SELECT 1 FROM message WHERE queue_id = ? AND lease_deadline > ? LIMIT 1"')
# promotion before moveExhausted
rep('''                    int limit = options.maxDeliveries().orElse(Integer.MAX_VALUE);''',
    '''                    try (PreparedStatement ps = c.prepareStatement(
                            "UPDATE message SET lease_deadline = NULL WHERE queue_id = ? AND lease_deadline <= ?")) {
                        ps.setLong(1, queueId);
                        ps.setLong(2, toNanos(now));
                        ps.executeUpdate();
                    }
                    try (PreparedStatement ps = c.prepareStatement(
                            "UPDATE message SET available_at = NULL WHERE queue_id = ? AND available_at <= ?")) {
                        ps.setLong(1, queueId);
                        ps.setLong(2, toNanos(now));
                        int n = ps.executeUpdate();
                        tx.delayed(queueId, -n);
                        tx.ready(queueId, n);
                    }
                    int limit = options.maxDeliveries().orElse(Integer.MAX_VALUE);''')
# peek binding
rep('''                        peek.setLong(1, queueId);
                        peek.setLong(2, toNanos(now));
                        peek.setLong(3, toNanos(now));
                        peek.setInt(4, limit);
                        peek.setInt(5, max);''',
    '''                        peek.setLong(1, queueId);
                        peek.setInt(2, limit);
                        peek.setInt(3, max);''')
rep('''                        ps.setLong(3, queueId);
                        ps.setLong(4, toNanos(now));
                        ps.setLong(5, toNanos(now));
                        ps.setInt(6, limit);
                        ps.setInt(7, max);''',
    '''                        ps.setLong(3, queueId);
                        ps.setInt(4, limit);
                        ps.setInt(5, max);''')
rep('''            ps.setLong(1, queueId);
            ps.setLong(2, toNanos(now));
            ps.setLong(3, toNanos(now));
            ps.setInt(4, limit);
            ps.setInt(5, DeadLetters.MAX_MOVES_PER_PULL + 1);''',
    '''            ps.setLong(1, queueId);
            ps.setInt(2, limit);
            ps.setInt(3, DeadLetters.MAX_MOVES_PER_PULL + 1);''')
open(p + "SqliteStorage.java", "w").write(s)
