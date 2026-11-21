import io.github.ppzxc.smq.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class Probe2 {
    static Path dir;
    static Smq open(String n) { return Smq.sqlite(dir.resolve(n + ".db")).open(); }
    static void fill(io.github.ppzxc.smq.Queue q, int n) {
        List<OutgoingMessage> b = new ArrayList<>();
        for (int i = 0; i < n; i++) { b.add(OutgoingMessage.of(new byte[100])); if (b.size() == 10000) { q.publish(b); b.clear(); } }
        if (!b.isEmpty()) q.publish(b);
    }
    static double pullAck(io.github.ppzxc.smq.Queue q, int ops, int chunk) throws Exception {
        long t = System.nanoTime(); long msgs = 0;
        for (int i = 0; i < ops; i++) { Chunk c = q.pull(chunk, Duration.ZERO); if (c.isEmpty()) throw new IllegalStateException("empty at " + i); msgs += c.messages().size(); q.settle(c.ackAll()); }
        return msgs / ((System.nanoTime() - t) / 1e9);
    }
    static double median(double[] v) { java.util.Arrays.sort(v); return v[v.length / 2]; }
    interface Run { double get() throws Exception; }
    static double med(Run r) throws Exception { double[] v = new double[5]; for (int i = 0; i < 5; i++) v[i] = r.get(); return median(v); }
    static void check(boolean ok, String what) { System.out.println((ok ? "  ok   " : "  FAIL ") + what); }

    public static void main(String[] a) throws Exception {
        dir = Files.createTempDirectory(Path.of(a[0]), "db");
        // ---- shallow costs (median of 5) ----
        try (Smq s = open("shallow")) { var q = s.declare("q", QueueOptions.defaults());
            for (int i = 0; i < 3000; i++) q.publish(OutgoingMessage.of(new byte[100])); pullAck(q, 3000, 1);
            System.out.printf("shallow publish 1         : %.0f msg/s%n", med(() -> { long t = System.nanoTime(); for (int i = 0; i < 3000; i++) q.publish(OutgoingMessage.of(new byte[100])); double r = 3000 / ((System.nanoTime() - t) / 1e9); pullAck(q, 30, 100); return r; }));
            System.out.printf("shallow publish batch 100 : %.0f msg/s%n", med(() -> { List<OutgoingMessage> b = new ArrayList<>(); for (int i = 0; i < 100; i++) b.add(OutgoingMessage.of(new byte[100])); long t = System.nanoTime(); for (int i = 0; i < 300; i++) q.publish(b); double r = 30000 / ((System.nanoTime() - t) / 1e9); pullAck(q, 300, 100); return r; }));
            System.out.printf("shallow pull1+ack         : %.0f msg/s%n", med(() -> { fill(q, 3000); return pullAck(q, 3000, 1); }));
            System.out.printf("shallow pull100+ack       : %.0f msg/s%n", med(() -> { fill(q, 30000); return pullAck(q, 300, 100); }));
            System.out.printf("shallow pub1+pull1+ack    : %.0f cycles/s%n", med(() -> { long t = System.nanoTime(); for (int i = 0; i < 3000; i++) { q.publish(OutgoingMessage.of(new byte[100])); q.settle(q.pull(1, Duration.ZERO).ackAll()); } return 3000 / ((System.nanoTime() - t) / 1e9); }));
        }
        // ---- deep ----
        int n = 200000;
        try (Smq s = open("plain")) { var q = s.declare("q", QueueOptions.defaults()); fill(q, n); pullAck(q, 300, 1);
            System.out.printf("deep %d plain            : %.0f pull+ack/s%n", n, pullAck(q, 2000, 1)); }
        try (Smq s = open("maxd")) { s.declare("dlq", QueueOptions.defaults()); var q = s.declare("q", QueueOptions.defaults().maxDeliveries(5, "dlq")); fill(q, n); pullAck(q, 300, 1);
            System.out.printf("deep %d maxDeliveries    : %.0f pull+ack/s%n", n, pullAck(q, 2000, 1)); }
        try (Smq s = open("sac")) { var q = s.declare("q", QueueOptions.defaults().singleActiveConsumer(true)); fill(q, n); pullAck(q, 300, 1);
            System.out.printf("deep %d singleActive     : %.0f pull+ack/s%n", n, pullAck(q, 2000, 1)); }
        try (Smq s = open("leased")) { var q = s.declare("q", QueueOptions.defaults()); fill(q, n);
            Chunk held = q.pull(n, Duration.ZERO, Duration.ofHours(1));
            long t = System.nanoTime(); int k = 500; for (int i = 0; i < k; i++) q.pull(1, Duration.ZERO);
            System.out.printf("leased-head %d empty poll: %.3f ms%n", held.messages().size(), (System.nanoTime() - t) / 1e6 / k);
            fill(q, 3000); pullAck(q, 300, 1);
            System.out.printf("leased-head %d           : %.0f pull+ack/s%n", n, pullAck(q, 2000, 1)); }
        try (Smq s = open("delayed")) { var q = s.declare("q", QueueOptions.defaults());
            List<OutgoingMessage> b = new ArrayList<>(); for (int i = 0; i < n; i++) b.add(OutgoingMessage.of(new byte[100]).withDelay(Duration.ofHours(1))); q.publish(b);
            long t = System.nanoTime(); int k = 500; for (int i = 0; i < k; i++) q.pull(1, Duration.ZERO);
            System.out.printf("delayed-head %d empty poll: %.3f ms%n", n, (System.nanoTime() - t) / 1e6 / k);
            fill(q, 3000); pullAck(q, 300, 1);
            System.out.printf("delayed-head %d          : %.0f pull+ack/s%n", n, pullAck(q, 2000, 1)); }
        // redelivered-ready backlog: every message nacked once, then consumed, on a maxDeliveries queue
        try (Smq s = open("nacked")) { s.declare("dlq", QueueOptions.defaults()); var q = s.declare("q", QueueOptions.defaults().maxDeliveries(5, "dlq")); fill(q, 100000);
            long t = System.nanoTime(); for (int i = 0; i < 100; i++) { Chunk c = q.pull(1000, Duration.ZERO); q.settle(c.settle().nackRest(Duration.ofMillis(1))); }
            System.out.printf("nack 100k in chunks of 1000: %.0f msg/s%n", 100000 / ((System.nanoTime() - t) / 1e9));
            Thread.sleep(50);
            t = System.nanoTime(); Chunk first = q.pull(1, Duration.ZERO);
            System.out.printf("first pull after 100k delays ended (promotion): %.1f ms%n", (System.nanoTime() - t) / 1e6); q.settle(first.ackAll());
            System.out.printf("nacked-once backlog 100k   : %.0f pull+ack/s%n", pullAck(q, 2000, 1)); }
        // mass lease expiry: 100k leased in one chunk with a 1ms lease, then pull
        try (Smq s = open("expire")) { var q = s.declare("q", QueueOptions.defaults()); fill(q, 100000);
            q.pull(100000, Duration.ZERO, Duration.ofMillis(1)); Thread.sleep(20);
            long t = System.nanoTime(); Chunk c = q.pull(1, Duration.ZERO);
            System.out.printf("first pull after 100k leases expired (promotion): %.1f ms, got id %d%n", (System.nanoTime() - t) / 1e6, c.messages().get(0).id().value());
            q.settle(c.ackAll());
            System.out.printf("expired backlog 100k       : %.0f pull+ack/s%n", pullAck(q, 2000, 1)); }
        // exhausted flood: maxDeliveries=1, 20k messages delivered once and nacked -> all must move to DLQ
        try (Smq s = open("flood")) { var dlq = s.declare("dlq", QueueOptions.defaults()); var q = s.declare("q", QueueOptions.defaults().maxDeliveries(1, "dlq")); fill(q, 20000);
            for (int i = 0; i < 20; i++) { Chunk c = q.pull(1000, Duration.ZERO); q.settle(c.settle().nackRest(Duration.ofMillis(1))); }
            Thread.sleep(20); q.publish(OutgoingMessage.of(new byte[1]));
            long t = System.nanoTime(); Chunk c = q.pull(1, Duration.ofSeconds(60));
            System.out.printf("pull behind 20k exhausted  : %.0f ms, dlq ready=%d, got=%d%n", (System.nanoTime() - t) / 1e6, dlq.counts().ready(), c.messages().size()); }

        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
        InstantSource clock = now::get;
        System.out.println("semantics:");
        try (Smq s = Smq.sqlite(dir.resolve("sem.db")).clock(clock).leaseDuration(Duration.ofSeconds(10)).open()) {
            var q = s.declare("q", QueueOptions.defaults());
            MessageId m1 = q.publish(OutgoingMessage.of(new byte[]{1})); MessageId m2 = q.publish(OutgoingMessage.of(new byte[]{2}).withDelay(Duration.ofSeconds(5))); MessageId m3 = q.publish(OutgoingMessage.of(new byte[]{3}));
            check(q.counts().equals(new QueueCounts(2, 0, 1)), "counts 2/0/1 after publish: " + q.counts());
            Chunk c1 = q.pull(10, Duration.ZERO);
            check(ids(c1).equals(List.of(m1, m3)), "delayed message skipped: " + ids(c1));
            now.set(now.get().plusSeconds(6));
            Chunk c2 = q.pull(10, Duration.ZERO);
            check(ids(c2).equals(List.of(m2)), "ended delay pulled: " + ids(c2));
            now.set(now.get().plusSeconds(20)); // all leases expired, nobody re-pulled
            check(q.extend(c1, Duration.ofSeconds(10)).outcomes().allOk(), "extend of expired, not re-pulled lease is OK (non-SAC)");
            now.set(now.get().plusSeconds(20));
            check(q.settle(c1.settle().ack(m1).build()).allOk(), "late ACK of expired, not re-pulled message is OK");
            Chunk c3 = q.pull(1, Duration.ZERO);
            check(ids(c3).equals(List.of(m2)), "expired leases come back in id order (m2 first): " + ids(c3));
            check(q.settle(c1.settle().nack(m3).build()).allOk(), "late NACK of promoted message still OK");
            check(!q.settle(c2.ackAll()).allOk(), "old lease on re-pulled m2 is STALE");
            check(q.counts().equals(new QueueCounts(1, 1, 0)), "counts 1/1/0: " + q.counts());
            Chunk c4 = q.pull(10, Duration.ZERO);
            check(ids(c4).equals(List.of(m3)) && c4.messages().get(0).deliveryCount() == 2, "m3 redelivered with count 2");
            q.settle(c3.ackAll()); q.settle(c4.ackAll());
            check(q.counts().equals(new QueueCounts(0, 0, 0)), "counts 0/0/0: " + q.counts());
            var sac = s.declare("sac", QueueOptions.defaults().singleActiveConsumer(true));
            sac.publish(OutgoingMessage.of(new byte[]{1})); sac.publish(OutgoingMessage.of(new byte[]{2}));
            Chunk a1 = sac.pull(1, Duration.ZERO);
            check(sac.pull(1, Duration.ZERO).isEmpty(), "SAC blocks second chunk");
            now.set(now.get().plusSeconds(11));
            check(!sac.extend(a1, Duration.ofSeconds(10)).outcomes().allOk(), "SAC extend of expired lease is STALE (before any pull)");
            Chunk a2 = sac.pull(2, Duration.ZERO);
            check(a2.messages().size() == 2, "SAC pulls both after expiry");
            check(!sac.extend(a1, Duration.ofSeconds(10)).outcomes().allOk(), "SAC extend of old lease STALE after re-pull");
            s.declare("dlq", QueueOptions.defaults()); var md = s.declare("md", QueueOptions.defaults().maxDeliveries(2, "dlq"));
            MessageId x = md.publish(OutgoingMessage.of(new byte[]{9}));
            md.settle(md.pull(1, Duration.ZERO).settle().nack(x).build()); md.settle(md.pull(1, Duration.ZERO).settle().nack(x).build());
            check(md.pull(1, Duration.ZERO).isEmpty() && s.queue("dlq").counts().ready() == 1 && md.counts().equals(new QueueCounts(0,0,0)), "exhausted message moved to DLQ on next pull");
        }
        // reopen keeps counts
        try (Smq s = Smq.sqlite(dir.resolve("sem.db")).clock(clock).open()) { check(s.queue("sac").counts().equals(new QueueCounts(0, 2, 0)), "reopen counts: " + s.queue("sac").counts()); }
        // query plans
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("sem.db")); Statement st = c.createStatement()) {
            String[] qs = {
              "SELECT id FROM message WHERE queue_id = 1 AND lease_deadline IS NULL AND available_at IS NULL AND delivery_count < 5 ORDER BY id LIMIT 10",
              "SELECT id FROM message WHERE queue_id = 1 AND lease_deadline IS NULL AND available_at IS NULL AND delivery_count > 0 AND delivery_count >= 5 ORDER BY delivery_count, id LIMIT 101",
              "SELECT 1 FROM message WHERE queue_id = 1 AND lease_deadline > 5 LIMIT 1",
              "UPDATE message SET lease_deadline = NULL WHERE queue_id = 1 AND lease_deadline <= 5",
              "UPDATE message SET available_at = NULL WHERE queue_id = 1 AND available_at <= 5",
              "SELECT EXISTS(SELECT 1 FROM message WHERE queue_id = 1 AND lease_deadline IS NULL AND available_at IS NULL) OR EXISTS(SELECT 1 FROM message WHERE queue_id = 1 AND lease_deadline <= 5) OR EXISTS(SELECT 1 FROM message WHERE queue_id = 1 AND available_at <= 5)",
              "DELETE FROM message WHERE queue_id = 1" };
            for (String q : qs) { try (ResultSet rs = st.executeQuery("EXPLAIN QUERY PLAN " + q)) { StringBuilder b = new StringBuilder(); while (rs.next()) b.append(rs.getString("detail")).append(" | "); System.out.println("plan: " + q.substring(0, Math.min(70, q.length())) + "...\n      " + b); } }
        } catch (SQLException e) { System.out.println("plan: " + e); }
    }
    static List<MessageId> ids(Chunk c) { return c.messages().stream().map(Message::id).toList(); }
}
