import io.github.ppzxc.smq.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.List; import java.util.ArrayList;

public class Probe {
    static Path dir;
    static Smq open(String n) throws Exception { return Smq.sqlite(dir.resolve(n + ".db")).open(); }
    static void fill(Queue q, int n) {
        List<OutgoingMessage> b = new ArrayList<>();
        for (int i = 0; i < n; i++) { b.add(OutgoingMessage.of(new byte[100])); if (b.size() == 10000) { q.publish(b); b.clear(); } }
        if (!b.isEmpty()) q.publish(b);
    }
    static double pullAck(Queue q, int ops) throws Exception {
        long t = System.nanoTime();
        for (int i = 0; i < ops; i++) { Chunk c = q.pull(1, Duration.ZERO); if (c.isEmpty()) throw new IllegalStateException("empty at " + i); q.settle(c.ackAll()); }
        return ops / ((System.nanoTime() - t) / 1e9);
    }
    public static void main(String[] a) throws Exception {
        dir = Files.createTempDirectory(Path.of(a[0]), "db");
        for (int n : new int[] {1000, 50000, 200000}) {
            try (Smq s = open("plain" + n)) { Queue q = s.declare("q", QueueOptions.defaults()); fill(q, n); pullAck(q, 200);
                System.out.printf("backlog=%d plain          : %.0f pull+ack/s%n", n, pullAck(q, 500)); }
            try (Smq s = open("maxd" + n)) { s.declare("dlq", QueueOptions.defaults()); Queue q = s.declare("q", QueueOptions.defaults().maxDeliveries(5, "dlq")); fill(q, n); pullAck(q, 200);
                System.out.printf("backlog=%d maxDeliveries  : %.0f pull+ack/s%n", n, pullAck(q, 500)); }
            try (Smq s = open("sac" + n)) { Queue q = s.declare("q", QueueOptions.defaults().singleActiveConsumer(true)); fill(q, n); pullAck(q, 200);
                System.out.printf("backlog=%d singleActive   : %.0f pull+ack/s%n", n, pullAck(q, 500)); }
        }
        // leased / delayed head
        for (int n : new int[] {1000, 100000}) {
            try (Smq s = open("leased" + n)) { Queue q = s.declare("q", QueueOptions.defaults()); fill(q, n);
                Chunk held = q.pull(n, Duration.ZERO, Duration.ofHours(1));
                long t = System.nanoTime(); int k = 200; for (int i = 0; i < k; i++) q.pull(1, Duration.ZERO);
                System.out.printf("leased-head=%d empty poll : %.3f ms/poll%n", held.messages().size(), (System.nanoTime() - t) / 1e6 / k);
                fill(q, 1000);
                System.out.printf("leased-head=%d            : %.0f pull+ack/s%n", n, pullAck(q, 300)); }
            try (Smq s = open("delayed" + n)) { Queue q = s.declare("q", QueueOptions.defaults());
                List<OutgoingMessage> b = new ArrayList<>(); for (int i = 0; i < n; i++) b.add(OutgoingMessage.of(new byte[100]).withDelay(Duration.ofHours(1))); q.publish(b);
                long t = System.nanoTime(); int k = 200; for (int i = 0; i < k; i++) q.pull(1, Duration.ZERO);
                System.out.printf("delayed-head=%d empty poll: %.3f ms/poll%n", n, (System.nanoTime() - t) / 1e6 / k);
                fill(q, 1000);
                System.out.printf("delayed-head=%d           : %.0f pull+ack/s%n", n, pullAck(q, 300)); }
        }
        // forever wait
        try (Smq s = open("forever")) { Queue q = s.declare("q", QueueOptions.defaults()); q.publish(OutgoingMessage.of(new byte[1]));
            try { q.pull(1, Duration.ofSeconds(Long.MAX_VALUE)); System.out.println("forever wait: ok"); } catch (Throwable t) { System.out.println("forever wait: " + t); }
            try { Chunk c = q.pull(1, Duration.ZERO); q.settle(c.nackAll(Duration.ofDays(365L * 1000))); System.out.println("sqlite nack 1000y: ok"); } catch (Throwable t) { System.out.println("sqlite nack 1000y: " + t); }
            try { q.pull(1, Duration.ZERO, Duration.ofDays(365L * 1000)); System.out.println("sqlite lease 1000y: ok"); } catch (Throwable t) { System.out.println("sqlite lease 1000y: " + t); }
            try { q.publish(OutgoingMessage.of(new byte[1]).withDelay(Duration.ofDays(365L * 1000))); System.out.println("sqlite delay 1000y: ok"); } catch (Throwable t) { System.out.println("sqlite delay 1000y: " + t); }
        }
        try (Smq s = Smq.inMemory().open()) { Queue q = s.declare("q", QueueOptions.defaults()); q.publish(OutgoingMessage.of(new byte[1]));
            try { Chunk c = q.pull(1, Duration.ZERO); q.settle(c.nackAll(Duration.ofDays(365L * 1000))); System.out.println("memory nack 1000y: ok"); } catch (Throwable t) { System.out.println("memory nack 1000y: " + t); } }
        // interrupt while waiting for the storage lock
        try (Smq s = open("intr")) { Queue q = s.declare("q", QueueOptions.defaults()); Queue big = s.declare("big", QueueOptions.defaults());
            List<OutgoingMessage> b = new ArrayList<>(); for (int i = 0; i < 400000; i++) b.add(OutgoingMessage.of(new byte[200]));
            Thread holder = new Thread(() -> big.publish(b)); 
            Throwable[] seen = new Throwable[1];
            Thread puller = new Thread(() -> { try { q.pull(1, Duration.ofSeconds(30)); seen[0] = new RuntimeException("returned normally"); } catch (Throwable t) { seen[0] = t; } });
            holder.start(); Thread.sleep(150); puller.start(); Thread.sleep(150); puller.interrupt(); puller.join(); holder.join();
            System.out.println("interrupt during lock wait: " + seen[0]); }
        // DLQ waiter latency when messages are moved by pull
        try (Smq s = open("dlqwake")) { Queue dlq = s.declare("dlq", QueueOptions.defaults()); Queue q = s.declare("q", QueueOptions.defaults().maxDeliveries(1, "dlq"));
            q.publish(OutgoingMessage.of(new byte[1])); q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofMillis(1))); Thread.sleep(600);
            long[] got = new long[1];
            Thread w = new Thread(() -> { try { dlq.pull(1, Duration.ofSeconds(5)); got[0] = System.nanoTime(); } catch (Exception e) {} });
            w.start(); Thread.sleep(700); long t0 = System.nanoTime(); q.pull(1, Duration.ZERO); w.join();
            System.out.printf("DLQ waiter woke %.0f ms after the move%n", (got[0] - t0) / 1e6); }
    }
}
