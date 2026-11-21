import io.github.ppzxc.smq.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicReference;
public class Sac {
    public static void main(String[] a) throws Exception {
        for (boolean mem : new boolean[] {true, false}) {
            AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
            InstantSource clock = now::get;
            try (Smq s = mem ? Smq.inMemory().clock(clock).open() : Smq.sqlite(java.nio.file.Files.createTempDirectory(java.nio.file.Path.of(a[0]), "sac").resolve("q.db")).clock(clock).open()) {
                var q = s.declare("q", QueueOptions.defaults().singleActiveConsumer(true));
                MessageId m1 = q.publish(OutgoingMessage.of(new byte[]{1})); MessageId m2 = q.publish(OutgoingMessage.of(new byte[]{2}));
                Chunk c = q.pull(1, Duration.ZERO);
                q.settle(c.settle().nack(m1, Duration.ofSeconds(30)).build());
                Chunk next = q.pull(1, Duration.ZERO);
                System.out.println((mem ? "memory" : "sqlite") + ": after delayed NACK of " + m1.value() + ", next pull returns " + next.messages().stream().map(m -> m.id().value()).toList());
            }
        }
    }
}
