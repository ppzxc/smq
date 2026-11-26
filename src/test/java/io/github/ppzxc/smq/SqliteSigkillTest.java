package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class SqliteSigkillTest {

    private static final int RECEIVED_BEFORE_KILL = 200;

    @TempDir
    Path dir;

    @Test
    @Timeout(120)
    void 쓰기_도중_SIGKILL_뒤_다시_열면_락이_풀려_있고_응답받은_발행이_남고_wal이_복구되며_shm이_없다() throws Exception {
        Path db = dir.resolve("smq.db");
        Path wal = Path.of(db + "-wal");
        Path shm = Path.of(db + "-shm");

        Path out = dir.resolve("child.out");
        Process child = startWriter(db, out);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        LineCounter counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            assertTrue(child.isAlive(), "자식이 발행 중에 끝났다");
            assertTrue(System.nanoTime() < deadline, "자식이 제때 발행하지 못했다");
            Thread.sleep(5);
        }
        child.destroyForcibly();
        assertTrue(child.waitFor(30, TimeUnit.SECONDS));

        long received = new LineCounter(out).count() - 1;
        assertTrue(received >= RECEIVED_BEFORE_KILL);

        assertTrue(Files.exists(wal) && Files.size(wal) > 0, "죽은 뒤 복구할 -wal이 남아 있어야 이 테스트가 복구를 본다");
        assertFalse(Files.exists(shm), "처음부터 -shm 없이 열었으므로 죽은 뒤에도 없다");

        try (Smq reopened = Smq.sqlite(db).open()) {

            long remaining =
                    reopened.declare("q", QueueOptions.defaults()).counts().ready();
            assertTrue(remaining >= received, "remaining=" + remaining + " received=" + received);
            assertTrue(remaining <= received + 1, "remaining=" + remaining + " received=" + received);

            assertFalse(Files.exists(shm), "복구한 뒤에도 -shm이 생기지 않는다");
        }
        assertFalse(Files.exists(shm));
    }

    private static final class LineCounter {
        private final Path file;
        private long offset;
        private long lines;
        private boolean partial;

        LineCounter(Path file) {
            this.file = file;
        }

        long count() throws Exception {
            try (SeekableByteChannel ch = Files.newByteChannel(file)) {
                ch.position(offset);
                ByteBuffer buf = ByteBuffer.allocate(8192);
                int n = ch.read(buf);
                while (n > 0) {
                    for (int i = 0; i < n; i++) {
                        if (buf.get(i) == '\n') {
                            lines++;
                            partial = false;
                        } else {
                            partial = true;
                        }
                    }
                    offset += n;
                    buf.clear();
                    n = ch.read(buf);
                }
            }
            return lines + (partial ? 1 : 0);
        }
    }

    private Process startWriter(Path db, Path out) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(
                        java,
                        "--enable-native-access=ALL-UNNAMED",
                        "-cp",
                        System.getProperty("java.class.path"),
                        SigkillWriterMain.class.getName(),
                        db.toString())
                .redirectOutput(out.toFile())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
    }
}
