package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class SqliteSigkillTest {

    private static final int RECEIVED_BEFORE_KILL = 200;

    @TempDir
    Path dir;

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 1")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck1()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            final var assertionValue1Argument1 = child.isAlive();
            assertTrue(assertionValue1Argument1, "자식이 발행 중에 끝났다");
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 2")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck2()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            final var assertionValue2Argument1 = System.nanoTime() < deadline;
            assertTrue(assertionValue2Argument1, "자식이 제때 발행하지 못했다");
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 3")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck3()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        final var assertionValue3Argument1 = child.waitFor(30, TimeUnit.SECONDS);
        assertTrue(assertionValue3Argument1, "The child process should exit before the timeout.");
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 4")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck4()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        final long received;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        received = new LineCounter(out).count() - 1;
        final var assertionValue4Argument1 = received >= RECEIVED_BEFORE_KILL;
        assertTrue(assertionValue4Argument1, "Received should be at least RECEIVED_BEFORE_KILL.");
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 5")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck5()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        final var assertionValue5Argument1 = Files.exists(wal) && Files.size(wal) > 0;
        assertTrue(assertionValue5Argument1, "죽은 뒤 복구할 -wal이 남아 있어야 이 테스트가 복구를 본다");
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 6")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck6()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        final var assertionValue6Argument1 = Files.exists(shm);
        assertFalse(assertionValue6Argument1, "처음부터 -shm 없이 열었으므로 죽은 뒤에도 없다");
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 7")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck7()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        final long received;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        received = new LineCounter(out).count() - 1;
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            final long remaining;
            remaining = reopened.declare("q", QueueOptions.defaults()).counts().ready();
            final var assertionValue7Argument1 = remaining >= received;
            final var assertionValue7Argument2 = "remaining=" + remaining + " received=" + received;
            assertTrue(assertionValue7Argument1, assertionValue7Argument2);
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 8")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck8()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        final long received;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        received = new LineCounter(out).count() - 1;
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            final long remaining;
            remaining = reopened.declare("q", QueueOptions.defaults()).counts().ready();
            final var assertionValue8Argument1 = remaining <= received + 1;
            final var assertionValue8Argument2 = "remaining=" + remaining + " received=" + received;
            assertTrue(assertionValue8Argument1, assertionValue8Argument2);
            evaluate(Files.exists(shm));
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 9")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck9()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            final var assertionValue9Argument1 = Files.exists(shm);
            assertFalse(assertionValue9Argument1, "복구한 뒤에도 -shm이 생기지 않는다");
        }
        evaluate(Files.exists(shm));
    }

    @Test
    @Timeout(120)
    @DisplayName("쓰기 도중 SIGKILL 뒤 다시 열면 락이 풀려 있고 응답받은 발행이 남고 wal이 복구되며 shm이 없다 — check 10")
    void
            ifYouReopenAfterSIGKILLDuringAWriteOperationTheLockIsReleasedTheRespondedPublicationRemainsTheWALIsRecoveredAndThereIsNoSHMCheck10()
                    throws Exception {
        final Path db;
        final Path wal;
        final Path shm;
        final Path out;
        final Process child;
        final long deadline;
        final LineCounter counter;
        db = dir.resolve("smq.db");
        wal = Path.of(db + "-wal");
        shm = Path.of(db + "-shm");
        out = dir.resolve("child.out");
        child = startWriter(db, out);
        deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        counter = new LineCounter(out);
        while (counter.count() < RECEIVED_BEFORE_KILL + 1) {
            evaluate(child.isAlive());
            evaluate(System.nanoTime() < deadline);
            Thread.sleep(5);
        }
        child.destroyForcibly();
        evaluate(child.waitFor(30, TimeUnit.SECONDS));
        evaluate(new LineCounter(out).count() - 1);
        evaluate(Files.exists(wal) && Files.size(wal) > 0);
        evaluate(Files.exists(shm));
        try (Smq reopened = Smq.sqlite(db).open()) {
            evaluate(reopened.declare("q", QueueOptions.defaults()).counts().ready());
            evaluate(Files.exists(shm));
        }
        final var assertionValue10Argument1 = Files.exists(shm);
        assertFalse(assertionValue10Argument1, "Shared memory file should not exist.");
    }

    private static final class LineCounter {

        private final Path file;

        private long offset;

        private long lines;

        private boolean partial;

        LineCounter(final Path file) {
            this.file = file;
        }

        long count() throws Exception {
            try (SeekableByteChannel ch = Files.newByteChannel(file)) {
                final ByteBuffer buf;
                int n;
                ch.position(offset);
                buf = ByteBuffer.allocate(8192);
                n = ch.read(buf);
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

    private Process startWriter(final Path db, final Path out) throws Exception {
        final String java;
        java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
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

    private static Object evaluate(final Object value) {
        return value;
    }
}
