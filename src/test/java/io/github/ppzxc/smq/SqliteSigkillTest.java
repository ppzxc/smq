package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * 쓰기 도중 SIGKILL(ADR-0006 검증 조건, 건너뛰지 말 것). 자식 JVM이 {@code PROCESS_CRASH}로 발행을 반복하다 죽으면, 다시 열었을 때 락이 풀려 있고
 * 응답을 받은 발행이 모두 남아 있으며 {@code -wal}이 복구되고 {@code -shm}이 생기지 않는다. SIGKILL로는 두 내구성 수준을 구별할 수 없어
 * {@code PROCESS_CRASH}만 쓴다. Linux·Windows CI 모두에서 assume 없이 돈다.
 */
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

        // 자식의 표준 출력은 파일로 받는다. 파이프는 프로세스가 죽을 때 읽지 못한 줄이 사라질 수 있다.
        Path out = dir.resolve("child.out");
        Process child = startWriter(db, out);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (lineCount(out) < RECEIVED_BEFORE_KILL + 1) { // 첫 줄은 READY다.
            assertTrue(child.isAlive(), "자식이 발행 중에 끝났다");
            assertTrue(System.nanoTime() < deadline, "자식이 제때 발행하지 못했다");
            Thread.sleep(1);
        }
        child.destroyForcibly();
        assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        // 죽기 전에 쓴 id는 모두 publish가 돌아온 뒤에 찍힌 것이다. 잘린 마지막 줄도 한 건으로 센다.
        long received = lineCount(out) - 1;
        assertTrue(received >= RECEIVED_BEFORE_KILL);

        assertTrue(Files.exists(wal) && Files.size(wal) > 0, "죽은 뒤 복구할 -wal이 남아 있어야 이 테스트가 복구를 본다");
        assertFalse(Files.exists(shm), "처음부터 -shm 없이 열었으므로 죽은 뒤에도 없다");

        // (a) 락이 풀려 바로 열린다.
        try (Smq reopened = Smq.sqlite(db).open()) {
            // (b) 응답받은 발행은 모두 남고, 응답을 받지 못한 진행 중 발행이 하나 더 남았을 수 있다.
            long remaining =
                    reopened.declare("q", QueueOptions.defaults()).counts().ready();
            assertTrue(remaining >= received, "remaining=" + remaining + " received=" + received);
            assertTrue(remaining <= received + 1, "remaining=" + remaining + " received=" + received);
            // (c) -wal을 복구하고 -shm 없이 열린다.
            assertFalse(Files.exists(shm), "복구한 뒤에도 -shm이 생기지 않는다");
        }
        assertFalse(Files.exists(shm));
    }

    private static long lineCount(Path file) throws Exception {
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            return lines.count();
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
