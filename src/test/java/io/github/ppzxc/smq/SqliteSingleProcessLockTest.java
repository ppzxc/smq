package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** SQLite 저장소는 한 JVM만 연다. 공개 열기 API로 그 강제를 검증한다(ADR-0012). */
class SqliteSingleProcessLockTest {

    @TempDir
    Path dir;

    @Test
    void 같은_JVM에서_같은_파일을_두_번_열면_즉시_StorageLockedException이고_경로가_메시지에_있다() {
        Path db = dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            StorageLockedException e = assertThrows(
                    StorageLockedException.class, () -> Smq.sqlite(db).open());
            assertTrue(e.getMessage().contains(db.toString()), e.getMessage());
            assertTrue(e.getMessage().contains(db + ".lock"), e.getMessage());
            assertNotNull(first);
        }
    }

    @Test
    void 닫고_나면_락_파일이_남아_있어도_다시_열_수_있다() {
        Path db = dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            assertNotNull(first);
        }
        assertTrue(Files.exists(Path.of(db + ".lock")));
        try (Smq second = Smq.sqlite(db).open()) {
            assertNotNull(second);
        }
    }

    /** ADR-0006 검증 조건, 건너뛰지 말 것. Windows 조건부 지원의 근거라서 assume 없이 Linux·Windows CI에서 모두 돈다. */
    @Test
    @Timeout(60)
    void 다른_JVM이_쥔_파일은_열_수_없고_그_JVM이_죽으면_다시_열_수_있다() throws Exception {
        Path db = dir.resolve("smq.db");
        Process child = startHolder(db);
        try {
            assertThrows(StorageLockedException.class, () -> Smq.sqlite(db).open());
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        }
        try (Smq reopened = Smq.sqlite(db).open()) {
            assertNotNull(reopened);
        }
    }

    @Test
    @Timeout(20)
    void 외부_도구가_DB를_쥐고_있으면_기다리지_않고_즉시_StorageLockedException이다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            assertNotNull(smq);
        }
        try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = external.createStatement()) {
            // 외부 도구가 EXCLUSIVE로 쥔 상태를 만든다.
            s.execute("PRAGMA locking_mode = EXCLUSIVE");
            s.execute("BEGIN EXCLUSIVE");
            long start = System.nanoTime();
            assertThrows(StorageLockedException.class, () -> Smq.sqlite(db).open());
            assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10)) < 0);
        }
    }

    @Test
    void smq가_열려_있는_동안_외부_연결은_읽을_수_없다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            smq.declare("q", QueueOptions.defaults());
            try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                    Statement s = external.createStatement()) {
                assertThrows(SQLException.class, () -> s.executeQuery("SELECT count(*) FROM message"));
            }
        }
    }

    @Test
    void 이전_다중_프로세스_사용이_남긴_shm_파일이_있어도_열린다() throws Exception {
        Path db = dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            assertNotNull(smq);
        }
        Files.write(Path.of(db + "-shm"), new byte[32768]);
        try (Smq reopened = Smq.sqlite(db).open()) {
            reopened.declare("q", QueueOptions.defaults());
        }
    }

    @Test
    void 락_파일을_만들_수_없으면_StorageLockedException이_아니라_SmqStorageException이다() {
        Path db = dir.resolve("없는_디렉터리").resolve("smq.db");
        SmqStorageException e =
                assertThrows(SmqStorageException.class, () -> Smq.sqlite(db).open());
        assertTrue(e.getMessage().contains(db + ".lock"), e.getMessage());
    }

    @Test
    void 스키마_판정이_거부해도_BUSY로_오인하지_않고_락을_풀어_다시_열어도_같은_거부다() throws Exception {
        Path newer = dir.resolve("newer.db");
        try (Smq smq = Smq.sqlite(newer).open()) {
            assertNotNull(smq);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + newer);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA user_version = 999");
        }
        Path foreign = dir.resolve("foreign.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + foreign);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t(x)");
        }
        for (Path db : new Path[] {newer, foreign}) {
            // 두 번 열어 첫 거부가 락 파일을 쥔 채 남지 않았는지 본다.
            for (int i = 0; i < 2; i++) {
                assertThrows(
                        IncompatibleStorageException.class, () -> Smq.sqlite(db).open(), db.toString());
            }
        }
    }

    private Process startHolder(Path db) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process p = new ProcessBuilder(
                        java,
                        "--enable-native-access=ALL-UNNAMED",
                        "-cp",
                        System.getProperty("java.class.path"),
                        LockHolderMain.class.getName(),
                        db.toString())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        assertEquals("READY", in.readLine());
        return p;
    }
}
