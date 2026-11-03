package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** SQLite 파일의 스키마 판정과 트랜잭션마다 하는 버전 확인(ADR 0004). 공개 API로 열고, 파일 조작만 JDBC로 한다. */
class SqliteSchemaTest {

    @TempDir
    Path dir;

    private Path db() {
        return dir.resolve("smq.db");
    }

    private static void jdbc(Path db, String... statements) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement()) {
            for (String sql : statements) {
                s.execute(sql);
            }
        }
    }

    private static int userVersion(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA user_version")) {
            return rs.getInt(1);
        }
    }

    /** 이 빌드가 지원하는 스키마 버전. 새 파일을 만들어 읽는다. */
    private int supportedVersion() throws Exception {
        Path probe = dir.resolve("probe.db");
        Smq.sqlite(probe).open().close();
        return userVersion(probe);
    }

    private static OutgoingMessage msg(String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void 더_높은_버전의_smq_파일은_발견한_버전과_지원하는_버전을_적어_거부하고_파일을_건드리지_않는다() throws Exception {
        int supported = supportedVersion();
        Smq.sqlite(db()).open().close();
        int newer = supported + 5;
        jdbc(db(), "PRAGMA user_version = " + newer);

        IncompatibleStorageException e = assertThrows(
                IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("found " + newer), e.getMessage());
        assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());
        assertEquals(newer, userVersion(db()));
    }

    @Test
    void 같은_버전의_smq_파일은_데이터를_그대로_둔_채_열린다() throws Exception {
        int supported = supportedVersion();
        try (Smq first = Smq.sqlite(db()).open()) {
            first.declare("q", QueueOptions.defaults()).publish(msg("a"));
        }

        try (Smq second = Smq.sqlite(db()).open()) {
            assertEquals(1, second.queue("q").counts().ready());
        }
        assertEquals(supported, userVersion(db()));
    }

    @Test
    void smq가_아닌_비어_있지_않은_파일은_거부하고_망가뜨리지_않는다() throws Exception {
        jdbc(db(), "CREATE TABLE foreign_table(x)", "INSERT INTO foreign_table VALUES (1)");

        assertThrows(IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db());
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM foreign_table")) {
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void sqlite_파일_형식이_아닌_파일도_거부한다() throws Exception {
        Files.writeString(db(), "this is not a database file at all, just some text\n".repeat(20));

        assertThrows(IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(Files.readString(db()).startsWith("this is not a database"));
    }

    @Test
    void 다른_표식이_있는_파일은_테이블이_없어도_거부한다() throws Exception {
        int supported = supportedVersion();
        jdbc(db(), "PRAGMA application_id = 12345");

        IncompatibleStorageException e = assertThrows(
                IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("application_id=12345"), e.getMessage());
        assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());
    }

    @Test
    void 다른_표식이_있으면_user_version이_지원하는_버전과_같아도_거부한다() throws Exception {
        int supported = supportedVersion();
        jdbc(db(), "PRAGMA application_id = 12345", "PRAGMA user_version = " + supported);

        IncompatibleStorageException e = assertThrows(
                IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("application_id=12345"), e.getMessage());
    }

    @Test
    void 크기가_0인_파일은_새_파일처럼_열린다() throws Exception {
        Files.createFile(db());

        try (Smq smq = Smq.sqlite(db()).open()) {
            assertNotNull(smq.declare("q", QueueOptions.defaults()));
        }
    }

    @Test
    void 지원하는_버전보다_낮은_smq_파일은_업그레이드_틀을_거치되_마이그레이션이_없으면_거부한다() throws Exception {
        int supported = supportedVersion();
        Smq.sqlite(db()).open().close();
        jdbc(db(), "PRAGMA user_version = " + (supported - 1));

        IncompatibleStorageException e = assertThrows(
                IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("found " + (supported - 1)), e.getMessage());
        assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());
        assertEquals(supported - 1, userVersion(db()));
    }

    /** 자식 JVM의 진입점. 시작 신호 파일이 생길 때까지 기다렸다가 열고, 연 인스턴스로 큐 하나를 쓴다. */
    public static final class OpenerMain {
        public static void main(String[] args) throws Exception {
            Path db = Path.of(args[0]);
            Path go = Path.of(args[1]);
            Files.createFile(Path.of(args[1] + ".ready." + args[2])); // 준비됐음을 부모에게 알린다.
            while (!Files.exists(go)) {
                Thread.sleep(1);
            }
            try (Smq smq = Smq.sqlite(db).open()) {
                smq.declare("q", QueueOptions.defaults()).publish(msg(args[2]));
            }
        }
    }

    /**
     * 꺼내기가 빈 결과를 한 번 받고 변경 힌트를 기다리는 자리에 멈출 때까지 기다린다. 고정 시간을 재지 않고
     * 스레드가 힌트의 {@code await}에 있는지를 본다. 그 자리는 한 번 꺼내 본 뒤에만 닿으므로, 이후의 변경은 대기 중에 일어난 것이다.
     */
    private static void awaitParkedInChangeHint(Thread t) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            for (StackTraceElement frame : t.getStackTrace()) {
                if (frame.getClassName().endsWith("ChangeHint")
                        && frame.getMethodName().equals("await")) {
                    return;
                }
            }
            if (!t.isAlive()) {
                fail("꺼내기가 대기에 들어가기 전에 끝났다");
            }
            Thread.sleep(1);
        }
        fail("꺼내기가 10초 안에 변경 힌트 대기에 들어가지 않았다: " + Arrays.toString(t.getStackTrace()));
    }
}
