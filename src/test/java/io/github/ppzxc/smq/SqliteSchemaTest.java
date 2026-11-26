package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        jdbc(db(), "PRAGMA user_version = 0");

        IncompatibleStorageException e = assertThrows(
                IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("found 0"), e.getMessage());
        assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());
        assertEquals(0, userVersion(db()));
    }

    @Test
    void 버전_1_견본을_열면_버전_2로_올라간다() throws Exception {
        Path copy = StorageFactory.copyV1Sample(db());
        assertEquals(1, userVersion(copy));
        Smq.sqlite(copy).open().close();
        assertEquals(2, userVersion(copy));
    }

    @Test
    void v1_변환이_실패하면_롤백되어_다시_올릴_수_있다() throws Exception {
        Path copy = StorageFactory.copyV1Sample(db());
        jdbc(copy, "UPDATE message SET available_at = lease_deadline WHERE lease_token IS NOT NULL", """
                CREATE TRIGGER fail_upgrade BEFORE UPDATE OF available_at ON message
                WHEN OLD.lease_token IS NOT NULL
                BEGIN SELECT RAISE(ABORT, 'injected upgrade failure'); END
                """);

        assertThrows(SmqStorageException.class, () -> Smq.sqlite(copy).open());
        assertEquals(1, userVersion(copy));

        jdbc(copy, "DROP TRIGGER fail_upgrade");
        Smq.sqlite(copy).open().close();
        assertEquals(2, userVersion(copy));
    }
}
