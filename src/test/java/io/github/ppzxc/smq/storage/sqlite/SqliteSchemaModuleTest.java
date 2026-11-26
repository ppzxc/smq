package io.github.ppzxc.smq.storage.sqlite;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.PolicyException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqliteSchemaModuleTest {

    @TempDir
    Path dir;

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("smq.db"));
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static String query(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            return rs.getString(1);
        }
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "Rollback must run for errors as well as exceptions before the failure is rethrown.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private static int establishInTransaction(SqliteSchema schema, Connection c) throws SQLException {
        exec(c, "BEGIN IMMEDIATE");
        try {
            int version = schema.establish(c);
            exec(c, "COMMIT");
            return version;
        } catch (Throwable t) {
            exec(c, "ROLLBACK");
            throw t;
        }
    }

    private static SqliteSchema production() {
        return new SqliteSchema(1, Map.of());
    }

    private void createV1File() throws SQLException {
        try (Connection c = connect()) {
            establishInTransaction(production(), c);
        }
    }

    @Test
    void 빈_파일이면_v1을_만들고_버전을_돌려준다() throws Exception {
        try (Connection c = connect()) {
            assertEquals(1, establishInTransaction(production(), c));
            assertEquals(String.valueOf(SqliteSchema.APPLICATION_ID), query(c, "PRAGMA application_id"));
            assertEquals("1", query(c, "PRAGMA user_version"));
            assertEquals("0", query(c, "SELECT value FROM meta WHERE key = 'lease_seq'"));
        }
    }

    @Test
    void application_id가_다른_파일은_smq_파일이_아니라고_거부한다() throws Exception {
        try (Connection c = connect()) {
            exec(c, "PRAGMA application_id = 12345");
            IncompatibleStorageException e =
                    assertThrows(IncompatibleStorageException.class, () -> establishInTransaction(production(), c));
            assertTrue(e.getMessage().startsWith("not an smq database"), e.getMessage());
        }
    }

    @Test
    void 버전_1만_지원하면_버전_2_파일을_거부한다() throws Exception {
        SqliteStorage.open(dir.resolve("smq.db"), Durability.PROCESS_CRASH).close();
        try (Connection c = connect()) {
            IncompatibleStorageException e =
                    assertThrows(IncompatibleStorageException.class, () -> establishInTransaction(production(), c));
            assertEquals("unsupported schema version: found 2, supported 1", e.getMessage());
            assertEquals("2", query(c, "PRAGMA user_version"));
        }
    }

    @Test
    void v1_파일을_v2로_올리면_DDL_데이터_변환_버전이_한_트랜잭션에서_일어난다() throws Exception {
        createV1File();
        try (Connection c = connect()) {
            exec(c, "INSERT INTO queue(name) VALUES ('q')");
            SqliteSchema v2 = new SqliteSchema(2, Map.of(1, conn -> {
                exec(conn, "ALTER TABLE queue ADD COLUMN note TEXT");
                exec(conn, "UPDATE queue SET note = upper(name)");
            }));
            assertEquals(2, establishInTransaction(v2, c));
            assertEquals("2", query(c, "PRAGMA user_version"));
            assertEquals("Q", query(c, "SELECT note FROM queue WHERE name = 'q'"));
        }
    }

    @Test
    void v1에서_v3까지_한_번에_차례로_올린다() throws Exception {
        createV1File();
        try (Connection c = connect()) {
            SqliteSchema v3 = new SqliteSchema(
                    3,
                    Map.of(
                            1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"),
                            2,
                                    conn -> {
                                        exec(conn, "CREATE TABLE step_b AS SELECT count(*) AS n FROM step_a");
                                        assertEquals("2", query(conn, "PRAGMA user_version"));
                                    }));
            assertEquals(3, establishInTransaction(v3, c));
            assertEquals("3", query(c, "PRAGMA user_version"));
            assertEquals("0", query(c, "SELECT n FROM step_b"));
        }
    }

    @Test
    void 업그레이드_단계가_중간에_실패하면_파일은_원래_버전으로_남는다() throws Exception {
        createV1File();
        try (Connection c = connect()) {
            SqliteSchema v3 = new SqliteSchema(
                    3,
                    Map.of(
                            1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"),
                            2, conn -> exec(conn, "THIS IS NOT SQL")));
            assertThrows(SQLException.class, () -> establishInTransaction(v3, c));
            assertEquals("1", query(c, "PRAGMA user_version"));
            assertEquals("0", query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'"));
        }
    }

    @Test
    void 중간_단계가_빠진_목록이면_지원하지_않는_버전으로_거부한다() throws Exception {
        createV1File();
        try (Connection c = connect()) {
            SqliteSchema gap = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)")));
            IncompatibleStorageException e =
                    assertThrows(IncompatibleStorageException.class, () -> establishInTransaction(gap, c));
            assertEquals("unsupported schema version: found 1, supported 3", e.getMessage());
            assertEquals("1", query(c, "PRAGMA user_version"));
            assertEquals("0", query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'"));
        }
    }
}
