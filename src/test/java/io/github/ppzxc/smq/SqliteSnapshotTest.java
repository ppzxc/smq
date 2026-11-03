package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@code Smq.snapshot(Path)}를 공개 API로만 검증한다. 스냅샷은 일반 SQLite 연결(외부 도구)로 읽는다. */
@Timeout(60)
class SqliteSnapshotTest {

    @TempDir
    Path dir;

    private Smq open() {
        return Smq.sqlite(dir.resolve("smq.db")).open();
    }

    private static OutgoingMessage msg(String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Connection external(Path file) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + file);
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next());
            return r.getString(1);
        }
    }

    private static List<String> payloads(Connection c) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery("SELECT payload FROM message ORDER BY id")) {
            while (r.next()) {
                out.add(new String(r.getBytes(1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    @Test
    void 스냅샷은_외부_SQLite로_열어_원본과_같은_메시지를_보여_준다() throws Exception {
        Path target = dir.resolve("snap.db");
        try (Smq smq = open()) {
            Queue q = smq.declare("orders", QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));

            smq.snapshot(target);
        }

        try (Connection c = external(target)) {
            assertEquals("ok", scalar(c, "PRAGMA integrity_check"));
            assertEquals(List.of("a", "b", "c"), payloads(c));
            assertEquals("orders", scalar(c, "SELECT name FROM queue"));
        }
    }
}
