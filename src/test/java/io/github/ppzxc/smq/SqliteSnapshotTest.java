package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    private Set<String> names(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(e -> e.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    @Test
    void 대상이_이미_있으면_예외를_던지고_기존_파일과_디렉터리를_그대로_둔다() throws Exception {
        Path target = dir.resolve("snap.db");
        byte[] existing = "not touched".getBytes(StandardCharsets.UTF_8);
        Files.write(target, existing);
        try (Smq smq = open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            Set<String> before = names(dir);

            assertThrows(SmqStorageException.class, () -> smq.snapshot(target));

            assertArrayEquals(existing, Files.readAllBytes(target));
            assertEquals(before, names(dir));
        }
    }

    @Test
    void 대상이_실행_중인_DB와_WAL과_락_파일과_겹치면_어떤_경로로_가리켜도_거부한다() throws Exception {
        Path alias = Files.createSymbolicLink(dir.resolveSibling(dir.getFileName() + "-alias"), dir);
        try (Smq smq = open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            Set<String> before = names(dir);

            for (String name : List.of("smq.db", "smq.db-wal", "smq.db.lock")) {
                assertThrows(IllegalArgumentException.class, () -> smq.snapshot(dir.resolve(name)), name);
                assertThrows(IllegalArgumentException.class, () -> smq.snapshot(alias.resolve(name)), name);
            }
            assertThrows(IllegalArgumentException.class, () -> smq.snapshot(dir.resolve("./smq.db")));

            assertEquals(before, names(dir));
        } finally {
            Files.delete(alias);
        }
    }

    @Test
    void 대상의_부모_디렉터리가_없으면_예외를_던지고_디렉터리를_만들지_않는다() throws Exception {
        Path missing = dir.resolve("no-such-dir");
        try (Smq smq = open()) {
            smq.declare("orders", QueueOptions.defaults());

            assertThrows(SmqStorageException.class, () -> smq.snapshot(missing.resolve("snap.db")));

            assertFalse(Files.exists(missing));
        }
    }

    @Test
    void 임시_파일을_만든_뒤_연결에_실패하면_임시_파일을_남기지_않는다() throws Exception {
        // 끊어진 심볼릭 링크는 Files.exists가 false라 사전 검사를 통과하지만 link(2)는 EEXIST로 실패한다.
        Path dangling = Files.createSymbolicLink(dir.resolve("snap.db"), dir.resolve("nowhere"));
        try (Smq smq = open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            Set<String> before = names(dir);

            assertThrows(SmqStorageException.class, () -> smq.snapshot(dangling));

            assertEquals(before, names(dir));
            assertEquals(dir.resolve("nowhere"), Files.readSymbolicLink(dangling));
        }
    }

    @Test
    void 스냅샷을_뜨는_동안_발행이_이어져도_스냅샷은_일관되고_모든_발행이_끝내_성공한다() throws Exception {
        int initial = 1_000;
        int concurrent = 300;
        byte[] big = new byte[16 * 1024];
        Path target = dir.resolve("snap.db");
        AtomicInteger published = new AtomicInteger();
        CountDownLatch warmedUp = new CountDownLatch(1);
        try (Smq smq = open()) {
            Queue q = smq.declare("orders", QueueOptions.defaults());
            List<OutgoingMessage> bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);

            Thread publisher = new Thread(() -> {
                for (int i = 0; i < concurrent; i++) {
                    q.publish(msg("p" + i));
                    published.incrementAndGet();
                    warmedUp.countDown();
                }
            });
            AtomicReference<Throwable> failure = new AtomicReference<>();
            publisher.setUncaughtExceptionHandler((t, e) -> failure.set(e));
            publisher.start();
            warmedUp.await();

            int before = published.get();
            smq.snapshot(target);
            int after = published.get();
            publisher.join();

            assertNull(failure.get());
            assertEquals(concurrent, published.get());
            assertEquals(initial + concurrent, q.counts().ready());
            try (Connection c = external(target)) {
                assertEquals("ok", scalar(c, "PRAGMA integrity_check"));
                int inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                // 발행은 하나뿐이라 스냅샷이 반영한 발행 수는, 시작 전에 끝난 수 이상이고 끝난 뒤 센 수에 진행 중이던 하나를 더한 수 이하다.
                assertTrue(inSnapshot >= initial + before, "snapshot=" + inSnapshot + " before=" + before);
                assertTrue(inSnapshot <= initial + after + 1, "snapshot=" + inSnapshot + " after=" + after);
            }
        }
    }

    @Test
    void 대상_디렉터리_이름에_작은따옴표가_있어도_스냅샷을_만든다() throws Exception {
        Path quoted = Files.createDirectory(dir.resolve("it's here"));
        Path target = quoted.resolve("snap's.db");
        try (Smq smq = open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));

            smq.snapshot(target);
        }

        try (Connection c = external(target)) {
            assertEquals(List.of("a"), payloads(c));
        }
    }
}
