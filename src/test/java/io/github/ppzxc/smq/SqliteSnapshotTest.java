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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
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

    /** 심볼릭 링크를 못 만드는 환경(권한 없는 Windows 등)에서는 테스트를 건너뛴다. */
    private static Path symlink(Path link, Path target) {
        try {
            return Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            Assumptions.abort("symbolic links are not available: " + e);
            throw new AssertionError(e);
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
        Path alias = symlink(dir.resolveSibling(dir.getFileName() + "-alias"), dir);
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
        Path dangling = symlink(dir.resolve("snap.db"), dir.resolve("nowhere"));
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
        int rounds = 5;
        byte[] big = new byte[16 * 1024];
        AtomicInteger published = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (Smq smq = open()) {
            Queue q = smq.declare("orders", QueueOptions.defaults());
            List<OutgoingMessage> bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);

            // 발행자는 멈추라고 할 때까지 돈다. 스냅샷은 항상 발행이 진행 중일 때 시작한다.
            Thread publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    int progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    assertNull(failure.get());
                    Path target = dir.resolve("snap" + round + ".db");

                    int before = published.get();
                    smq.snapshot(target);
                    int after = published.get();

                    try (Connection c = external(target)) {
                        assertEquals("ok", scalar(c, "PRAGMA integrity_check"));
                        int inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        // 발행자는 하나뿐이라 스냅샷이 반영한 발행 수는, 시작 전에 끝난 수 이상이고 끝난 뒤 센 수에 진행 중이던
                        // 하나를 더한 수 이하다.
                        assertTrue(inSnapshot >= initial + before, "snapshot=" + inSnapshot + " before=" + before);
                        assertTrue(inSnapshot <= initial + after + 1, "snapshot=" + inSnapshot + " after=" + after);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }

            assertNull(failure.get());
            assertEquals(initial + published.get(), q.counts().ready());
        }
    }

    @Test
    void 스냅샷을_뜬_뒤에도_같은_Smq로_발행하고_꺼내고_정산할_수_있다() throws Exception {
        try (Smq smq = open()) {
            Queue q = smq.declare("orders", QueueOptions.defaults());
            q.publish(msg("a"));
            smq.snapshot(dir.resolve("snap.db"));

            q.publish(msg("b"));
            Chunk chunk = q.pull(2, Duration.ZERO);

            assertEquals(2, chunk.messages().size());
            assertTrue(q.settle(chunk.ackAll()).allOk());
            assertEquals(0, q.counts().ready());
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

    /**
     * 스냅샷이 오래 걸리도록 DB를 크게(약 64MB) 만들고, 임시 파일에 쓰기가 시작된 것을 관측하면 close를 부른다. 관측은 최대 5초만
     * 기다리고, 못 해도 거짓 실패하지 않는다. 그때는 불변식만 확인한다. 스냅샷이 성공했다면 완전한 파일이고, 실패했다면
     * 대상도 임시 파일도 남지 않는다. 관측했을 때(경합을 실제로 만들었을 때)는 스냅샷이 반드시 성공해야 한다.
     */
    @Test
    void 스냅샷_도중_다른_스레드가_close를_불러도_스냅샷은_완전한_파일로_끝난다() throws Exception {
        int rows = 4_000;
        byte[] big = new byte[16 * 1024];
        Path target = dir.resolve("snap.db");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        boolean observed;
        Smq smq = open();
        try {
            List<OutgoingMessage> bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);

            Thread snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            // 임시 파일에 쓰기가 시작됐으면 스냅샷이 저장소의 lock을 쥔 채 도는 중이다.
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }

            smq.close();
            // 반환 시점이 아니라 결과로 확인하므로 스레드 스케줄링에 기대지 않는다.
            snapshotter.join(Duration.ofSeconds(30));
            assertFalse(snapshotter.isAlive(), "스냅샷이 끝나지 않았다");
        } finally {
            smq.close();
        }

        if (observed) {
            assertNull(failure.get(), "스냅샷이 도는 중 close를 불러도 스냅샷은 성공해야 한다");
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                assertEquals("ok", scalar(c, "PRAGMA integrity_check"));
                assertEquals(String.valueOf(rows), scalar(c, "SELECT count(*) FROM message"));
            }
            assertEquals(Set.of("smq.db", "smq.db.lock", "snap.db"), withoutWal(names(dir)));
        } else {
            // 스냅샷이 시작되기 전에 close가 이겼다. 거짓 실패가 아니며 흔적이 없어야 한다.
            assertFalse(Files.exists(target));
            assertEquals(Set.of("smq.db", "smq.db.lock"), withoutWal(names(dir)));
        }
    }

    private boolean snapshotInProgress() throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.anyMatch(e -> {
                String name = e.getFileName().toString();
                try {
                    return name.startsWith("smq-snapshot-") && Files.size(e) > 0;
                } catch (IOException gone) {
                    return false;
                }
            });
        }
    }

    private static Set<String> withoutWal(Set<String> names) {
        return names.stream().filter(n -> !n.endsWith("-wal")).collect(Collectors.toSet());
    }

    @Test
    void 닫힌_뒤_스냅샷은_예외가_나고_대상_파일도_임시_파일도_남지_않는다() throws Exception {
        Path target = dir.resolve("snap.db");
        Smq smq = open();
        smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
        smq.close();
        Set<String> before = names(dir);

        assertThrows(Exception.class, () -> smq.snapshot(target));

        assertFalse(Files.exists(target));
        assertEquals(before, names(dir));
    }
}
