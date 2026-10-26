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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db); Statement s = c.createStatement()) {
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

        IncompatibleStorageException e = assertThrows(IncompatibleStorageException.class,
                () -> Smq.sqlite(db()).open());

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
        jdbc(db(), "PRAGMA application_id = 12345");

        assertThrows(IncompatibleStorageException.class, () -> Smq.sqlite(db()).open());
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

        IncompatibleStorageException e = assertThrows(IncompatibleStorageException.class,
                () -> Smq.sqlite(db()).open());

        assertTrue(e.getMessage().contains("found " + (supported - 1)), e.getMessage());
        assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());
        assertEquals(supported - 1, userVersion(db()));
    }

    @Test
    void 여러_스레드가_동시에_같은_새_파일을_열어도_모두_성공하고_스키마가_한_번만_만들어진다() throws Exception {
        int threads = 8;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Smq>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    return Smq.sqlite(db()).open();
                }));
            }
            List<Smq> opened = new ArrayList<>();
            for (Future<Smq> f : futures) {
                opened.add(f.get(30, TimeUnit.SECONDS));
            }
            try {
                opened.getFirst().declare("q", QueueOptions.defaults()).publish(msg("a"));
                for (Smq smq : opened) {
                    assertEquals(1, smq.queue("q").counts().ready());
                }
            } finally {
                opened.forEach(Smq::close);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** 자식 JVM의 진입점. 시작 신호 파일이 생길 때까지 기다렸다가 열고, 연 인스턴스로 큐 하나를 쓴다. */
    public static final class OpenerMain {
        public static void main(String[] args) throws Exception {
            Path db = Path.of(args[0]);
            Path go = Path.of(args[1]);
            while (!Files.exists(go)) {
                Thread.sleep(1);
            }
            try (Smq smq = Smq.sqlite(db).open()) {
                smq.declare("q", QueueOptions.defaults()).publish(msg(args[2]));
            }
        }
    }

    @Test
    void 여러_프로세스가_동시에_같은_새_파일을_열어도_모두_성공하고_스키마가_한_번만_만들어진다() throws Exception {
        int processes = 4;
        Path go = dir.resolve("go");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<Process> children = new ArrayList<>();
        try {
            for (int i = 0; i < processes; i++) {
                children.add(new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                        OpenerMain.class.getName(), db().toString(), go.toString(), "m" + i)
                        .redirectErrorStream(true)
                        .redirectOutput(dir.resolve("child" + i + ".log").toFile())
                        .start());
            }
            Thread.sleep(1500); // 모두 신호를 기다리는 상태가 되게 한다.
            Files.createFile(go);
            for (int i = 0; i < processes; i++) {
                Process child = children.get(i);
                assertTrue(child.waitFor(60, TimeUnit.SECONDS), "child " + i + " did not finish");
                assertEquals(0, child.exitValue(), Files.readString(dir.resolve("child" + i + ".log")));
            }
        } finally {
            children.forEach(Process::destroyForcibly);
        }
        try (Smq smq = Smq.sqlite(db()).open()) {
            assertEquals(processes, smq.queue("q").counts().ready());
        }
    }

    @Test
    void 열린_뒤_다른_쪽에서_버전이_바뀌면_다음_호출이_실패하고_그_인스턴스는_계속_실패한다() throws Exception {
        int supported = supportedVersion();
        try (Smq smq = Smq.sqlite(db()).open()) {
            Queue q = smq.declare("q", QueueOptions.defaults());
            q.publish(msg("a"));

            jdbc(db(), "PRAGMA user_version = " + (supported + 1));

            IncompatibleStorageException e = assertThrows(IncompatibleStorageException.class, q::counts);
            assertTrue(e.getMessage().contains("found " + (supported + 1)), e.getMessage());
            assertTrue(e.getMessage().contains("supported " + supported), e.getMessage());

            // 버전이 되돌아와도 그 인스턴스는 영구히 실패한 상태다.
            jdbc(db(), "PRAGMA user_version = " + supported);
            assertThrows(IncompatibleStorageException.class, () -> q.publish(msg("b")));
            assertThrows(IncompatibleStorageException.class, () -> smq.declare("other", QueueOptions.defaults()));
            assertThrows(IncompatibleStorageException.class, () -> q.pull(1, Duration.ZERO));
        }
    }

    @Test
    void 꺼내기_대기_중에_버전이_바뀌면_예외가_pull_밖으로_전파된다() throws Exception {
        int supported = supportedVersion();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Smq smq = Smq.sqlite(db()).open()) {
            Queue q = smq.declare("q", QueueOptions.defaults());
            Future<Chunk> waiting = pool.submit(() -> q.pull(1, Duration.ofSeconds(30)));
            Thread.sleep(200);

            jdbc(db(), "PRAGMA user_version = " + (supported + 1));

            ExecutionException e = assertThrows(ExecutionException.class, () -> waiting.get(10, TimeUnit.SECONDS));
            assertInstanceOf(IncompatibleStorageException.class, e.getCause());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void 멈춘_인스턴스가_쥐고_있던_리스는_다른_인스턴스에서_만료_뒤_재전달된다() throws Exception {
        int supported = supportedVersion();
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        MessageId id;
        try (Smq stale = Smq.sqlite(db()).clock(clock).open()) {
            Queue q = stale.declare("q", QueueOptions.defaults());
            id = q.publish(msg("a"));
            Chunk lost = q.pull(1, Duration.ZERO, Duration.ofSeconds(10));

            jdbc(db(), "PRAGMA user_version = " + (supported + 1));
            assertThrows(IncompatibleStorageException.class, () -> q.settle(lost.ackAll()));
        }
        // 같은 스키마로 쓰는 다른 인스턴스를 버전을 되돌려 흉내 낸다.
        jdbc(db(), "PRAGMA user_version = " + supported);
        try (Smq other = Smq.sqlite(db()).clock(clock).open()) {
            Queue q = other.queue("q");
            assertTrue(q.pull(1, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(10));
            Chunk again = q.pull(1, Duration.ZERO);
            assertEquals(id, again.messages().getFirst().id());
            assertEquals(2, again.messages().getFirst().deliveryCount());
            assertTrue(q.settle(again.ackAll()).allOk());
        }
    }
}
