package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.storage.sqlite.SqliteStorageTestSupport;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.InstantSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 다른 프로세스가 쓰기 락을 쥘 때 호출이 얼마나 막히는지 고정한다(#68). 꺼내기는 wait 안에서만 막히고,
 * 그 밖의 연산과 close()는 재시도가 끝날 때까지 직렬로 막힌다(ADR-0009). 다른 프로세스는 같은 파일을 연 별도 연결로
 * 흉내 낸다. 시도 5회, busy timeout 300ms면 쓰기 한 번이 BUSY로 실패하기까지 약 1.6초다.
 */
class SqliteBusyWaitTest {

    private static final int BUSY_TIMEOUT_MILLIS = 300;
    private static final int MAX_ATTEMPTS = 5;
    /** 쓰기 한 번이 BUSY로 포기하기까지의 하한(시도 5회 x 300ms). 여유를 두고 1초로 잡는다. */
    private static final long BLOCKED_AT_LEAST_MILLIS = 1000;

    @TempDir
    Path dir;

    private Smq smq;
    private Queue queue;
    private Connection other;

    @BeforeEach
    void setUp() throws Exception {
        open(null);
    }

    /** 하한을 정해 다시 연다. null이면 기본값(100ms)이다. */
    private void open(Duration lockWaitFloor) throws Exception {
        if (smq != null) {
            smq.close();
            other.close();
        }
        Path db = dir.resolve("smq.db");
        smq = new DefaultSmq(
                lockWaitFloor == null
                        ? SqliteStorageTestSupport.open(db, BUSY_TIMEOUT_MILLIS, MAX_ATTEMPTS)
                        : SqliteStorageTestSupport.open(db, BUSY_TIMEOUT_MILLIS, MAX_ATTEMPTS, lockWaitFloor),
                InstantSource.system(),
                Duration.ofSeconds(30));
        queue = smq.declare("q", QueueOptions.defaults());
        if (queue.counts().ready() == 0) {
            queue.publish(OutgoingMessage.of(new byte[] {1}));
        }
        other = DriverManager.getConnection("jdbc:sqlite:" + db);
    }

    @AfterEach
    void tearDown() throws Exception {
        releaseOther();
        other.close();
        smq.close();
    }

    private void holdWriteLock() throws SQLException {
        try (Statement s = other.createStatement()) {
            s.execute("BEGIN IMMEDIATE");
        }
    }

    private void releaseOther() {
        try (Statement s = other.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // 이미 끝났다.
        }
    }

    private static long millisSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    /** 다른 스레드가 쓰기를 시도해 lock을 쥔 채 재시도하게 하고, 그 스레드가 BUSY 대기에 들어갈 때까지 기다린다. */
    private Thread startBlockedWriter(AtomicReference<Throwable> failure) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            started.countDown();
            try {
                queue.publish(OutgoingMessage.of(new byte[] {2}));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        writer.start();
        started.await();
        Thread.sleep(100); // 첫 BUSY 대기(300ms) 안으로 들어가게 한다.
        return writer;
    }

    @Test
    void 쓰기_락을_다른_연결이_쥐면_꺼내기는_wait가_지나면_빈_청크로_돌아온다() throws Exception {
        holdWriteLock();

        long start = System.nanoTime();
        Chunk chunk = queue.pull(1, Duration.ofMillis(50));

        assertTrue(chunk.isEmpty());
        // 마감은 첫 시도 뒤에 확인하므로 최악은 wait에 busy timeout 한 번을 더한 값이다.
        assertTrue(millisSince(start) < BUSY_TIMEOUT_MILLIS + 400, "elapsed=" + millisSince(start));
    }

    @Test
    void wait가_넉넉하면_BUSY_재시도_소진은_여전히_예외다() throws Exception {
        holdWriteLock();

        long start = System.nanoTime();
        assertThrows(SmqStorageException.class, () -> queue.pull(1, Duration.ofSeconds(30)));

        assertTrue(millisSince(start) >= BLOCKED_AT_LEAST_MILLIS, "elapsed=" + millisSince(start));
    }

    @Test
    void 경합이_없으면_wait가_0이어도_꺼낸다() throws Exception {
        Chunk chunk = queue.pull(1, Duration.ZERO);

        assertEquals(1, chunk.messages().size());
    }

    @Test
    void 락을_쥔_쪽이_놓으면_wait_안에_꺼낸다() throws Exception {
        holdWriteLock();
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                return;
            }
            releaseOther();
        });
        releaser.start();

        Chunk chunk = queue.pull(1, Duration.ofSeconds(10));

        assertEquals(1, chunk.messages().size());
        releaser.join();
    }

    @Test
    void 같은_프로세스의_쓰기가_재시도_중이면_꺼내기는_wait_안에_lock을_못_잡고_빈_청크로_돌아온다() throws Exception {
        holdWriteLock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = startBlockedWriter(failure);

        long start = System.nanoTime();
        Chunk chunk = queue.pull(1, Duration.ofMillis(50));

        assertTrue(chunk.isEmpty());
        assertTrue(millisSince(start) < 300, "elapsed=" + millisSince(start));
        writer.join();
        assertInstanceOf(SmqStorageException.class, failure.get());
    }

    @Test
    void 같은_프로세스의_쓰기가_재시도_중이면_close는_lock을_기다린다() throws Exception {
        holdWriteLock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = startBlockedWriter(failure);

        long start = System.nanoTime();
        smq.close();

        assertTrue(millisSince(start) >= BLOCKED_AT_LEAST_MILLIS - 200, "elapsed=" + millisSince(start));
        writer.join();
    }

    @Test
    void 같은_프로세스의_쓰기가_재시도_중이면_다른_쓰기도_lock을_기다린다() throws Exception {
        holdWriteLock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = startBlockedWriter(failure);

        long start = System.nanoTime();
        assertThrows(SmqStorageException.class, () -> queue.publish(OutgoingMessage.of(new byte[] {3})));

        // 앞선 쓰기의 재시도가 끝나야 이 쓰기가 lock을 잡고, 그다음 자기 재시도를 한 번 더 한다.
        assertTrue(millisSince(start) >= BLOCKED_AT_LEAST_MILLIS, "elapsed=" + millisSince(start));
        writer.join();
    }

    @Test
    void lock_대기_하한이_0이면_재시도_중인_쓰기_뒤에서_wait만큼만_기다리고_빈_청크로_돌아온다() throws Exception {
        open(Duration.ZERO);
        holdWriteLock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = startBlockedWriter(failure);

        long start = System.nanoTime();
        Chunk chunk = queue.pull(1, Duration.ZERO);

        assertTrue(chunk.isEmpty());
        assertTrue(millisSince(start) < 50, "elapsed=" + millisSince(start));
        writer.join();
    }

    @Test
    void lock_대기_하한을_늘리면_wait가_짧아도_그만큼_lock을_기다린다() throws Exception {
        open(Duration.ofSeconds(5));
        holdWriteLock();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = startBlockedWriter(failure);

        long start = System.nanoTime();
        Chunk chunk = queue.pull(1, Duration.ZERO);

        // 쓰기 스레드가 포기해 lock을 놓을 때까지 기다린 뒤, 다른 연결이 쥔 락 때문에 마감이 지나 빈 청크다.
        assertTrue(chunk.isEmpty());
        assertTrue(millisSince(start) >= BLOCKED_AT_LEAST_MILLIS - 200, "elapsed=" + millisSince(start));
        writer.join();
    }

    @Test
    void lock_대기_하한은_음수일_수_없다() {
        assertThrows(IllegalArgumentException.class,
                () -> Smq.sqlite(dir.resolve("neg.db")).lockWaitFloor(Duration.ofMillis(-1)));
    }
}
