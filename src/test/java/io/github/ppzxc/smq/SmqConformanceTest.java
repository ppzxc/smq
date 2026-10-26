package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 공개 API만으로 작성한 적합성 스위트. 저장소 팩토리만 하위 클래스가 정한다. */
abstract class SmqConformanceTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    final MutableClock clock = new MutableClock(T0);
    private final List<Smq> opened = new ArrayList<>();

    abstract Smq.Builder builder(Path dir);

    Smq open(Duration lease) {
        Smq smq = builder(dir).clock(clock).leaseDuration(lease).open();
        opened.add(smq);
        return smq;
    }

    Smq open() {
        Smq smq = builder(dir).clock(clock).open();
        opened.add(smq);
        return smq;
    }

    @AfterEach
    void closeAll() {
        opened.forEach(Smq::close);
    }

    static OutgoingMessage msg(String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    static String text(Message m) {
        return new String(m.payload(), StandardCharsets.UTF_8);
    }

    @Test
    void 열고_try_with_resources로_닫을_수_있다() {
        try (Smq smq = builder(dir).open()) {
            assertNotNull(smq);
        }
    }

    @Test
    void 선언한_큐를_queue로_얻고_없으면_QueueNotFoundException() {
        Smq smq = open();
        Queue declared = smq.declare("orders", QueueOptions.defaults());
        assertEquals("orders", declared.name());
        assertEquals("orders", smq.queue("orders").name());
        QueueNotFoundException e = assertThrows(QueueNotFoundException.class, () -> smq.queue("missing"));
        assertInstanceOf(SmqException.class, e);
    }

    @Test
    void 발행하면_id_목록을_돌려주고_단건_발행도_된다() {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        assertEquals(3, ids.size());
        assertEquals(ids.stream().sorted().toList(), ids);
        assertEquals(3, ids.stream().distinct().count());
        MessageId single = q.publish(msg("d"));
        assertTrue(single.compareTo(ids.getLast()) > 0);
    }

    @Test
    void payload와_헤더가_그대로_전달된다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        byte[] payload = {0, 1, 2, (byte) 0xff, 0};
        Map<String, String> headers = Map.of("k", "v", "한글", "값", "empty", "");
        q.publish(OutgoingMessage.of(payload).withHeaders(headers));
        Message m = q.pull(1, Duration.ZERO).messages().getFirst();
        assertArrayEquals(payload, m.payload());
        assertEquals(headers, m.headers());
    }

    @Test
    void pull은_id_순서로_최대_max개를_돌려주고_메타데이터를_담는다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(2, Duration.ZERO);
        assertEquals(2, c.messages().size());
        assertEquals(ids.subList(0, 2), c.messages().stream().map(Message::id).toList());
        assertEquals("a", text(c.messages().get(0)));
        assertEquals(1, c.messages().get(0).deliveryCount());
        assertEquals(T0, c.messages().get(0).publishedAt());
        assertEquals("q", c.queue());
    }

    @Test
    void 메시지가_없으면_null이나_예외가_아니라_빈_청크() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        Chunk c = q.pull(10, Duration.ZERO);
        assertNotNull(c);
        assertTrue(c.messages().isEmpty());
        assertTrue(q.settle(c.ackAll()).byMessage().isEmpty());
    }

    @Test
    void wait_동안_기다리다_도착한_메시지를_돌려준다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        Thread publisher = Thread.ofPlatform().start(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            q.publish(msg("late"));
        });
        Chunk c = q.pull(1, Duration.ofSeconds(5));
        publisher.join();
        assertEquals(1, c.messages().size());
    }

    @Test
    void wait가_지나면_빈_청크이고_시계가_멈춰_있어도_돌아온다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        long start = System.nanoTime();
        Chunk c = q.pull(1, Duration.ofMillis(50));
        assertTrue(c.messages().isEmpty());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 2000);
    }

    @Test
    void 리스_기한은_주입한_시계와_리스_시간으로_계산된다() throws Exception {
        Queue q = open(Duration.ofSeconds(45)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        assertEquals(T0.plusSeconds(45), q.pull(1, Duration.ZERO).leaseDeadline());
        q.publish(msg("b"));
        assertEquals(T0.plusSeconds(7), q.pull(1, Duration.ZERO, Duration.ofSeconds(7)).leaseDeadline());
    }

    @Test
    void 기본_리스_시간은_30초() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        assertEquals(T0.plusSeconds(30), q.pull(1, Duration.ZERO).leaseDeadline());
    }

    @Test
    void ackAll은_메시지를_삭제하고_OK를_돌려준다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(10, Duration.ZERO);
        Outcomes r = q.settle(c.ackAll());
        assertTrue(r.allOk());
        assertEquals(ids.size(), r.byMessage().size());
        assertEquals(0, q.counts().ready());
        assertTrue(q.pull(10, Duration.ZERO).messages().isEmpty());
    }

    @Test
    void 같은_정산을_다시_하면_STALE() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        Settlement s = c.ackAll();
        assertTrue(q.settle(s).allOk());
        Outcomes again = q.settle(s);
        assertFalse(again.allOk());
        assertEquals(List.of(id), again.stale());
    }

    @Test
    void counts는_준비_메시지_개수를_돌려준다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        assertEquals(0, q.counts().ready());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        assertEquals(3, q.counts().ready());
        q.pull(2, Duration.ZERO);
        assertEquals(1, q.counts().ready());
    }

    @Test
    void 큐마다_메시지가_분리된다() throws Exception {
        Smq smq = open();
        Queue a = smq.declare("a", QueueOptions.defaults());
        Queue b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        assertEquals(0, b.counts().ready());
        assertTrue(b.pull(1, Duration.ZERO).messages().isEmpty());
        assertEquals(1, a.pull(1, Duration.ZERO).messages().size());
    }

    @Test
    void 발행_배치는_원자적이다_중간에_잘못된_원소가_있으면_하나도_들어가지_않는다() {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<OutgoingMessage> batch = new ArrayList<>();
        batch.add(msg("a"));
        batch.add(null);
        batch.add(msg("c"));
        assertThrows(NullPointerException.class, () -> q.publish(batch));
        assertEquals(0, q.counts().ready());
    }

    @Test
    void FIFO_순서는_발행_순서와_배치_경계를_넘어_유지된다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("1"), msg("2")));
        q.publish(msg("3"));
        q.publish(List.of(msg("4"), msg("5")));
        List<String> got = new ArrayList<>();
        for (Message m : q.pull(2, Duration.ZERO).messages()) got.add(text(m));
        for (Message m : q.pull(10, Duration.ZERO).messages()) got.add(text(m));
        assertEquals(List.of("1", "2", "3", "4", "5"), got);
    }

    @Test
    void 인자_오류는_IllegalArgumentException() {
        Smq smq = open();
        for (String bad : new String[] {"", "has space", "a/b", "x".repeat(201)}) {
            assertThrows(IllegalArgumentException.class, () -> smq.declare(bad, QueueOptions.defaults()), bad);
            assertThrows(IllegalArgumentException.class, () -> smq.queue(bad), bad);
        }
        Queue q = smq.declare("q", QueueOptions.defaults());
        assertThrows(IllegalArgumentException.class, () -> q.pull(0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> q.pull(-1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> q.pull(1, Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> q.pull(1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> smq.queue("q").pull(1, Duration.ZERO, Duration.ofSeconds(-1)));
    }

    @Test
    void 예외_계층의_뿌리는_unchecked_SmqException() {
        assertTrue(RuntimeException.class.isAssignableFrom(SmqException.class));
        assertTrue(SmqException.class.isAssignableFrom(QueueNotFoundException.class));
    }
}
