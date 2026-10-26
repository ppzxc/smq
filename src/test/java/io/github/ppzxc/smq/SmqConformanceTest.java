package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
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

    @Test
    void 같은_옵션으로_여러_번_선언해도_아무_일이_없다() throws Exception {
        Smq smq = open();
        smq.declare("orders.dead", QueueOptions.defaults());
        QueueOptions options = QueueOptions.defaults().maxDeliveries(5, "orders.dead");
        Queue first = smq.declare("orders", options);
        first.publish(msg("a"));
        Queue again = smq.declare("orders", options);
        assertEquals(options, again.options());
        assertEquals(options, smq.queue("orders").options());
        assertEquals(1, again.counts().ready());
    }

    @Test
    void 이미_있는_큐를_다른_옵션으로_선언하면_기존과_요청_옵션을_담은_QueueDeclarationConflictException() {
        Smq smq = open();
        smq.declare("dead", QueueOptions.defaults());
        QueueOptions existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        QueueOptions requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        QueueDeclarationConflictException e =
                assertThrows(QueueDeclarationConflictException.class, () -> smq.declare("orders", requested));
        assertInstanceOf(SmqException.class, e);
        assertEquals("orders", e.queue());
        assertEquals(existing, e.existing());
        assertEquals(requested, e.requested());
        assertEquals(existing, smq.queue("orders").options());
    }

    @Test
    void maxDeliveries는_dead_letter_큐_없이_지정할_수_없고_지정하지_않으면_무제한이다() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueueOptions(OptionalInt.of(3), Optional.empty(), false));
        assertThrows(IllegalArgumentException.class, () -> QueueOptions.defaults().maxDeliveries(0, "dead"));
        assertTrue(QueueOptions.defaults().maxDeliveries().isEmpty());
        assertTrue(QueueOptions.defaults().deadLetterQueue("dead").maxDeliveries().isEmpty());
        Smq smq = open();
        smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("dead"));
        assertTrue(q.options().maxDeliveries().isEmpty());
        assertEquals(Optional.of("dead"), smq.queue("orders").options().deadLetterQueue());
    }

    @Test
    void 자기_자신을_dead_letter_큐로_지정하면_오류이고_없는_큐를_지정하면_선언_오류다() {
        Smq smq = open();
        assertThrows(IllegalArgumentException.class,
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        assertThrows(IllegalArgumentException.class,
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        QueueNotFoundException e = assertThrows(QueueNotFoundException.class,
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        assertEquals("missing", e.queue());
        assertThrows(QueueNotFoundException.class, () -> smq.queue("orders"));
    }

    @Test
    void singleActiveConsumer가_저장되어_options와_재선언_비교에_반영된다() {
        Smq smq = open();
        QueueOptions single = QueueOptions.defaults().singleActiveConsumer(true);
        Queue q = smq.declare("ordered", single);
        assertTrue(q.options().singleActiveConsumer());
        assertTrue(smq.queue("ordered").options().singleActiveConsumer());
        assertEquals(single, smq.declare("ordered", single).options());
        QueueDeclarationConflictException e = assertThrows(QueueDeclarationConflictException.class,
                () -> smq.declare("ordered", QueueOptions.defaults()));
        assertEquals(single, e.existing());
        assertFalse(smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer());
        assertThrows(QueueDeclarationConflictException.class, () -> smq.declare("plain", single));
    }

    @Test
    void delete는_메시지째_지우고_지웠으면_true_없었으면_false() {
        Smq smq = open();
        Queue q = smq.declare("orders", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        assertTrue(smq.delete("orders"));
        assertFalse(smq.delete("orders"));
        assertFalse(smq.delete("never-declared"));
        assertThrows(QueueNotFoundException.class, () -> smq.queue("orders"));
        assertThrows(IllegalArgumentException.class, () -> smq.delete("bad name"));
        assertEquals(0, smq.declare("orders", QueueOptions.defaults()).counts().ready());
    }

    @Test
    void 다른_큐가_dead_letter_큐로_참조하는_큐를_지우면_QueueInUseException() {
        Smq smq = open();
        smq.declare("dead", QueueOptions.defaults());
        smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "dead"));
        QueueInUseException e = assertThrows(QueueInUseException.class, () -> smq.delete("dead"));
        assertInstanceOf(SmqException.class, e);
        assertEquals("dead", e.queue());
        assertEquals("orders", e.referencedBy());
        assertEquals("dead", smq.queue("dead").name());
        assertTrue(smq.delete("orders"));
        assertTrue(smq.delete("dead"));
    }

    @Test
    void dead_letter_큐_연쇄에서도_참조_중인_큐는_지울_수_없다() {
        Smq smq = open();
        smq.declare("c", QueueOptions.defaults());
        smq.declare("b", QueueOptions.defaults().deadLetterQueue("c"));
        smq.declare("a", QueueOptions.defaults().deadLetterQueue("b"));
        assertThrows(QueueInUseException.class, () -> smq.delete("c"));
        assertThrows(QueueInUseException.class, () -> smq.delete("b"));
        assertTrue(smq.delete("a"));
        assertTrue(smq.delete("b"));
        assertTrue(smq.delete("c"));
    }

    @Test
    void 삭제한_큐의_핸들로_발행하거나_꺼내면_QueueNotFoundException() {
        Smq smq = open();
        Queue q = smq.declare("orders", QueueOptions.defaults());
        smq.delete("orders");
        assertThrows(QueueNotFoundException.class, () -> q.publish(msg("a")));
        assertThrows(QueueNotFoundException.class, () -> q.pull(1, Duration.ZERO));
        assertThrows(QueueNotFoundException.class, q::counts);
    }

    @Test
    void 삭제한_큐의_리스로_들어온_정산은_STALE이고_같은_이름으로_다시_선언한_큐는_새_큐다() throws Exception {
        Smq smq = open();
        Queue old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        Chunk oldChunk = old.pull(10, Duration.ZERO);
        assertEquals(2, oldChunk.messages().size());
        smq.delete("orders");

        Outcomes afterDelete = old.settle(oldChunk.ackAll());
        for (Message m : oldChunk.messages()) {
            assertEquals(Outcome.STALE, afterDelete.byMessage().get(m.id()));
        }

        Queue fresh = smq.declare("orders", QueueOptions.defaults());
        assertEquals(new QueueCounts(0, 0, 0), fresh.counts());
        fresh.publish(msg("new-1"));
        Chunk freshChunk = fresh.pull(10, Duration.ZERO);
        assertEquals("new-1", text(freshChunk.messages().getFirst()));

        Outcomes stale = fresh.settle(oldChunk.ackAll());
        for (Message m : oldChunk.messages()) {
            assertEquals(Outcome.STALE, stale.byMessage().get(m.id()));
        }
        assertEquals(new QueueCounts(0, 1, 0), fresh.counts());
        assertEquals(Outcome.OK, fresh.settle(freshChunk.ackAll()).byMessage().get(freshChunk.messages().getFirst().id()));
    }

    @Test
    void 리스가_만료된_메시지는_원래_id_자리로_돌아가_다시_꺼내지고_전달_횟수가_늘어난다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk first = q.pull(2, Duration.ZERO);
        assertEquals(List.of(1, 1), first.messages().stream().map(Message::deliveryCount).toList());

        clock.advance(Duration.ofSeconds(10));
        Chunk second = q.pull(10, Duration.ZERO);
        assertEquals(ids, second.messages().stream().map(Message::id).toList());
        assertEquals(List.of(2, 2, 1), second.messages().stream().map(Message::deliveryCount).toList());

        clock.advance(Duration.ofSeconds(10));
        Chunk third = q.pull(10, Duration.ZERO);
        assertEquals(List.of(3, 3, 2), third.messages().stream().map(Message::deliveryCount).toList());
    }

    @Test
    void 리스_기한_전에는_다시_꺼낼_수_없다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO);
        clock.advance(Duration.ofSeconds(9));
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
    }

    @Test
    void counts는_리스_중_개수를_돌려주고_기한이_지났지만_다시_꺼내지_않은_메시지는_준비로_센다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        assertEquals(new QueueCounts(1, 2, 0), q.counts());
        clock.advance(Duration.ofSeconds(10));
        assertEquals(new QueueCounts(3, 0, 0), q.counts());
        q.pull(1, Duration.ZERO);
        assertEquals(new QueueCounts(2, 1, 0), q.counts());
    }

    @Test
    void pull에서_덮어쓴_리스_시간이_청크의_기한과_만료_시점에_반영된다() throws Exception {
        Queue q = open(Duration.ofSeconds(60)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO, Duration.ofSeconds(5));
        assertEquals(T0.plusSeconds(5), c.leaseDeadline());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(2, q.pull(1, Duration.ZERO).messages().getFirst().deliveryCount());
    }

    @Test
    void extend는_리스를_지금부터_d_뒤로_바꾸고_새_기한과_메시지별_OK를_돌려준다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(10, Duration.ZERO);
        clock.advance(Duration.ofSeconds(6));
        Extension ext = q.extend(c, Duration.ofSeconds(20));
        assertEquals(T0.plusSeconds(26), ext.leaseDeadline());
        assertTrue(ext.outcomes().allOk());
        assertEquals(Set.copyOf(ids), ext.outcomes().byMessage().keySet());

        clock.advance(Duration.ofSeconds(19));
        assertTrue(q.pull(10, Duration.ZERO).isEmpty());
        assertEquals(new QueueCounts(0, 2, 0), q.counts());
        clock.advance(Duration.ofSeconds(1));
        assertEquals(2, q.pull(10, Duration.ZERO).messages().size());
    }

    @Test
    void 같은_연장을_다시_해도_안전하다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(10, Duration.ZERO);
        Extension first = q.extend(c, Duration.ofSeconds(20));
        Extension again = q.extend(c, Duration.ofSeconds(20));
        assertEquals(first, again);
        assertTrue(again.outcomes().allOk());
        assertEquals(new QueueCounts(0, 2, 0), q.counts());
        assertTrue(q.settle(c.ackAll()).allOk());
    }

    @Test
    void 기한이_지났어도_아무도_다시_꺼내지_않았다면_원래_리스의_정산과_연장은_OK다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(10, Duration.ZERO);
        clock.advance(Duration.ofSeconds(30));
        Extension ext = q.extend(c, Duration.ofSeconds(10));
        assertTrue(ext.outcomes().allOk());
        assertEquals(new QueueCounts(0, 2, 0), q.counts());
        clock.advance(Duration.ofSeconds(30));
        assertTrue(q.settle(c.ackAll()).allOk());
        assertEquals(new QueueCounts(0, 0, 0), q.counts());
    }

    @Test
    void 다른_컨슈머가_다시_꺼간_메시지에_대한_늦은_정산과_연장은_STALE이고_효력이_없다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk slow = q.pull(10, Duration.ZERO);
        clock.advance(Duration.ofSeconds(10));
        Chunk fast = q.pull(1, Duration.ZERO);
        assertEquals(ids.getFirst(), fast.messages().getFirst().id());

        Outcomes late = q.settle(slow.ackAll());
        assertEquals(Outcome.STALE, late.byMessage().get(ids.getFirst()));
        assertEquals(Outcome.OK, late.byMessage().get(ids.get(1)));

        Extension ext = q.extend(slow, Duration.ofSeconds(100));
        assertEquals(List.of(ids.getFirst(), ids.get(1)), ext.outcomes().stale());
        // 연장은 효력이 없으므로 fast의 리스는 그대로이고 기한이 되면 만료된다.
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
        assertTrue(q.settle(fast.ackAll()).allOk());
    }

    @Test
    void ACK한_메시지를_같은_리스로_다시_연장하면_STALE이다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        assertTrue(q.settle(c.ackAll()).allOk());
        Extension ext = q.extend(c, Duration.ofSeconds(10));
        assertEquals(List.of(id), ext.outcomes().stale());
    }

    @Test
    void 빈_청크를_연장하면_빈_결과이고_인자_오류는_IllegalArgumentException() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        Chunk empty = q.pull(1, Duration.ZERO);
        assertTrue(q.extend(empty, Duration.ofSeconds(1)).outcomes().byMessage().isEmpty());
        q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        assertThrows(IllegalArgumentException.class, () -> q.extend(c, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> q.extend(c, null));
        assertThrows(NullPointerException.class, () -> q.extend(null, Duration.ofSeconds(1)));
    }
}
