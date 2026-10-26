package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * 공개 API만으로 작성한 적합성 스위트. 저장소 팩토리만 하위 클래스가 정한다.
 * 다시 꺼내기 루프가 잘못되어도 무한 반복 대신 실패하도록 시간 제한을 둔다.
 */
@Timeout(60)
abstract class SmqConformanceTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    final MutableClock clock = new MutableClock(T0);
    private final List<Smq> opened = new ArrayList<>();

    /** 저장소별 빌더를 같은 모양으로 다루게 하는 테스트용 어댑터. */
    interface TestBuilder {
        TestBuilder clock(InstantSource clock);

        TestBuilder leaseDuration(Duration leaseDuration);

        Smq open();
    }

    abstract TestBuilder builder(Path dir);

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

    @Test
    void chunk_ack_id로_메시지_하나만_ACK한다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(3, Duration.ZERO);
        Outcomes r = q.settle(c.ack(ids.get(1)));
        assertTrue(r.allOk());
        assertEquals(Set.of(ids.get(1)), r.byMessage().keySet());

        clock.advance(Duration.ofSeconds(10));
        assertEquals(List.of(ids.get(0), ids.get(2)),
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void settle_빌더로_ACK와_NACK을_섞고_ackRest로_마무리한다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));
        Chunk c = q.pull(4, Duration.ZERO);
        Outcomes r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
        assertTrue(r.allOk());
        assertEquals(Set.copyOf(ids), r.byMessage().keySet());

        Chunk next = q.pull(10, Duration.ZERO);
        assertEquals(List.of(ids.get(1)), next.messages().stream().map(Message::id).toList());
        assertEquals(2, next.messages().getFirst().deliveryCount());
    }

    @Test
    void build로_마무리하면_고른_메시지만_정산하고_나머지는_리스에_남는다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(2, Duration.ZERO);
        Outcomes r = q.settle(c.settle().ack(ids.get(0)).build());
        assertEquals(Set.of(ids.get(0)), r.byMessage().keySet());
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
    }

    @Test
    void 지연_없이_NACK한_메시지는_자기_id_자리로_돌아가_다음_꺼내기에서_먼저_나온다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(2, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(ids.get(1)).ackRest()).allOk());
        // 아직 꺼내지 않은 c보다 NACK한 b가 먼저다.
        Chunk next = q.pull(10, Duration.ZERO);
        assertEquals(List.of(ids.get(1), ids.get(2)), next.messages().stream().map(Message::id).toList());
    }

    @Test
    void 정산_빌더는_청크에_없거나_이미_고른_메시지를_거부한다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(1, Duration.ZERO);
        assertThrows(IllegalArgumentException.class, () -> c.settle().ack(ids.get(1)));
        assertThrows(IllegalArgumentException.class, () -> c.settle().ack(ids.get(0)).nack(ids.get(0)));
    }

    @Test
    void 섞인_정산은_원자적이라_중간_상태가_다른_꺼내기에_보이지_않는다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            Chunk c = q.pull(3, Duration.ZERO);
            clock.advance(Duration.ofSeconds(10));
            // 기한이 지난 리스라 3개 모두 준비로 센다. 정산 뒤에는 NACK한 b 하나만 남는다.
            assertEquals(3, q.counts().ready());
            AtomicBoolean done = new AtomicBoolean();
            List<Long> seen = Collections.synchronizedList(new ArrayList<>());
            Thread observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts().ready());
                }
            });
            observer.start();
            Outcomes r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            assertTrue(r.allOk());
            assertTrue(seen.stream().allMatch(n -> n == 3 || n == 1), "torn state: " + seen);
            assertEquals(1, q.counts().ready());
            Chunk drain = q.pull(10, Duration.ZERO);
            assertTrue(q.settle(drain.ackAll()).allOk());
        }
    }

    @Test
    void 같은_Settlement_값으로_다시_settle하면_이미_반영된_메시지는_STALE이다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(2, Duration.ZERO);
        Settlement s = c.settle().ack(ids.get(0)).nack(ids.get(1)).build();
        assertTrue(q.settle(s).allOk());
        Outcomes again = q.settle(s);
        assertEquals(ids, again.stale());
        // NACK한 메시지는 준비 상태로 남아 있고 두 번째 반영이 건드리지 않는다.
        assertEquals(1, q.counts().ready());
    }

    @Test
    void 같은_청크의_두_번째_ackRest는_앞서_NACK한_메시지를_지우지_않는다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(3, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(ids.get(1)).ack(ids.get(0)).build()).allOk());
        Outcomes second = q.settle(c.settle().ackRest());
        assertEquals(List.of(ids.get(0), ids.get(1)), second.stale());
        assertEquals(Outcome.OK, second.byMessage().get(ids.get(2)));
        assertEquals(List.of(ids.get(1)),
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void 지연_없이_NACK되어_준비_상태인_메시지를_같은_리스로_연장해도_STALE이다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(id).build()).allOk());
        assertEquals(List.of(id), q.extend(c, Duration.ofSeconds(10)).outcomes().stale());
        // 연장이 효력이 없으니 메시지는 계속 준비 상태다.
        assertEquals(1, q.counts().ready());
    }

    @Test
    void 정산된_메시지에_같은_리스로_다시_정산하면_NACK도_STALE이다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(id).build()).allOk());
        assertEquals(List.of(id), q.settle(c.settle().nack(id).build()).stale());
        assertEquals(List.of(id), q.settle(c.ack(id)).stale());
    }

    @Test
    void FIFO_순서_그리고_NACK_뒤_원래_자리로_복귀() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        ids = new ArrayList<>(ids);
        ids.addAll(q.publish(List.of(msg("c"), msg("d"))));
        Chunk c = q.pull(3, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(ids.get(0)).ack(ids.get(1)).nack(ids.get(2)).build()).allOk());
        assertEquals(List.of(ids.get(0), ids.get(2), ids.get(3)),
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void withDelay로_발행한_메시지는_지연이_지나야_꺼낼_수_있다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a").withDelay(Duration.ofSeconds(10)));
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofSeconds(9));
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofSeconds(1));
        assertEquals(List.of(id), q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void nack_id_지연은_지연이_지나야_다시_꺼낼_수_있다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(2, Duration.ZERO);
        assertTrue(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5)).ackRest()).allOk());
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(List.of(ids.get(0)), q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void nackRest_지연은_고르지_않은_나머지를_지연_NACK한다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(3, Duration.ZERO);
        assertTrue(q.settle(c.settle().ack(ids.get(0)).nackRest(Duration.ofSeconds(5))).allOk());
        assertTrue(q.pull(10, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(List.of(ids.get(1), ids.get(2)),
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void nackAll_지연은_청크_전체를_지연_NACK한다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b")));
        Chunk c = q.pull(2, Duration.ZERO);
        assertTrue(q.settle(c.nackAll(Duration.ofSeconds(5))).allOk());
        assertTrue(q.pull(10, Duration.ZERO).isEmpty());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(ids, q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void 지연이_끝난_메시지는_자기_id_자리로_들어가_더_큰_id의_준비_메시지보다_먼저_나온다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId delayed = q.publish(msg("a").withDelay(Duration.ofSeconds(5)));
        MessageId nacked = q.publish(msg("b"));
        MessageId ready = q.publish(msg("c"));
        Chunk first = q.pull(1, Duration.ZERO);
        assertEquals(List.of(nacked), first.messages().stream().map(Message::id).toList());
        assertTrue(q.settle(first.settle().nack(nacked, Duration.ofSeconds(5)).build()).allOk());
        assertEquals(List.of(ready), q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(List.of(delayed, nacked),
                q.pull(2, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    void counts는_지연됨_개수를_돌려주고_지연이_끝나면_준비로_센다() throws Exception {
        Queue q = open(Duration.ofSeconds(60)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10)));
        assertEquals(new QueueCounts(3, 0, 1), q.counts());
        Chunk c = q.pull(2, Duration.ZERO);
        assertEquals(new QueueCounts(1, 2, 1), q.counts());
        assertTrue(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5)).ackRest()).allOk());
        assertEquals(new QueueCounts(1, 0, 2), q.counts());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(new QueueCounts(2, 0, 1), q.counts());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(new QueueCounts(3, 0, 0), q.counts());
    }

    @Test
    void 지연과_리스_시간은_양수여야_한다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        for (Duration bad : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1), null}) {
            assertThrows(IllegalArgumentException.class, () -> msg("a").withDelay(bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> c.settle().nack(id, bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> c.settle().nackRest(bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> c.nackAll(bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> q.pull(1, Duration.ZERO, bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> q.extend(c, bad), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> builder(dir).leaseDuration(bad), String.valueOf(bad));
        }
        assertThrows(IllegalArgumentException.class, () -> new OutgoingMessage(new byte[0], Map.of(), null));
        // 잘못된 인자는 아무것도 바꾸지 않는다.
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
    }

    // ---- 꺼내기 대기와 종료 (#29) ----

    private static long millisSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    /** 대기 스레드가 꺼내기에 들어가 기다리기 시작할 시간을 준다. */
    private static void letWaiterBlock() throws InterruptedException {
        Thread.sleep(150);
    }

    @Test
    void 메시지가_있으면_긴_wait에도_바로_돌아온다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        long start = System.nanoTime();
        Chunk c = q.pull(1, Duration.ofSeconds(30));
        assertEquals(1, c.messages().size());
        assertTrue(millisSince(start) < 2000);
    }

    @Test
    void 메시지가_없으면_wait만큼_기다린_뒤_빈_청크를_돌려준다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        long start = System.nanoTime();
        Chunk c = q.pull(1, Duration.ofMillis(300));
        long elapsed = millisSince(start);
        assertTrue(c.isEmpty());
        assertTrue(elapsed >= 290, "elapsed=" + elapsed);
        assertTrue(elapsed < 3000, "elapsed=" + elapsed);
    }

    @Test
    void 같은_프로세스에서_발행하면_기다리던_꺼내기가_곧바로_깨어난다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        var result = new java.util.concurrent.CompletableFuture<Chunk>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        long start = System.nanoTime();
        q.publish(msg("a"));
        Chunk c = result.get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals("a", text(c.messages().getFirst()));
        assertTrue(millisSince(start) < 1000);
    }

    @Test
    void 시계를_앞으로_돌린_효과는_다음_폴링에서_반영되어_지연_메시지가_나온다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        q.publish(msg("later").withDelay(Duration.ofMinutes(10)));
        var result = new java.util.concurrent.CompletableFuture<Chunk>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        assertFalse(result.isDone());
        clock.advance(Duration.ofMinutes(10));
        Chunk c = result.get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals("later", text(c.messages().getFirst()));
    }

    @Test
    void 기다리던_스레드를_interrupt하면_InterruptedException() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var interruptedFlag = new java.util.concurrent.atomic.AtomicBoolean();
        Thread t = Thread.ofPlatform().start(() -> {
            try {
                q.pull(1, Duration.ofSeconds(30));
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedFlag.set(Thread.currentThread().isInterrupted());
        });
        letWaiterBlock();
        long start = System.nanoTime();
        t.interrupt();
        t.join(10_000);
        assertFalse(t.isAlive());
        assertInstanceOf(InterruptedException.class, thrown.get());
        assertTrue(millisSince(start) < 2000);
    }

    @Test
    void Smq를_닫으면_기다리던_꺼내기가_빈_청크를_받고_깨어난다() throws Exception {
        Smq smq = open();
        Queue q = smq.declare("q", QueueOptions.defaults());
        int waiters = 3;
        List<java.util.concurrent.CompletableFuture<Chunk>> results = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            var f = new java.util.concurrent.CompletableFuture<Chunk>();
            results.add(f);
            Thread.ofPlatform().start(() -> {
                try {
                    f.complete(q.pull(1, Duration.ofSeconds(30)));
                } catch (Throwable e) {
                    f.completeExceptionally(e);
                }
            });
        }
        letWaiterBlock();
        long start = System.nanoTime();
        smq.close();
        for (var f : results) {
            assertTrue(f.get(10, java.util.concurrent.TimeUnit.SECONDS).isEmpty());
        }
        assertTrue(millisSince(start) < 2000);
    }

    @Test
    void 닫은_뒤의_꺼내기는_기다리지_않고_빈_청크이고_두_번_닫아도_안전하다() throws Exception {
        Smq smq = open();
        Queue q = smq.declare("q", QueueOptions.defaults());
        smq.close();
        smq.close();
        long start = System.nanoTime();
        assertTrue(q.pull(1, Duration.ofSeconds(30)).isEmpty());
        assertTrue(millisSince(start) < 2000);
    }

    @Test
    void 여러_스레드가_하나의_Smq를_함께_써도_모든_메시지가_정확히_한_번_전달된다() throws Exception {
        Queue q = open().declare("q", QueueOptions.defaults());
        int producers = 4;
        int perProducer = 100;
        int consumers = 4;
        int total = producers * perProducer;
        var seen = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        var duplicates = new java.util.concurrent.atomic.AtomicInteger();
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var delivered = new java.util.concurrent.atomic.AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    while (delivered.get() < total) {
                        Chunk chunk = q.pull(7, Duration.ofMillis(200));
                        for (Message m : chunk.messages()) {
                            if (!seen.add(text(m))) {
                                duplicates.incrementAndGet();
                            }
                        }
                        if (!chunk.isEmpty()) {
                            q.settle(chunk.ackAll());
                            delivered.addAndGet(chunk.messages().size());
                        }
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            }));
        }
        for (int p = 0; p < producers; p++) {
            int id = p;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    for (int i = 0; i < perProducer; i++) {
                        q.publish(msg(id + "-" + i));
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            }));
        }
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse(t.isAlive());
        }
        assertNull(failure.get());
        assertEquals(0, duplicates.get());
        assertEquals(total, seen.size());
        assertEquals(new QueueCounts(0, 0, 0), q.counts());
    }

    @Test
    void reject한_메시지는_dead_letter_큐로_옮겨지고_payload와_헤더를_유지하며_smq_헤더를_갖는다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("orders.dead", QueueOptions.defaults());
        Queue q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        MessageId id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        Chunk c = q.pull(1, Duration.ZERO);
        assertEquals(2, c.messages().getFirst().deliveryCount());
        clock.advance(Duration.ofSeconds(5));

        assertTrue(q.settle(c.settle().reject(id).build()).allOk());

        assertEquals(new QueueCounts(0, 0, 0), q.counts());
        assertEquals(new QueueCounts(1, 0, 0), dead.counts());
        Message moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertEquals("poison", text(moved));
        assertNotEquals(id, moved.id());
        assertEquals(1, moved.deliveryCount());
        assertEquals(Map.of("trace", "t-1",
                "smq-original-queue", "orders",
                "smq-original-message-id", String.valueOf(id.value()),
                "smq-last-delivery-count", "2",
                "smq-moved-at", T0.plusSeconds(5).toString(),
                "smq-reason", "rejected"), moved.headers());
    }

    @Test
    void reject한_메시지의_이전_리스로_다시_정산하면_STALE이다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        MessageId id = q.publish(msg("a"));
        Chunk c = q.pull(1, Duration.ZERO);
        assertTrue(q.settle(c.settle().reject(id).build()).allOk());
        assertEquals(Outcome.STALE, q.settle(c.settle().reject(id).build()).byMessage().get(id));
        assertEquals(new QueueCounts(1, 0, 0), smq.queue("dead").counts());
    }

    @Test
    void dead_letter_큐가_없는_큐에서_reject하면_NoDeadLetterQueueException이고_섞인_정산_전체가_반영되지_않는다() throws Exception {
        Queue q = open(Duration.ofSeconds(10)).declare("q", QueueOptions.defaults());
        List<MessageId> ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Chunk c = q.pull(3, Duration.ZERO);

        NoDeadLetterQueueException e = assertThrows(NoDeadLetterQueueException.class,
                () -> q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        assertEquals("q", e.queue());

        assertEquals(new QueueCounts(0, 3, 0), q.counts());
        clock.advance(Duration.ofSeconds(10));
        Chunk again = q.pull(10, Duration.ZERO);
        assertEquals(ids, again.messages().stream().map(Message::id).toList());
        assertTrue(again.messages().stream().allMatch(m -> m.deliveryCount() == 2));
    }

    @Test
    void dead_letter_큐는_평범한_큐라_꺼내고_정산하고_연쇄로_dead_letter를_둘_수_있다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue last = smq.declare("last", QueueOptions.defaults());
        Queue middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        Queue q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        MessageId id = q.publish(msg("a"));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());

        Chunk inMiddle = middle.pull(1, Duration.ZERO);
        Message m = inMiddle.messages().getFirst();
        assertEquals("q", m.headers().get("smq-original-queue"));
        assertTrue(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());

        Chunk lastChunk = last.pull(1, Duration.ZERO);
        Message end = lastChunk.messages().getFirst();
        assertEquals("a", text(end));
        assertEquals("middle", end.headers().get("smq-original-queue"));
        assertEquals(String.valueOf(m.id().value()), end.headers().get("smq-original-message-id"));
        assertEquals("rejected", end.headers().get("smq-reason"));

        assertThrows(NoDeadLetterQueueException.class,
                () -> last.settle(lastChunk.settle().reject(end.id()).build()));
        assertTrue(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    void smq_접두사는_예약이라_같은_이름의_사용자_헤더는_시스템_값으로_덮어써진다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        MessageId id = q.publish(msg("a").withHeaders(Map.of("smq-reason", "mine", "trace", "t-1")));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());

        Message moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertEquals("rejected", moved.headers().get("smq-reason"));
        assertEquals("t-1", moved.headers().get("trace"));
    }

    @Test
    void 최대_전달_횟수에_도달한_메시지는_다시_꺼낼_때_dead_letter_큐로_옮겨지고_꺼내기는_다음_메시지로_진행한다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        MessageId a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        MessageId b = q.publish(msg("b"));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        assertTrue(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        clock.advance(Duration.ofSeconds(3));

        Chunk c = q.pull(1, Duration.ZERO);

        assertEquals(List.of(b), c.messages().stream().map(Message::id).toList());
        assertEquals(1, c.messages().getFirst().deliveryCount());
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
        assertEquals(new QueueCounts(1, 0, 0), dead.counts());
        Message moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertEquals("a", text(moved));
        assertEquals(Map.of("trace", "t-1",
                "smq-original-queue", "q",
                "smq-original-message-id", String.valueOf(a.value()),
                "smq-last-delivery-count", "2",
                "smq-moved-at", T0.plusSeconds(3).toString(),
                "smq-reason", "max-deliveries"), moved.headers());
    }

    @Test
    void 리스가_만료된_채_최대_전달_횟수에_도달한_메시지도_다음_꺼내기에서_옮겨진다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        assertEquals(1, q.pull(1, Duration.ZERO).messages().size());
        clock.advance(Duration.ofSeconds(10));

        assertTrue(q.pull(1, Duration.ZERO).isEmpty());

        assertEquals(new QueueCounts(0, 0, 0), q.counts());
        assertEquals(new QueueCounts(1, 0, 0), dead.counts());
    }

    @Test
    void 마지막_전달에서_ACK하면_정상적으로_성공하고_옮겨지지_않는다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        MessageId a = q.publish(msg("a"));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        Chunk last = q.pull(1, Duration.ZERO);
        assertEquals(2, last.messages().getFirst().deliveryCount());
        clock.advance(Duration.ofSeconds(20));

        assertTrue(q.settle(last.ackAll()).allOk());

        assertEquals(new QueueCounts(0, 0, 0), q.counts());
        assertEquals(new QueueCounts(0, 0, 0), dead.counts());
    }

    @Test
    void 마지막_전달에서_지연_NACK한_메시지는_지연이_끝나야_옮겨지고_그동안_지연됨으로_센다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5))).allOk());

        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        assertEquals(new QueueCounts(0, 0, 1), q.counts());
        assertEquals(new QueueCounts(0, 0, 0), dead.counts());

        clock.advance(Duration.ofSeconds(5));
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        assertEquals(new QueueCounts(0, 0, 0), q.counts());
        assertEquals(new QueueCounts(1, 0, 0), dead.counts());
    }

    @Test
    void dead_letter가_한_번에_옮기는_상한보다_많이_쌓여_있어도_pull_한_번이_남은_준비_메시지를_돌려준다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        int poison = 1000;
        List<OutgoingMessage> batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        Chunk first = q.pull(poison, Duration.ZERO);
        assertEquals(poison, first.messages().size());
        assertTrue(q.settle(first.nackAll(Duration.ofSeconds(1))).allOk());
        MessageId good = q.publish(msg("good"));
        clock.advance(Duration.ofSeconds(1));

        Chunk c = q.pull(1, Duration.ZERO);

        assertEquals(List.of(good), c.messages().stream().map(Message::id).toList());
        assertTrue(q.pull(1, Duration.ZERO).isEmpty());
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
        assertEquals(new QueueCounts(poison, 0, 0), dead.counts());
    }

    @Test
    void 상한보다_많이_쌓인_dead_letter_뒤에_준비_메시지가_없으면_pull은_다_옮기고_빈_청크를_돌려준다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        int poison = 1000;
        List<OutgoingMessage> batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        assertEquals(poison, q.pull(poison, Duration.ZERO).messages().size());
        clock.advance(Duration.ofSeconds(10));

        assertTrue(q.pull(1, Duration.ZERO).isEmpty());

        assertEquals(new QueueCounts(0, 0, 0), q.counts());
        assertEquals(new QueueCounts(poison, 0, 0), dead.counts());
    }

    @Test
    void 청크가_차도_뒤쪽의_도달_메시지는_같은_꺼내기에서_옮겨진다() throws Exception {
        Smq smq = open(Duration.ofSeconds(10));
        Queue dead = smq.declare("dead", QueueOptions.defaults());
        Queue q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        MessageId normal = q.publish(OutgoingMessage.of("normal".getBytes(StandardCharsets.UTF_8))
                .withDelay(Duration.ofSeconds(5)));
        q.publish(msg("poison"));
        assertTrue(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(1))).allOk());
        clock.advance(Duration.ofSeconds(5));

        Chunk c = q.pull(1, Duration.ZERO);

        assertEquals(List.of(normal), c.messages().stream().map(Message::id).toList());
        assertEquals(new QueueCounts(0, 1, 0), q.counts());
        assertEquals(new QueueCounts(1, 0, 0), dead.counts());
    }
}
