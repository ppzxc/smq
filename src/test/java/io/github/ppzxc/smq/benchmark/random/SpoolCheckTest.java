package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 디스크 기록의 조각 검사. 기록을 몇 조각으로 나누든 한 번에 검사한 결과와 같아야 한다. 그렇지 않으면 조각 경계에서 위반을 놓치거나
 * 지어낸다.
 */
class SpoolCheckTest {

    private static final InvariantChecker.Config CONFIG = new InvariantChecker.Config(false, 0);
    private static final long LOAD_START = 1_000;
    private static final long LOAD_END = 2_000;

    @TempDir
    Path dir;

    private final List<Published> pub = new ArrayList<>();
    private final List<Delivery> del = new ArrayList<>();
    private long nextId = 1;
    private long chunk = 10_000;

    private static Tag tag(long seq) {
        return new Tag(9, seq);
    }

    private static Content content(long seq) {
        return new Content(seq * 1_000_003L, Map.of("n", Long.toString(seq)));
    }

    /** 발행하고 한 번에 꺼내 ACK한 메시지. */
    private void good(long seq, long publishedAt, long pulledAt) {
        Tag t = tag(seq);
        pub.add(new Published(t, content(seq), publishedAt, 0));
        del.add(delivery(t, seq, 1, chunk++, 0, pulledAt, Settle.ack(pulledAt + 5)));
    }

    private Delivery delivery(Tag t, long seq, int count, long chunkId, int position, long at, Settle settle) {
        return new Delivery(
                QueueKind.TARGET,
                t,
                nextId++,
                count,
                content(seq),
                new Pull(chunkId, position, at, at + 30_000),
                settle);
    }

    private static List<CountsSample> drained() {
        return List.of(
                new CountsSample(QueueKind.TARGET, true, 0, 0, 0),
                new CountsSample(QueueKind.DEAD_LETTER, true, 0, 0, 0));
    }

    private void spool() throws Exception {
        try (RecordSpool spool = new RecordSpool(dir)) {
            pub.forEach(spool::add);
            del.forEach(spool::add);
        }
    }

    private static List<String> described(List<Violation> vs) {
        return vs.stream().map(RecordsJson::describe).sorted().toList();
    }

    private void assertSameAsWholeCheck(int partitions) throws Exception {
        List<CountsSample> samples = drained();
        List<Violation> whole = InvariantChecker.check(CONFIG, pub, del, samples);
        SpoolCheck.Result r = SpoolCheck.run(dir, CONFIG, samples, LOAD_START, LOAD_END, partitions);
        assertEquals(described(whole), described(r.violations()), "조각 " + partitions + "개");
    }

    @Test
    void 위반이_없는_기록은_조각을_몇_개로_나눠도_위반이_없다() throws Exception {
        for (long s = 0; s < 300; s++) {
            good(s, 1_100, 1_200);
        }
        spool();
        for (int k : new int[] {1, 2, 7, 33}) {
            assertSameAsWholeCheck(k);
            assertTrue(SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, k)
                    .violations()
                    .isEmpty());
        }
    }

    @Test
    void 유실_유령_재전달_청크_순서_위반을_일괄_검사와_같게_찾는다() throws Exception {
        for (long s = 0; s < 120; s++) {
            good(s, 1_100, 1_200);
        }
        // 유실: 발행했지만 한 번도 꺼내지 않았다.
        pub.add(new Published(tag(900), content(900), 1_100, 0));
        // 유령: 발행하지 않은 태그가 전달됐다.
        del.add(delivery(tag(901), 901, 1, 500, 0, 1_300, Settle.ack(1_305)));
        // 중복: ACK가 반영된 뒤 다시 전달됐다.
        Tag dup = tag(902);
        pub.add(new Published(dup, content(902), 1_100, 0));
        del.add(delivery(dup, 902, 1, 501, 0, 1_200, Settle.ack(1_205)));
        del.add(delivery(dup, 902, 2, 502, 0, 1_400, Settle.ack(1_405)));
        // 청크 순서: 한 청크 안에서 메시지 id가 줄어든다. 두 태그가 서로 다른 조각에 가도록 여러 태그를 쓴다.
        for (int i = 0; i < 6; i++) {
            Tag t = tag(950 + i);
            pub.add(new Published(t, content(950 + i), 1_100, 0));
            del.add(new Delivery(
                    QueueKind.TARGET,
                    t,
                    1_000 - i,
                    1,
                    content(950 + i),
                    new Pull(600, i, 1_300, 31_300),
                    Settle.ack(1_305)));
        }
        spool();
        List<Violation> whole = InvariantChecker.check(CONFIG, pub, del, drained());
        assertFalse(whole.isEmpty(), "시나리오가 위반을 만들어야 한다");
        assertTrue(whole.stream().anyMatch(v -> v.invariant() == Invariant.CHUNK_ORDER));
        for (int k : new int[] {1, 2, 5, 16}) {
            assertSameAsWholeCheck(k);
        }
    }

    @Test
    void 부하_구간에_발행한_수와_첫_전달_지연을_센다() throws Exception {
        good(1, 500, 1_100); // 부하 전에 발행: 세지 않는다.
        good(2, 1_100, 1_150); // 지연 50ms
        good(3, 1_500, 1_900); // 지연 400ms
        good(4, 1_950, 2_100); // 꺼낸 시각이 구간 밖: 지연에 넣지 않는다.
        spool();
        SpoolCheck.Result r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertEquals(3, r.publishedInLoad());
        assertEquals(50.0, r.p50Millis());
        assertEquals(400.0, r.p99Millis());
    }

    @Test
    void 같은_메시지_id가_서로_다른_태그로_전달된_유령은_조각이_달라도_찾는다() throws Exception {
        for (long s = 0; s < 60; s++) {
            good(s, 1_100, 1_200);
        }
        // 두 태그가 같은 큐의 같은 메시지 id로 전달됐다. 태그가 다르므로 태그 조각은 서로 다를 수 있다.
        for (int i = 0; i < 20; i++) {
            Tag a = tag(700 + 2 * i);
            Tag b = tag(701 + 2 * i);
            pub.add(new Published(a, content(700 + 2 * i), 1_100, 0));
            pub.add(new Published(b, content(701 + 2 * i), 1_100, 0));
            long id = 5_000 + i;
            del.add(new Delivery(
                    QueueKind.TARGET,
                    a,
                    id,
                    1,
                    content(700 + 2 * i),
                    new Pull(chunk++, 0, 1_300, 31_300),
                    Settle.ack(1_305)));
            del.add(new Delivery(
                    QueueKind.TARGET,
                    b,
                    id,
                    1,
                    content(701 + 2 * i),
                    new Pull(chunk++, 0, 1_300, 31_300),
                    Settle.ack(1_305)));
        }
        spool();
        assertTrue(InvariantChecker.check(CONFIG, pub, del, drained()).stream()
                .anyMatch(v -> v.invariant() == Invariant.GHOST));
        for (int k : new int[] {1, 2, 7, 16}) {
            long ghosts = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, k).violations().stream()
                    .filter(v -> v.invariant() == Invariant.GHOST && v.detail().contains("서로 다른 태그"))
                    .count();
            assertEquals(20, ghosts, "조각 " + k + "개");
        }
    }

    @Test
    void 쓰다_죽어_끝이_잘린_파일이_있어도_앞의_기록은_검사한다() throws Exception {
        for (long s = 0; s < 50; s++) {
            good(s, 1_100, 1_200);
        }
        spool();
        Path last = RecordSpool.deliveriesFiles(dir).getFirst();
        byte[] bytes = Files.readAllBytes(last);
        Files.write(last, java.util.Arrays.copyOf(bytes, bytes.length - 7));
        SpoolCheck.Result r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertFalse(r.violations().isEmpty(), "잘려서 사라진 전달은 유실로 보인다");
        assertTrue(r.violations().stream().allMatch(v -> v.invariant() == Invariant.LOSS));
    }

    @Test
    void 검사가_끝나면_임시_조각_파일을_남기지_않는다() throws Exception {
        good(1, 1_100, 1_200);
        spool();
        SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 4);
        try (Stream<Path> s = Files.list(dir)) {
            assertTrue(s.noneMatch(p -> p.getFileName().toString().equals("check")));
        }
    }

    @Test
    void 위반이_상한을_넘으면_상한까지만_모은다() throws Exception {
        for (long s = 0; s < SpoolCheck.MAX_VIOLATIONS + 200L; s++) {
            pub.add(new Published(tag(s), content(s), 1_100, 0)); // 모두 유실
        }
        spool();
        SpoolCheck.Result r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 2);
        assertEquals(SpoolCheck.MAX_VIOLATIONS, r.violations().size());
    }

    @Test
    void 단일_활성_컨슈머_검사는_조각으로_하지_못해_거부한다() throws Exception {
        good(1, 1_100, 1_200);
        spool();
        assertThrows(
                IllegalArgumentException.class,
                () -> SpoolCheck.run(dir, new InvariantChecker.Config(true, 0), drained(), LOAD_START, LOAD_END));
    }
}
