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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    private static Tag tag(final long seq) {
        return new Tag(9, seq);
    }

    private static Content content(final long seq) {
        return new Content(seq * 1_000_003L, Map.of("n", Long.toString(seq)));
    }

    private void good(final long seq, final long publishedAt, final long pulledAt) {
        final Tag t;
        t = tag(seq);
        pub.add(new Published(t, content(seq), publishedAt, 0));
        del.add(delivery(t, seq, 1, chunk, 0, pulledAt, Settle.ack(pulledAt + 5)));
        chunk++;
    }

    private Delivery delivery(
            final Tag t,
            final long seq,
            final int count,
            final long chunkId,
            final int position,
            final long at,
            final Settle settle) {
        final long id;
        id = nextId;
        nextId++;
        return new Delivery(
                QueueKind.TARGET, t, id, count, content(seq), new Pull(chunkId, position, at, at + 30_000), settle);
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

    private static List<String> described(final List<Violation> vs) {
        return vs.stream().map(RecordsJson::describe).sorted().toList();
    }

    private void assertSameAsWholeCheck(final int partitions) throws Exception {
        final List<CountsSample> samples;
        final List<Violation> whole;
        final SpoolCheck.Result r;
        samples = drained();
        whole = InvariantChecker.findViolations(CONFIG, pub, del, samples);
        r = SpoolCheck.run(dir, CONFIG, samples, LOAD_START, LOAD_END, partitions);
        assertEquals(
                described(whole), described(r.violations()), "Partitioned violations should match the whole check.");
    }

    private void prepareValidRecords() throws Exception {
        for (long s = 0; s < 300; s++) {
            good(s, 1_100, 1_200);
        }
        spool();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 33})
    @DisplayName("위반이 없는 기록은 조각을 몇 개로 나눠도 위반이 없다")
    void validRecordsMatchWholeCheck(final int partitions) throws Exception {
        prepareValidRecords();
        assertSameAsWholeCheck(partitions);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 33})
    @DisplayName("위반이 없는 기록은 조각을 몇 개로 나눠도 위반이 없다")
    void validRecordsHaveNoViolations(final int partitions) throws Exception {
        prepareValidRecords();
        assertTrue(
                SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, partitions)
                        .violations()
                        .isEmpty(),
                "Valid records should have no violations.");
    }

    private void prepareViolationScenario() throws Exception {
        final Tag dup;
        for (long s = 0; s < 120; s++) {
            good(s, 1_100, 1_200);
        }

        pub.add(new Published(tag(900), content(900), 1_100, 0));

        del.add(delivery(tag(901), 901, 1, 500, 0, 1_300, Settle.ack(1_305)));

        dup = tag(902);
        pub.add(new Published(dup, content(902), 1_100, 0));
        del.add(delivery(dup, 902, 1, 501, 0, 1_200, Settle.ack(1_205)));
        del.add(delivery(dup, 902, 2, 502, 0, 1_400, Settle.ack(1_405)));

        for (int i = 0; i < 6; i++) {
            final Tag t;
            t = tag(950 + i);
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
    }

    @Test
    @DisplayName("유실 유령 재전달 청크 순서 시나리오에서 위반을 찾는다")
    void violationScenarioProducesViolations() throws Exception {
        prepareViolationScenario();
        assertFalse(
                InvariantChecker.findViolations(CONFIG, pub, del, drained()).isEmpty(),
                "The scenario should produce violations.");
    }

    @Test
    @DisplayName("청크 순서 위반을 찾는다")
    void violationScenarioIncludesChunkOrder() throws Exception {
        prepareViolationScenario();
        assertTrue(
                InvariantChecker.findViolations(CONFIG, pub, del, drained()).stream()
                        .anyMatch(v -> v.invariant() == Invariant.CHUNK_ORDER),
                "The expected invariant violation should be reported.");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 16})
    @DisplayName("유실 유령 재전달 청크 순서 위반을 일괄 검사와 같게 찾는다")
    void partitionedViolationsMatchWholeCheck(final int partitions) throws Exception {
        prepareViolationScenario();
        assertSameAsWholeCheck(partitions);
    }

    @Test
    @DisplayName("부하 구간에 발행한 수와 첫 전달 지연을 센다 — counts Published Messages In Load")
    void countsPublishedMessagesInLoad() throws Exception {
        final SpoolCheck.Result r;
        good(1, 500, 1_100);
        good(2, 1_100, 1_150);
        good(3, 1_500, 1_900);
        good(4, 1_950, 2_100);
        spool();
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertEquals(3, r.publishedInLoad(), "Published in load should match the expected value.");
    }

    @Test
    @DisplayName("부하 구간에 발행한 수와 첫 전달 지연을 센다 — measures Median Delivery Latency")
    void measuresMedianDeliveryLatency() throws Exception {
        final SpoolCheck.Result r;
        good(1, 500, 1_100);
        good(2, 1_100, 1_150);
        good(3, 1_500, 1_900);
        good(4, 1_950, 2_100);
        spool();
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertEquals(50.0, r.p50Millis(), "P50 millis should match the expected value.");
    }

    @Test
    @DisplayName("부하 구간에 발행한 수와 첫 전달 지연을 센다 — measures P99Delivery Latency")
    void measuresP99DeliveryLatency() throws Exception {
        final SpoolCheck.Result r;
        good(1, 500, 1_100);
        good(2, 1_100, 1_150);
        good(3, 1_500, 1_900);
        good(4, 1_950, 2_100);
        spool();
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertEquals(400.0, r.p99Millis(), "P99 millis should match the expected value.");
    }

    private void prepareDifferentTagGhosts() throws Exception {
        for (long s = 0; s < 60; s++) {
            good(s, 1_100, 1_200);
        }

        for (int i = 0; i < 20; i++) {
            final Tag a;
            final Tag b;
            final long id;
            a = tag(700 + 2 * i);
            b = tag(701 + 2 * i);
            pub.add(new Published(a, content(700 + 2 * i), 1_100, 0));
            pub.add(new Published(b, content(701 + 2 * i), 1_100, 0));
            id = 5_000L + i;
            del.add(new Delivery(
                    QueueKind.TARGET,
                    a,
                    id,
                    1,
                    content(700 + 2 * i),
                    new Pull(chunk, 0, 1_300, 31_300),
                    Settle.ack(1_305)));
            chunk++;
            del.add(new Delivery(
                    QueueKind.TARGET,
                    b,
                    id,
                    1,
                    content(701 + 2 * i),
                    new Pull(chunk, 0, 1_300, 31_300),
                    Settle.ack(1_305)));
            chunk++;
        }
        spool();
    }

    @Test
    @DisplayName("같은 메시지 id가 서로 다른 태그로 전달된 유령을 일괄 검사에서 찾는다")
    void wholeCheckFindsDifferentTagGhosts() throws Exception {
        prepareDifferentTagGhosts();
        assertTrue(
                InvariantChecker.findViolations(CONFIG, pub, del, drained()).stream()
                        .anyMatch(v -> v.invariant() == Invariant.GHOST),
                "The expected invariant violation should be reported.");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 16})
    @DisplayName("같은 메시지 id가 서로 다른 태그로 전달된 유령은 조각이 달라도 찾는다")
    void partitionedCheckFindsDifferentTagGhosts(final int partitions) throws Exception {
        final long ghosts;
        prepareDifferentTagGhosts();
        ghosts = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, partitions).violations().stream()
                .filter(v -> v.invariant() == Invariant.GHOST && v.detail().contains("서로 다른 태그"))
                .count();
        assertEquals(20, ghosts, "Partitioned check should find all different-tag ghosts.");
    }

    @Test
    @DisplayName("쓰다 죽어 끝이 잘린 파일이 있어도 앞의 기록은 검사한다 — truncated Delivery File Produces Violations")
    void truncatedDeliveryFileProducesViolations() throws Exception {
        final Path last;
        final byte[] bytes;
        final SpoolCheck.Result r;
        for (long s = 0; s < 50; s++) {
            good(s, 1_100, 1_200);
        }
        spool();
        last = RecordSpool.deliveriesFiles(dir).getFirst();
        bytes = Files.readAllBytes(last);
        Files.write(last, java.util.Arrays.copyOf(bytes, bytes.length - 7));
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertFalse(r.violations().isEmpty(), "잘려서 사라진 전달은 유실로 보인다");
    }

    @Test
    @DisplayName("쓰다 죽어 끝이 잘린 파일이 있어도 앞의 기록은 검사한다 — truncated Delivery File Produces Only Loss Violations")
    void truncatedDeliveryFileProducesOnlyLossViolations() throws Exception {
        final Path last;
        final byte[] bytes;
        final SpoolCheck.Result r;
        for (long s = 0; s < 50; s++) {
            good(s, 1_100, 1_200);
        }
        spool();
        last = RecordSpool.deliveriesFiles(dir).getFirst();
        bytes = Files.readAllBytes(last);
        Files.write(last, java.util.Arrays.copyOf(bytes, bytes.length - 7));
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 3);
        assertTrue(
                r.violations().stream().allMatch(v -> v.invariant() == Invariant.LOSS),
                "All violations should report message loss.");
    }

    @Test
    @DisplayName("검사가 끝나면 임시 조각 파일을 남기지 않는다")
    void onceTheScanIsCompleteNoTemporaryFragmentFilesAreLeftBehind() throws Exception {
        good(1, 1_100, 1_200);
        spool();
        SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 4);
        try (Stream<Path> s = Files.list(dir)) {
            assertTrue(
                    s.noneMatch(p -> p.getFileName().toString().equals("check")),
                    "Temporary check files should be removed.");
        }
    }

    @Test
    @DisplayName("위반이 상한을 넘으면 상한까지만 모은다")
    void ifTheNumberOfViolationsExceedsTheUpperLimitOnlyCollectUpToTheUpperLimit() throws Exception {
        final SpoolCheck.Result r;
        for (long s = 0; s < SpoolCheck.MAX_VIOLATIONS + 200L; s++) {
            pub.add(new Published(tag(s), content(s), 1_100, 0));
        }
        spool();
        r = SpoolCheck.run(dir, CONFIG, drained(), LOAD_START, LOAD_END, 2);
        assertEquals(SpoolCheck.MAX_VIOLATIONS, r.violations().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 검사는 조각으로 하지 못해 거부한다")
    void theSingleActiveConsumerCheckIsRejectedBecauseItCannotBePerformedInParts() throws Exception {
        good(1, 1_100, 1_200);
        spool();
        assertThrows(
                IllegalArgumentException.class,
                () -> SpoolCheck.run(dir, new InvariantChecker.Config(true, 0), drained(), LOAD_START, LOAD_END),
                "Run should throw IllegalArgumentException.");
    }
}
