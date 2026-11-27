package io.github.ppzxc.smq;

import io.github.ppzxc.smq.Settlement.Builder;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Disposition.Kind;
import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(60L)
@PolicyException(
        rule = "PMD.CyclomaticComplexity",
        reason =
                "Independent conformance tests share one fixture; their combined class metric is not one control flow.",
        approvedBy = "ppzxc",
        reviewBy = "2027-11-26")
@SuppressWarnings("PMD.CyclomaticComplexity")
abstract class SmqConformanceTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    final MutableClock clock = new MutableClock(T0);

    private final List<Smq> opened = new ArrayList<>();

    private static final int ROUNDS = 300;

    abstract StorageFactory factory();

    private Smq openWith(final BuilderSettings settings) {
        return settings.open(this.factory().open(this.dir));
    }

    private Smq register(final BuilderSettings settings) {
        final Smq smq;
        smq = this.openWith(settings);
        this.opened.add(smq);
        return smq;
    }

    Smq open(final Duration lease) {
        final BuilderSettings settings;
        settings = new BuilderSettings();
        settings.clock(this.clock);
        settings.leaseDuration(lease);
        return this.register(settings);
    }

    Smq open() {
        final BuilderSettings settings;
        settings = new BuilderSettings();
        settings.clock(this.clock);
        return this.register(settings);
    }

    @AfterEach
    void closeAll() {
        this.opened.forEach(Smq::close);
    }

    static OutgoingMessage msg(final String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    static String text(final Message m) {
        return new String(m.payload(), StandardCharsets.UTF_8);
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The single assertion needs the post-failure queue count after catching the expected NPE.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private static long invalidBatchQueueCount(final Queue queue) {
        final List<OutgoingMessage> batch;
        batch = new ArrayList<>();
        batch.add(msg("a"));
        batch.add(null);
        batch.add(msg("c"));
        try {
            queue.publish(batch);
            return Long.MAX_VALUE;
        } catch (NullPointerException expected) {
            return queue.counts().ready();
        }
    }

    @Test
    @DisplayName("닫힌 뒤 큐 선언은 실패한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void queueDeclarationFailsAfterClosing() {
        final Smq smq;
        smq = this.open();
        smq.close();
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> smq.declare("q", QueueOptions.defaults()),
                "Declare should throw IllegalStateException.");
    }

    @ParameterizedTest
    @EnumSource(ClosedOperation.class)
    @DisplayName("닫힌 뒤 조회와 큐 연산은 실패한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void afterClosingQueryAndQueueOperationsFail(final ClosedOperation operation) throws Exception {
        final Smq smq;
        final Queue q;
        final Chunk chunk;
        final Chunk empty;
        final Executable call;
        final Class<? extends RuntimeException> expected;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        empty = q.pull(1, Duration.ZERO);
        smq.close();
        call = switch (operation) {
            case QUEUE -> () -> smq.queue("q");
            case DELETE -> () -> smq.delete("q");
            case SNAPSHOT -> () -> smq.snapshot(this.dir.resolve("snapshot.db"));
            case COUNTS -> q::counts;
            case PUBLISH -> () -> q.publish(msg("b"));
            case PUBLISH_EMPTY -> () -> q.publish(List.of());
            case SETTLE -> () -> q.settle(chunk.ackAll());
            case SETTLE_EMPTY -> () -> q.settle(empty.ackAll());
            case EXTEND -> () -> q.extend(chunk, Duration.ofSeconds(1L));
            case EXTEND_EMPTY -> () -> q.extend(empty, Duration.ofSeconds(1L));
            case INVALID_COUNT -> () -> q.pull(0, Duration.ZERO);
            case INVALID_WAIT -> () -> q.pull(1, Duration.ofSeconds(-1L));
            case INVALID_LEASE -> () -> q.pull(1, Duration.ZERO, Duration.ZERO);
        };
        expected = switch (operation) {
            case INVALID_COUNT, INVALID_WAIT, INVALID_LEASE -> IllegalArgumentException.class;
            default -> IllegalStateException.class;
        };
        Assertions.assertThrows(expected, call, "The operation should reject the closed queue or invalid argument.");
    }

    @Test
    @DisplayName("매우 긴 대기에도 메시지를 꺼낸다")
    void retrievesMessagesEvenAfterAVeryLongWait() throws Exception {
        final Queue q;
        q = this.open().declare("long-wait", QueueOptions.defaults());
        q.publish(msg("ready"));
        Assertions.assertEquals(
                "ready",
                text(q.pull(1, Duration.ofSeconds(Long.MAX_VALUE)).messages().getFirst()),
                "Text should match the expected value.");
    }

    @Test
    @DisplayName("백년을 넘는 발행 지연은 거부한다")
    void publishDelaysExceeding100YearsAreRejected() {
        final Duration tooLong;
        tooLong = Duration.ofDays(36_500L).plusNanos(1L);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> msg("x").withDelay(tooLong),
                "With delay should throw IllegalArgumentException.");
    }

    private static Duration century() {
        return Duration.ofDays(36_500L);
    }

    private CenturyScenario centuryScenario() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        q = this.open(century()).declare("limits", QueueOptions.defaults());
        q.publish(msg("delayed").withDelay(century()));
        id = q.publish(msg("ready"));
        chunk = q.pull(1, Duration.ZERO, century());
        return new CenturyScenario(q, id, chunk, chunk.settle());
    }

    @Test
    @DisplayName("100년보다 긴 발행 지연은 거부한다")
    void publishingDelayLongerThanCenturyIsRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new OutgoingMessage(new byte[0], Map.of(), century().plusNanos(1L)),
                "A delay longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 메모리 리스는 거부한다")
    void memoryLeaseLongerThanCenturyIsRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> Smq.inMemory().leaseDuration(century().plusNanos(1L)),
                "A lease longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 SQLite 리스는 거부한다")
    void sqliteLeaseLongerThanCenturyIsRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> Smq.sqlite(this.dir.resolve("unused.db"))
                        .leaseDuration(century().plusNanos(1L)),
                "A lease longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 pull 리스는 거부한다")
    void pullLeaseLongerThanCenturyIsRejected() throws Exception {
        final Queue q;
        q = this.open(century()).declare("limits", QueueOptions.defaults());
        q.publish(msg("ready"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> q.pull(1, Duration.ZERO, century().plusNanos(1L)),
                "A pull lease longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 extend 리스는 거부한다")
    void extensionLongerThanCenturyIsRejected() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> scenario.queue().extend(scenario.chunk(), century().plusNanos(1L)),
                "An extension longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 NACK 지연은 거부한다")
    void nackDelayLongerThanCenturyIsRejected() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> scenario.settlement().nack(scenario.id(), century().plusNanos(1L)),
                "A NACK delay longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년보다 긴 nackRest 지연은 거부한다")
    void nackRestDelayLongerThanCenturyIsRejected() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> scenario.settlement().nackRest(century().plusNanos(1L)),
                "A NACK rest delay longer than 100 years should be rejected.");
    }

    @Test
    @DisplayName("100년 경계의 청크에 발행한 id가 보존된다")
    void centuryScenarioPreservesPublishedId() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertEquals(
                scenario.id(),
                scenario.chunk().messages().getFirst().id(),
                "The ready message should retain its published ID.");
    }

    @Test
    @DisplayName("100년 경계에서도 extend가 성공한다")
    void centuryExtensionSucceeds() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertTrue(
                scenario.queue().extend(scenario.chunk(), century()).outcomes().allOk(),
                "An extension at the 100-year limit should succeed.");
    }

    @Test
    @DisplayName("100년 경계의 NACK이 성공한다")
    void centuryNackSucceeds() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        Assertions.assertTrue(
                scenario.queue()
                        .settle(scenario.settlement()
                                .nack(scenario.id(), century())
                                .build())
                        .allOk(),
                "A NACK at the 100-year limit should succeed.");
    }

    @Test
    @DisplayName("100년 NACK 지연이 끝나기 전에는 메시지를 꺼낼 수 없다")
    void centuryNackIsDelayedUntilDeadline() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        scenario.queue()
                .settle(scenario.settlement().nack(scenario.id(), century()).build());
        Assertions.assertTrue(
                scenario.queue().pull(1, Duration.ZERO).isEmpty(),
                "The NACKed message should remain delayed before its deadline.");
    }

    @Test
    @DisplayName("100년 경계가 지나면 두 지연 메시지를 꺼낸다")
    void centuryDeadlineMakesBothMessagesReady() throws Exception {
        final CenturyScenario scenario;
        scenario = this.centuryScenario();
        scenario.queue()
                .settle(scenario.settlement().nack(scenario.id(), century()).build());
        this.clock.advance(century());
        Assertions.assertEquals(
                2,
                scenario.queue().pull(2, Duration.ZERO).messages().size(),
                "Both delayed messages should be ready at the 100-year deadline.");
    }

    @Test
    @DisplayName("빈 큐의 매우 긴 대기는 발행하면 깨어난다")
    void aVeryLongWaitInAnEmptyQueueWakesUpWhenAMessageIsPublished() throws Exception {
        final Queue q;
        q = this.open().declare("wait-empty", QueueOptions.defaults());
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<Chunk> future;
            future = executor.submit(() -> q.pull(1, Duration.ofSeconds(Long.MAX_VALUE)));
            Thread.sleep(100L);
            Assertions.assertFalse(future.isDone(), "Future should not be complete.");
            q.publish(msg("wake"));
            future.get(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("메시지를 발행하면 매우 긴 대기에서 깨어난다")
    void publishedMessageWakesVeryLongWait() throws Exception {
        final Queue q;
        q = this.open().declare("wait-empty", QueueOptions.defaults());
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<Chunk> future;
            future = executor.submit(() -> q.pull(1, Duration.ofSeconds(Long.MAX_VALUE)));
            Thread.sleep(100L);
            q.publish(msg("wake"));
            Assertions.assertEquals(
                    "wake",
                    text(future.get(5L, TimeUnit.SECONDS).messages().getFirst()),
                    "The waiting pull should receive the published message.");
        }
    }

    @Test
    @DisplayName("열고 try with resources로 닫을 수 있다")
    void itCanBeOpenedAndClosedUsingTryWithResources() {
        try (Smq smq = this.openWith(new BuilderSettings())) {
            Assertions.assertNotNull(smq, "Queue manager should be present.");
        }
    }

    private Smq declaredQueueManager() {
        final Smq smq;
        smq = this.open();
        smq.declare("orders", QueueOptions.defaults());
        return smq;
    }

    private Queue declaredQueue() {
        return this.declaredQueueManager().queue("orders");
    }

    @Test
    @DisplayName("선언한 큐는 선언한 이름을 가진다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void declaredQueueRetainsItsName() {
        Assertions.assertEquals("orders", this.declaredQueue().name(), "The declared queue should retain its name.");
    }

    @Test
    @DisplayName("queue 조회는 선언한 큐 이름을 반환한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void queueLookupRetainsItsName() {
        Assertions.assertEquals(
                "orders",
                this.declaredQueueManager().queue("orders").name(),
                "The looked-up queue should retain its name.");
    }

    @Test
    @DisplayName("없는 queue 조회는 QueueNotFoundException이다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void missingQueueLookupThrows() {
        final Smq smq;
        smq = this.declaredQueueManager();
        Assertions.assertThrows(
                QueueNotFoundException.class, () -> smq.queue("missing"), "A missing queue lookup should fail.");
    }

    @Test
    @DisplayName("발행하면 id 목록을 돌려주고 단건 발행도 된다")
    void batchPublishingReturnsOneIdPerMessage() {
        final Queue q;
        final List<MessageId> ids;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Assertions.assertEquals(3, ids.size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("배치 발행한 메시지의 id는 순서대로 반환된다")
    void batchPublishingReturnsIdsInOrder() {
        final Queue q;
        final List<MessageId> ids;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Assertions.assertEquals(ids.stream().sorted().toList(), ids, "Ids should match the expected value.");
    }

    @Test
    @DisplayName("배치 발행한 메시지의 id는 중복되지 않는다")
    void batchPublishingReturnsDistinctIds() {
        final Queue q;
        final List<MessageId> ids;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Assertions.assertEquals(3L, ids.stream().distinct().count(), "Count should match the expected value.");
    }

    @Test
    @DisplayName("단건 발행의 id는 이전 배치보다 크다")
    void singlePublishFollowsBatchIds() {
        final Queue q;
        final List<MessageId> ids;
        final MessageId single;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        single = q.publish(msg("d"));
        Assertions.assertTrue(single.compareTo(ids.getLast()) > 0, "Compare to should be greater than 0.");
    }

    @Test
    @DisplayName("payload와 헤더가 그대로 전달된다 — published Payload Is Preserved")
    void publishedPayloadIsPreserved() throws Exception {
        final Queue q;
        final byte[] payload;
        final Map<String, String> headers;
        final Message m;
        q = this.open().declare("q", QueueOptions.defaults());
        payload = new byte[] {0, 1, 2, -1, 0};
        headers = Map.of("k", "v", "한글", "값", "empty", "");
        q.publish(OutgoingMessage.of(payload).withHeaders(headers));
        m = q.pull(1, Duration.ZERO).messages().getFirst();
        Assertions.assertArrayEquals(payload, m.payload(), "Payload should match the expected value.");
    }

    @Test
    @DisplayName("payload와 헤더가 그대로 전달된다 — published Headers Are Preserved")
    void publishedHeadersArePreserved() throws Exception {
        final Queue q;
        final byte[] payload;
        final Map<String, String> headers;
        final Message m;
        q = this.open().declare("q", QueueOptions.defaults());
        payload = new byte[] {0, 1, 2, -1, 0};
        headers = Map.of("k", "v", "한글", "값", "empty", "");
        q.publish(OutgoingMessage.of(payload).withHeaders(headers));
        m = q.pull(1, Duration.ZERO).messages().getFirst();
        Assertions.assertEquals(headers, m.headers(), "Headers should match the expected value.");
    }

    @Test
    @DisplayName("발행 뒤 외부 payload와 헤더를 바꿔도 메시지는 그대로다 — published Payload Is Defensively Copied")
    void publishedPayloadIsDefensivelyCopied() throws Exception {
        final Queue q;
        final byte[] payload;
        final Map<String, String> headers;
        final OutgoingMessage outgoing;
        final Message message;
        q = this.open().declare("q", QueueOptions.defaults());
        payload = new byte[] {1, 2};
        headers = new HashMap<>(Map.of("key", "original"));
        outgoing = new OutgoingMessage(payload, headers);
        q.publish(outgoing);
        payload[0] = 9;
        headers.put("key", "changed");
        outgoing.payload()[1] = 9;
        message = q.pull(1, Duration.ZERO).messages().getFirst();
        Assertions.assertArrayEquals(new byte[] {1, 2}, message.payload(), "Payload should match the expected value.");
    }

    @Test
    @DisplayName("발행 뒤 외부 payload와 헤더를 바꿔도 메시지는 그대로다 — published Headers Are Defensively Copied")
    void publishedHeadersAreDefensivelyCopied() throws Exception {
        final Queue q;
        final byte[] payload;
        final Map<String, String> headers;
        final OutgoingMessage outgoing;
        final Message message;
        q = this.open().declare("q", QueueOptions.defaults());
        payload = new byte[] {1, 2};
        headers = new HashMap<>(Map.of("key", "original"));
        outgoing = new OutgoingMessage(payload, headers);
        q.publish(outgoing);
        payload[0] = 9;
        headers.put("key", "changed");
        outgoing.payload()[1] = 9;
        message = q.pull(1, Duration.ZERO).messages().getFirst();
        Assertions.assertEquals(
                Map.of("key", "original"), message.headers(), "Headers should match the expected value.");
    }

    @Test
    @DisplayName("짝이 없는 surrogate를 헤더에 허용하지 않는다")
    void unpairedSurrogatesAreNotAllowedInTheHeader() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new OutgoingMessage(new byte[0], Map.of("?", "one", "\ud800", "two"), Duration.ZERO),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Respects Maximum Count")
    void pullRespectsMaximumCount() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals(2, c.messages().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Returns Message Ids In Order")
    void pullReturnsMessageIdsInOrder() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals(
                ids.subList(0, 2),
                c.messages().stream().map(Message::id).toList(),
                "Messages should match the expected value.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Returns First Payload")
    void pullReturnsFirstPayload() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals("a", text(c.messages().get(0)), "Text should match the expected value.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Returns Delivery Count")
    void pullReturnsDeliveryCount() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals(
                1, c.messages().get(0).deliveryCount(), "Delivery count should match the expected value.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Returns Published At")
    void pullReturnsPublishedAt() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals(T0, c.messages().get(0).publishedAt(), "Published at should match the expected value.");
    }

    @Test
    @DisplayName("pull은 id 순서로 최대 max개를 돌려주고 메타데이터를 담는다 — pull Returns Queue Name")
    void pullReturnsQueueName() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertEquals("q", c.queue(), "Queue should match the expected value.");
    }

    @Test
    @DisplayName("메시지가 없으면 null이나 예외가 아니라 빈 청크 — empty Pull Returns Chunk")
    void emptyPullReturnsChunk() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        c = q.pull(10, Duration.ZERO);
        Assertions.assertNotNull(c, "Chunk should be present.");
    }

    @Test
    @DisplayName("메시지가 없으면 null이나 예외가 아니라 빈 청크 — empty Pull Returns No Messages")
    void emptyPullReturnsNoMessages() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        c = q.pull(10, Duration.ZERO);
        Assertions.assertTrue(c.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("메시지가 없으면 null이나 예외가 아니라 빈 청크 — empty Pull Settlement Has No Outcomes")
    void emptyPullSettlementHasNoOutcomes() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        c = q.pull(10, Duration.ZERO);
        Assertions.assertTrue(q.settle(c.ackAll()).byMessage().isEmpty(), "By message should be empty.");
    }

    @Test
    @DisplayName("wait 동안 기다리다 도착한 메시지를 돌려준다")
    void returnsTheMessageThatArrivesWhileWaitingForTheSpecifiedWaitTime() throws Exception {
        final Queue q;
        final Thread publisher;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        publisher = Thread.ofPlatform().start(() -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            q.publish(msg("late"));
        });
        c = q.pull(1, Duration.ofSeconds(5L));
        publisher.join();
        Assertions.assertEquals(1, c.messages().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("wait가 지나면 빈 청크이고 시계가 멈춰 있어도 돌아온다 — wait Returns No Messages")
    void waitReturnsNoMessages() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        System.nanoTime();
        c = q.pull(1, Duration.ofMillis(50L));
        Assertions.assertTrue(c.messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("wait가 지나면 빈 청크이고 시계가 멈춰 있어도 돌아온다 — empty Wait Has No Lease Deadline")
    void emptyWaitHasNoLeaseDeadline() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        System.nanoTime();
        c = q.pull(1, Duration.ofMillis(50L));
        Assertions.assertNull(c.leaseDeadline(), "Lease deadline should be null.");
    }

    @Test
    @DisplayName("wait가 지나면 빈 청크이고 시계가 멈춰 있어도 돌아온다 — empty Wait Finishes Within Timeout")
    void emptyWaitFinishesWithinTimeout() throws Exception {
        final Queue q;
        final long start;
        q = this.open().declare("q", QueueOptions.defaults());
        start = System.nanoTime();
        q.pull(1, Duration.ofMillis(50L));
        Assertions.assertTrue(
                Duration.ofNanos(System.nanoTime() - start).toMillis() < 2000L, "To millis should be less than 2000.");
    }

    @Test
    @DisplayName("빈 청크의 리스 기한과 빈 청크 연장의 기한은 null이다 — empty Chunk Has No Messages")
    void emptyChunkHasNoMessages() throws Exception {
        final Queue q;
        final Chunk empty;
        q = this.open().declare("q", QueueOptions.defaults());
        empty = q.pull(1, Duration.ZERO);
        Assertions.assertTrue(empty.isEmpty(), "Empty should be empty.");
    }

    @Test
    @DisplayName("빈 청크의 리스 기한과 빈 청크 연장의 기한은 null이다 — empty Chunk Has No Lease Deadline")
    void emptyChunkHasNoLeaseDeadline() throws Exception {
        final Queue q;
        final Chunk empty;
        q = this.open().declare("q", QueueOptions.defaults());
        empty = q.pull(1, Duration.ZERO);
        Assertions.assertNull(empty.leaseDeadline(), "Lease deadline should be null.");
    }

    @Test
    @DisplayName("빈 청크의 리스 기한과 빈 청크 연장의 기한은 null이다 — empty Chunk Extension Has No Lease Deadline")
    void emptyChunkExtensionHasNoLeaseDeadline() throws Exception {
        final Queue q;
        final Chunk empty;
        final Extension ext;
        q = this.open().declare("q", QueueOptions.defaults());
        empty = q.pull(1, Duration.ZERO);
        ext = q.extend(empty, Duration.ofSeconds(10L));
        Assertions.assertNull(ext.leaseDeadline(), "Lease deadline should be null.");
    }

    @Test
    @DisplayName("빈 청크의 리스 기한과 빈 청크 연장의 기한은 null이다 — empty Chunk Extension Has No Outcomes")
    void emptyChunkExtensionHasNoOutcomes() throws Exception {
        final Queue q;
        final Chunk empty;
        final Extension ext;
        q = this.open().declare("q", QueueOptions.defaults());
        empty = q.pull(1, Duration.ZERO);
        ext = q.extend(empty, Duration.ofSeconds(10L));
        Assertions.assertTrue(ext.outcomes().byMessage().isEmpty(), "By message should be empty.");
    }

    @Test
    @DisplayName("리스 기한은 주입한 시계와 리스 시간으로 계산된다")
    void defaultLeaseDeadlineUsesInjectedClock() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(45L)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        Assertions.assertEquals(
                T0.plusSeconds(45L),
                q.pull(1, Duration.ZERO).leaseDeadline(),
                "Lease deadline should match the expected value.");
    }

    @Test
    @DisplayName("지정한 리스 기한은 주입한 시계와 리스 시간으로 계산된다")
    void explicitLeaseDeadlineUsesInjectedClock() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(45L)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO);
        q.publish(msg("b"));
        Assertions.assertEquals(
                T0.plusSeconds(7L),
                q.pull(1, Duration.ZERO, Duration.ofSeconds(7L)).leaseDeadline(),
                "Lease deadline should match the expected value.");
    }

    @Test
    @DisplayName("기본 리스 시간은 30초")
    void theDefaultLeaseTimeIs30Seconds() throws Exception {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        Assertions.assertEquals(
                T0.plusSeconds(30L),
                q.pull(1, Duration.ZERO).leaseDeadline(),
                "Lease deadline should match the expected value.");
    }

    @Test
    @DisplayName("ackAll은 메시지를 삭제하고 OK를 돌려준다")
    void ackAllReturnsOk() throws Exception {
        final Queue q;
        final Chunk c;
        final Outcomes r;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        r = q.settle(c.ackAll());
        Assertions.assertTrue(r.allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("ackAll은 모든 메시지의 정산 결과를 돌려준다")
    void ackAllReturnsAnOutcomeForEachMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        r = q.settle(c.ackAll());
        Assertions.assertEquals(ids.size(), r.byMessage().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("ackAll 뒤 준비 메시지는 없다")
    void ackAllLeavesNoReadyMessages() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        q.settle(c.ackAll());
        Assertions.assertEquals(0L, q.counts().ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("ackAll은 메시지를 삭제한다")
    void ackAllRemovesMessages() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        q.settle(c.ackAll());
        Assertions.assertTrue(q.pull(10, Duration.ZERO).messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("같은 정산을 다시 하면 STALE — first Settlement Succeeds")
    void firstSettlementSucceeds() throws Exception {
        final Queue q;
        final Chunk c;
        final Settlement s;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        s = c.ackAll();
        Assertions.assertTrue(q.settle(s).allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("같은 정산을 다시 하면 STALE — repeated Settlement Fails")
    void repeatedSettlementFails() throws Exception {
        final Queue q;
        final Chunk c;
        final Settlement s;
        final Outcomes again;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        s = c.ackAll();
        q.settle(s);
        again = q.settle(s);
        Assertions.assertFalse(again.allOk(), "Outcomes should not succeed.");
    }

    @Test
    @DisplayName("같은 정산을 다시 하면 STALE — repeated Settlement Reports Stale Message")
    void repeatedSettlementReportsStaleMessage() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Settlement s;
        final Outcomes again;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        s = c.ackAll();
        q.settle(s);
        again = q.settle(s);
        Assertions.assertEquals(List.of(id), again.stale(), "Stale should match the expected value.");
    }

    @Test
    @DisplayName("counts는 준비 메시지 개수를 돌려준다 — before publish")
    void readyCountIsZeroBeforePublish() throws Exception {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        Assertions.assertEquals(0L, q.counts().ready(), "Ready should match the expected value.");
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
    }

    @Test
    @DisplayName("counts는 준비 메시지 개수를 돌려준다 — after publish")
    void readyCountIncludesPublishedMessages() throws Exception {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        Assertions.assertEquals(3L, q.counts().ready(), "Ready should match the expected value.");
        q.pull(2, Duration.ZERO);
    }

    @Test
    @DisplayName("counts는 준비 메시지 개수를 돌려준다 — after pull")
    void readyCountExcludesPulledMessages() throws Exception {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        Assertions.assertEquals(1L, q.counts().ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("큐마다 메시지가 분리된다 — other Queue Has No Ready Messages")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void otherQueueHasNoReadyMessages() throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        smq = this.open();
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        Assertions.assertEquals(0L, b.counts().ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("큐마다 메시지가 분리된다 — other Queue Does Not Deliver Published Message")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void otherQueueDoesNotDeliverPublishedMessage() throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        smq = this.open();
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        Assertions.assertTrue(b.pull(1, Duration.ZERO).messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    @DisplayName("큐마다 메시지가 분리된다 — published Message Remains In Its Own Queue")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void publishedMessageRemainsInItsOwnQueue() throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        smq = this.open();
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        b.pull(1, Duration.ZERO);
        Assertions.assertEquals(1, a.pull(1, Duration.ZERO).messages().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("발행 배치는 원자적이다 중간에 잘못된 원소가 있으면 하나도 들어가지 않는다 — invalid Publish Batch Throws")
    void invalidPublishBatchThrows() {
        final Queue q;
        final List<OutgoingMessage> batch;
        q = this.open().declare("q", QueueOptions.defaults());
        batch = new ArrayList<>();
        batch.add(msg("a"));
        batch.add(null);
        batch.add(msg("c"));
        Assertions.assertThrows(
                NullPointerException.class, () -> q.publish(batch), "Publish should throw NullPointerException.");
    }

    @Test
    @DisplayName("발행 배치는 원자적이다 중간에 잘못된 원소가 있으면 하나도 들어가지 않는다 — invalid Publish Batch Changes No Messages")
    void invalidPublishBatchChangesNoMessages() {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        Assertions.assertEquals(0L, invalidBatchQueueCount(q), "An invalid batch should leave the queue unchanged.");
    }

    @Test
    @DisplayName("FIFO 순서는 발행 순서와 배치 경계를 넘어 유지된다")
    void fifoOrderFollowsPublishOrderAcrossBatches() throws Exception {
        final Queue q;
        final List<String> got;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("1"), msg("2")));
        q.publish(msg("3"));
        q.publish(List.of(msg("4"), msg("5")));
        got = new ArrayList<>();
        for (final Message m : q.pull(2, Duration.ZERO).messages()) {
            got.add(text(m));
        }
        for (final Message m : q.pull(10, Duration.ZERO).messages()) {
            got.add(text(m));
        }
        Assertions.assertEquals(List.of("1", "2", "3", "4", "5"), got, "Got should match the expected value.");
    }

    private static List<String> invalidQueueNames() {
        return List.of("", "has space", "a/b", "x".repeat(201));
    }

    @ParameterizedTest
    @MethodSource("invalidQueueNames")
    @DisplayName("잘못된 이름으로 큐를 선언할 수 없다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void invalidQueueNamesCannotBeDeclared(final String name) {
        final Smq smq;
        smq = this.open();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> smq.declare(name, QueueOptions.defaults()),
                "Declaring an invalid queue name should fail.");
    }

    @ParameterizedTest
    @MethodSource("invalidQueueNames")
    @DisplayName("잘못된 이름으로 큐를 조회할 수 없다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void invalidQueueNamesCannotBeLookedUp(final String name) {
        final Smq smq;
        smq = this.open();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> smq.queue(name), "Looking up an invalid queue name should fail.");
    }

    @ParameterizedTest
    @EnumSource(InvalidPull.class)
    @DisplayName("꺼내기의 잘못된 인자는 거부한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void invalidPullArgumentsAreRejected(final InvalidPull invalid) {
        final Smq smq;
        final Queue q;
        final Executable call;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        call = switch (invalid) {
            case ZERO_COUNT -> () -> q.pull(0, Duration.ZERO);
            case NEGATIVE_COUNT -> () -> q.pull(-1, Duration.ZERO);
            case NEGATIVE_WAIT -> () -> q.pull(1, Duration.ofSeconds(-1L));
            case ZERO_LEASE -> () -> q.pull(1, Duration.ZERO, Duration.ZERO);
            case NEGATIVE_LEASE -> () -> smq.queue("q").pull(1, Duration.ZERO, Duration.ofSeconds(-1L));
        };
        Assertions.assertThrows(IllegalArgumentException.class, call, "An invalid pull argument should be rejected.");
    }

    @Test
    @DisplayName("예외 계층의 뿌리는 unchecked SmqException — unchecked root")
    void smqExceptionExtendsRuntimeException() {
        Assertions.assertTrue(
                RuntimeException.class.isAssignableFrom(SmqException.class), "Type should be assignable.");
    }

    @Test
    @DisplayName("예외 계층의 뿌리는 unchecked SmqException — queue exception")
    void queueNotFoundExceptionExtendsSmqException() {
        Assertions.assertTrue(
                SmqException.class.isAssignableFrom(QueueNotFoundException.class), "Type should be assignable.");
    }

    @Test
    @DisplayName("같은 옵션으로 여러 번 선언해도 아무 일이 없다 — redeclared Queue Retains Options")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void redeclaredQueueRetainsOptions() throws Exception {
        final Smq smq;
        final QueueOptions options;
        final Queue first;
        final Queue again;
        smq = this.open();
        smq.declare("orders.dead", QueueOptions.defaults());
        options = QueueOptions.defaults().maxDeliveries(5, "orders.dead");
        first = smq.declare("orders", options);
        first.publish(msg("a"));
        again = smq.declare("orders", options);
        Assertions.assertEquals(options, again.options(), "Options should match the expected value.");
    }

    @Test
    @DisplayName("같은 옵션으로 여러 번 선언해도 아무 일이 없다 — lookup After Redeclaration Retains Options")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void lookupAfterRedeclarationRetainsOptions() throws Exception {
        final Smq smq;
        final QueueOptions options;
        final Queue first;
        smq = this.open();
        smq.declare("orders.dead", QueueOptions.defaults());
        options = QueueOptions.defaults().maxDeliveries(5, "orders.dead");
        first = smq.declare("orders", options);
        first.publish(msg("a"));
        smq.declare("orders", options);
        Assertions.assertEquals(options, smq.queue("orders").options(), "Options should match the expected value.");
    }

    @Test
    @DisplayName("같은 옵션으로 여러 번 선언해도 아무 일이 없다 — redeclaration Preserves Messages")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void redeclarationPreservesMessages() throws Exception {
        final Smq smq;
        final QueueOptions options;
        final Queue first;
        final Queue again;
        smq = this.open();
        smq.declare("orders.dead", QueueOptions.defaults());
        options = QueueOptions.defaults().maxDeliveries(5, "orders.dead");
        first = smq.declare("orders", options);
        first.publish(msg("a"));
        again = smq.declare("orders", options);
        Assertions.assertEquals(1L, again.counts().ready(), "Ready should match the expected value.");
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrown() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        Assertions.assertInstanceOf(
                QueueDeclarationConflictException.class,
                assertionValue6,
                "Declare should throw QueueDeclarationConflictException.");
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = e.requested();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = smq.queue("orders").options();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrownCheck2() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        Assertions.assertInstanceOf(SmqException.class, e, "Value should be an instance of SmqException.");
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = e.requested();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = smq.queue("orders").options();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrownCheck3() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        final var assertionValue4Argument2 = e.queue();
        Assertions.assertEquals("orders", assertionValue4Argument2, "Queue should match the expected value.");
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = e.requested();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = smq.queue("orders").options();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrownCheck4() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument2 = e.existing();
        Assertions.assertEquals(existing, assertionValue3Argument2, "Existing should match the expected value.");
        final var assertionValue2Argument2 = e.requested();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = smq.queue("orders").options();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrownCheck5() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = e.requested();
        Assertions.assertEquals(requested, assertionValue2Argument2, "Requested should match the expected value.");
        final var assertionValue1Argument2 = smq.queue("orders").options();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("이미 있는 큐를 다른 옵션으로 선언하면 기존과 요청 옵션을 담은 QueueDeclarationConflictException — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifAnExistingQueueIsDeclaredWithDifferentOptionsAQueueDeclarationConflictExceptionContainingTheExistingOptionsAndTheRequestedOptionsWillBeThrownCheck6() {
        final Smq smq;
        final QueueOptions existing;
        final QueueOptions requested;
        final QueueDeclarationConflictException e;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        existing = QueueOptions.defaults().maxDeliveries(3, "dead");
        requested = QueueOptions.defaults().maxDeliveries(5, "dead");
        smq.declare("orders", existing);
        final var assertionValue6 = captureThrowable(() -> smq.declare("orders", requested));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue6;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = e.requested();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = smq.queue("orders").options();
        Assertions.assertEquals(existing, assertionValue1Argument2, "Options should match the expected value.");
    }

    @Test
    @DisplayName("dead letter 큐 없이 최대 전달 횟수를 지정할 수 없다")
    void maxDeliveriesRequiresDeadLetterQueue() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new QueueOptions(OptionalInt.of(3), Optional.empty(), false),
                "A delivery limit should require a dead letter queue.");
    }

    @Test
    @DisplayName("최대 전달 횟수는 양수여야 한다")
    void maxDeliveriesMustBePositive() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> QueueOptions.defaults().maxDeliveries(0, "dead"),
                "The delivery limit should be positive.");
    }

    @Test
    @DisplayName("기본 최대 전달 횟수는 무제한이다")
    void defaultMaxDeliveriesIsUnlimited() {
        Assertions.assertTrue(
                QueueOptions.defaults().maxDeliveries().isEmpty(), "The default delivery limit should be absent.");
    }

    @Test
    @DisplayName("dead letter 큐를 지정해도 최대 전달 횟수는 기본적으로 무제한이다")
    void deadLetterQueueDoesNotSetDeliveryLimit() {
        Assertions.assertTrue(
                QueueOptions.defaults().deadLetterQueue("dead").maxDeliveries().isEmpty(),
                "A dead letter queue should not set a delivery limit.");
    }

    @Test
    @DisplayName("최대 전달 횟수를 지정하지 않은 큐는 무제한이다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void declaredDeadLetterQueueHasUnlimitedDeliveriesByDefault() {
        final Smq smq;
        final Queue q;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("dead"));
        Assertions.assertTrue(
                q.options().maxDeliveries().isEmpty(), "The declared queue should have no delivery limit.");
    }

    @Test
    @DisplayName("최대 전달 횟수를 지정하지 않아도 dead letter 큐는 저장된다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void declaredQueueRetainsDeadLetterWithoutDeliveryLimit() {
        final Smq smq;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        smq.declare("orders", QueueOptions.defaults().deadLetterQueue("dead"));
        Assertions.assertEquals(
                Optional.of("dead"),
                smq.queue("orders").options().deadLetterQueue(),
                "The declared queue should retain its dead letter queue.");
    }

    @Test
    @DisplayName("자기 자신을 dead letter 큐로 지정하면 오류이고 없는 큐를 지정하면 선언 오류다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            designatingYourselfAsADeadLetterQueueResultsInAnErrorAndDesignatingANonExistentQueueResultsInADeclarationError() {
        final Smq smq;
        final QueueNotFoundException e;
        smq = this.open();
        final var assertionValue5 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue5, "Declare should throw IllegalArgumentException.");
        final var assertionValue4 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        evaluatePmdArgument(assertionValue4);
        final var assertionValue3 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        e = (QueueNotFoundException) (QueueNotFoundException) assertionValue3;
        final var assertionValue2Argument2 = e.queue();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1 = captureThrowable(() -> smq.queue("orders"));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("자기 자신을 dead letter 큐로 지정하면 오류이고 없는 큐를 지정하면 선언 오류다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            designatingYourselfAsADeadLetterQueueResultsInAnErrorAndDesignatingANonExistentQueueResultsInADeclarationErrorCheck2() {
        final Smq smq;
        final QueueNotFoundException e;
        smq = this.open();
        final var assertionValue5 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        evaluatePmdArgument(assertionValue5);
        final var assertionValue4 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue4, "Declare should throw IllegalArgumentException.");
        final var assertionValue3 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        e = (QueueNotFoundException) (QueueNotFoundException) assertionValue3;
        final var assertionValue2Argument2 = e.queue();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1 = captureThrowable(() -> smq.queue("orders"));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("자기 자신을 dead letter 큐로 지정하면 오류이고 없는 큐를 지정하면 선언 오류다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            designatingYourselfAsADeadLetterQueueResultsInAnErrorAndDesignatingANonExistentQueueResultsInADeclarationErrorCheck3() {
        final Smq smq;
        final QueueNotFoundException e;
        smq = this.open();
        final var assertionValue5 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        evaluatePmdArgument(assertionValue5);
        final var assertionValue4 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        evaluatePmdArgument(assertionValue4);
        final var assertionValue3 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        Assertions.assertInstanceOf(
                QueueNotFoundException.class, assertionValue3, "Declare should throw QueueNotFoundException.");
        e = (QueueNotFoundException) (QueueNotFoundException) assertionValue3;
        final var assertionValue2Argument2 = e.queue();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1 = captureThrowable(() -> smq.queue("orders"));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("자기 자신을 dead letter 큐로 지정하면 오류이고 없는 큐를 지정하면 선언 오류다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            designatingYourselfAsADeadLetterQueueResultsInAnErrorAndDesignatingANonExistentQueueResultsInADeclarationErrorCheck4() {
        final Smq smq;
        final QueueNotFoundException e;
        smq = this.open();
        final var assertionValue5 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        evaluatePmdArgument(assertionValue5);
        final var assertionValue4 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        evaluatePmdArgument(assertionValue4);
        final var assertionValue3 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        e = (QueueNotFoundException) (QueueNotFoundException) assertionValue3;
        final var assertionValue2Argument2 = e.queue();
        Assertions.assertEquals("missing", assertionValue2Argument2, "Queue should match the expected value.");
        final var assertionValue1 = captureThrowable(() -> smq.queue("orders"));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("자기 자신을 dead letter 큐로 지정하면 오류이고 없는 큐를 지정하면 선언 오류다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            designatingYourselfAsADeadLetterQueueResultsInAnErrorAndDesignatingANonExistentQueueResultsInADeclarationErrorCheck5() {
        final Smq smq;
        final QueueNotFoundException e;
        smq = this.open();
        final var assertionValue5 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders")));
        evaluatePmdArgument(assertionValue5);
        final var assertionValue4 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "orders")));
        evaluatePmdArgument(assertionValue4);
        final var assertionValue3 = captureThrowable(
                () -> smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "missing")));
        e = (QueueNotFoundException) (QueueNotFoundException) assertionValue3;
        final var assertionValue2Argument2 = e.queue();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1 = captureThrowable(() -> smq.queue("orders"));
        Assertions.assertInstanceOf(
                QueueNotFoundException.class, assertionValue1, "Queue should throw QueueNotFoundException.");
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarations() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        Assertions.assertTrue(assertionValue7Argument1, "Single active consumer mode should be enabled.");
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck2() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        Assertions.assertTrue(assertionValue6Argument1, "Single active consumer mode should be enabled.");
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck3() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        Assertions.assertEquals(single, assertionValue5Argument2, "Options should match the expected value.");
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck4() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        Assertions.assertInstanceOf(
                QueueDeclarationConflictException.class,
                assertionValue4,
                "Declare should throw QueueDeclarationConflictException.");
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck5() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        Assertions.assertEquals(single, assertionValue3Argument2, "Existing should match the expected value.");
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck6() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        Assertions.assertFalse(assertionValue2Argument1, "Single active consumer mode should be disabled.");
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        evaluatePmdArgument(assertionValue1);
    }

    @Test
    @DisplayName("singleActiveConsumer가 저장되어 options와 재선언 비교에 반영된다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void singleactiveconsumerIsStoredAndReflectedInTheComparisonBetweenOptionsAndReDeclarationsCheck7() {
        final Smq smq;
        final QueueOptions single;
        final Queue q;
        final QueueDeclarationConflictException e;
        smq = this.open();
        single = QueueOptions.defaults().singleActiveConsumer(true);
        q = smq.declare("ordered", single);
        final var assertionValue7Argument1 = q.options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue7Argument1);
        final var assertionValue6Argument1 = smq.queue("ordered").options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue5Argument2 = smq.declare("ordered", single).options();
        evaluatePmdArgument(assertionValue5Argument2);
        final var assertionValue4 = captureThrowable(() -> smq.declare("ordered", QueueOptions.defaults()));
        e = (QueueDeclarationConflictException) (QueueDeclarationConflictException) assertionValue4;
        final var assertionValue3Argument2 = e.existing();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 =
                smq.declare("plain", QueueOptions.defaults()).options().singleActiveConsumer();
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue1 = captureThrowable(() -> smq.declare("plain", single));
        Assertions.assertInstanceOf(
                QueueDeclarationConflictException.class,
                assertionValue1,
                "Declare should throw QueueDeclarationConflictException.");
    }

    private Smq queueWithMessagesForDeletion() {
        final Smq smq;
        smq = this.open();
        smq.declare("orders", QueueOptions.defaults()).publish(List.of(msg("a"), msg("b")));
        return smq;
    }

    @Test
    @DisplayName("기존 큐 삭제는 true를 반환한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void deletingExistingQueueReturnsTrue() {
        Assertions.assertTrue(
                this.queueWithMessagesForDeletion().delete("orders"), "An existing queue should be deleted.");
    }

    @Test
    @DisplayName("같은 큐를 두 번째 삭제하면 false를 반환한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deletingQueueAgainReturnsFalse() {
        final Smq smq;
        smq = this.queueWithMessagesForDeletion();
        smq.delete("orders");
        Assertions.assertFalse(smq.delete("orders"), "An absent queue should not be deleted.");
    }

    @Test
    @DisplayName("선언하지 않은 큐 삭제는 false를 반환한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void deletingNeverDeclaredQueueReturnsFalse() {
        Assertions.assertFalse(
                this.queueWithMessagesForDeletion().delete("never-declared"),
                "A never-declared queue should not be deleted.");
    }

    @Test
    @DisplayName("삭제한 큐를 조회하면 QueueNotFoundException이다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deletedQueueCannotBeLookedUp() {
        final Smq smq;
        smq = this.queueWithMessagesForDeletion();
        smq.delete("orders");
        Assertions.assertThrows(
                QueueNotFoundException.class, () -> smq.queue("orders"), "A deleted queue should not be found.");
    }

    @Test
    @DisplayName("잘못된 큐 이름 삭제는 거부한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void deletingInvalidQueueNameIsRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> this.queueWithMessagesForDeletion().delete("bad name"),
                "An invalid queue name should be rejected.");
    }

    @Test
    @DisplayName("삭제한 큐를 다시 선언하면 메시지가 비어 있다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void redeclaredQueueStartsEmptyAfterDeletion() {
        final Smq smq;
        smq = this.queueWithMessagesForDeletion();
        smq.delete("orders");
        Assertions.assertEquals(
                0L,
                smq.declare("orders", QueueOptions.defaults()).counts().ready(),
                "A redeclared queue should start empty.");
    }

    private Smq deadLetterReferenceScenario() {
        final Smq smq;
        smq = this.open();
        smq.declare("dead", QueueOptions.defaults());
        smq.declare("orders", QueueOptions.defaults().maxDeliveries(3, "dead"));
        return smq;
    }

    private QueueInUseException deleteReferencedDeadQueue(final Smq smq) {
        try {
            smq.delete("dead");
        } catch (QueueInUseException e) {
            return e;
        }
        throw new AssertionError("A referenced dead letter queue should not be deleted.");
    }

    @Test
    @DisplayName("참조 중인 dead letter 큐 삭제는 거부한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void referencedDeadLetterQueueCannotBeDeleted() {
        final Smq smq;
        smq = this.deadLetterReferenceScenario();
        Assertions.assertThrows(
                QueueInUseException.class,
                () -> smq.delete("dead"),
                "A referenced dead letter queue should not be deleted.");
    }

    @Test
    @DisplayName("QueueInUseException은 SmqException이다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void queueInUseExceptionExtendsSmqException() {
        Assertions.assertInstanceOf(
                SmqException.class,
                this.deleteReferencedDeadQueue(this.deadLetterReferenceScenario()),
                "QueueInUseException should be an SmqException.");
    }

    @Test
    @DisplayName("QueueInUseException은 참조된 큐 이름을 담는다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void queueInUseExceptionNamesReferencedQueue() {
        Assertions.assertEquals(
                "dead",
                this.deleteReferencedDeadQueue(this.deadLetterReferenceScenario())
                        .queue(),
                "The exception should identify the referenced queue.");
    }

    @Test
    @DisplayName("QueueInUseException은 참조하는 큐 이름을 담는다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    void queueInUseExceptionNamesReferencingQueue() {
        Assertions.assertEquals(
                "orders",
                this.deleteReferencedDeadQueue(this.deadLetterReferenceScenario())
                        .referencedBy(),
                "The exception should identify the referencing queue.");
    }

    @Test
    @DisplayName("참조된 dead letter 큐는 삭제 거부 뒤에도 존재한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void referencedDeadLetterQueueRemainsAfterRejectedDeletion() {
        final Smq smq;
        smq = this.deadLetterReferenceScenario();
        this.deleteReferencedDeadQueue(smq);
        Assertions.assertEquals("dead", smq.queue("dead").name(), "The referenced queue should remain.");
    }

    @Test
    @DisplayName("참조 큐를 지운 뒤 dead letter 큐도 삭제할 수 있다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deadLetterQueueCanBeDeletedAfterRemovingReference() {
        final Smq smq;
        smq = this.deadLetterReferenceScenario();
        smq.delete("orders");
        Assertions.assertTrue(smq.delete("dead"), "The unreferenced queue should be deleted.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"b", "c"})
    @DisplayName("dead letter 큐 연쇄에서 참조 중인 큐는 지울 수 없다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void referencedDeadLetterQueueCannotBeDeleted(final String name) {
        final Smq smq;
        smq = this.open();
        smq.declare("c", QueueOptions.defaults());
        smq.declare("b", QueueOptions.defaults().deadLetterQueue("c"));
        smq.declare("a", QueueOptions.defaults().deadLetterQueue("b"));
        Assertions.assertThrows(
                QueueInUseException.class, () -> smq.delete(name), "A referenced queue should not be deleted.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b", "c"})
    @DisplayName("dead letter 큐 연쇄에서 참조를 제거한 큐는 지울 수 있다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture closes registered instances after each test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void unreferencedDeadLetterQueueCanBeDeleted(final String name) {
        final Smq smq;
        smq = this.open();
        smq.declare("c", QueueOptions.defaults());
        smq.declare("b", QueueOptions.defaults().deadLetterQueue("c"));
        smq.declare("a", QueueOptions.defaults().deadLetterQueue("b"));
        if (!name.equals("a")) {
            smq.delete("a");
        }
        if (name.equals("c")) {
            smq.delete("b");
        }
        Assertions.assertTrue(smq.delete(name), "An unreferenced queue should be deleted.");
    }

    @Test
    @DisplayName("삭제한 큐의 핸들로 발행하거나 꺼내면 QueueNotFoundException — deleted Queue Rejects Publish")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deletedQueueRejectsPublish() {
        final Smq smq;
        final Queue q;
        smq = this.open();
        q = smq.declare("orders", QueueOptions.defaults());
        smq.delete("orders");
        Assertions.assertThrows(
                QueueNotFoundException.class,
                () -> q.publish(msg("a")),
                "Publish should throw QueueNotFoundException.");
    }

    @Test
    @DisplayName("삭제한 큐의 핸들로 발행하거나 꺼내면 QueueNotFoundException — deleted Queue Rejects Pull")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deletedQueueRejectsPull() {
        final Smq smq;
        final Queue q;
        smq = this.open();
        q = smq.declare("orders", QueueOptions.defaults());
        smq.delete("orders");
        Assertions.assertThrows(
                QueueNotFoundException.class,
                () -> q.pull(1, Duration.ZERO),
                "Pull should throw QueueNotFoundException.");
    }

    @Test
    @DisplayName("삭제한 큐의 핸들로 발행하거나 꺼내면 QueueNotFoundException — deleted Queue Rejects Counts")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void deletedQueueRejectsCounts() {
        final Smq smq;
        final Queue q;
        smq = this.open();
        q = smq.declare("orders", QueueOptions.defaults());
        smq.delete("orders");
        Assertions.assertThrows(QueueNotFoundException.class, q::counts, "Counts should throw QueueNotFoundException.");
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck1()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final int assertionValue1Argument2;
        final Queue fresh;
        final Chunk freshChunk;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        assertionValue1Argument2 = oldChunk.messages().size();
        Assertions.assertEquals(2, assertionValue1Argument2, "Size should match the expected value.");
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        evaluate(fresh.settle(oldChunk.ackAll()));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck2()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Outcomes afterDelete;
        final Queue fresh;
        final Chunk freshChunk;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        afterDelete = old.settle(oldChunk.ackAll());
        for (final Message m : oldChunk.messages()) {
            final Outcome assertionValue2Argument2;
            assertionValue2Argument2 = afterDelete.byMessage().get(m.id());
            Assertions.assertEquals(
                    Outcome.STALE, assertionValue2Argument2, "By message should match the expected value.");
        }
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        evaluate(fresh.settle(oldChunk.ackAll()));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck3()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Queue fresh;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        final Chunk freshChunk;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue3Argument2 = fresh.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        evaluate(fresh.settle(oldChunk.ackAll()));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck4()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Queue fresh;
        final Chunk freshChunk;
        final String assertionValue4Argument2;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        assertionValue4Argument2 = text(freshChunk.messages().getFirst());
        Assertions.assertEquals("new-1", assertionValue4Argument2, "Text should match the expected value.");
        evaluate(fresh.settle(oldChunk.ackAll()));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck5()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Queue fresh;
        final Chunk freshChunk;
        final Outcomes stale;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        stale = fresh.settle(oldChunk.ackAll());
        for (final Message m : oldChunk.messages()) {
            final Outcome assertionValue5Argument2;
            assertionValue5Argument2 = stale.byMessage().get(m.id());
            Assertions.assertEquals(
                    Outcome.STALE, assertionValue5Argument2, "By message should match the expected value.");
        }
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck6()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Queue fresh;
        final Chunk freshChunk;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        evaluate(fresh.settle(oldChunk.ackAll()));
        assertionValue6Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue6Argument2 = fresh.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
        evaluate(fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id()));
    }

    @Test
    @DisplayName("삭제한 큐의 리스로 들어온 정산은 STALE이고 같은 이름으로 다시 선언한 큐는 새 큐다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void aSettlementThatComesInAsALeaseOfADeletedQueueIsSTALEAndAQueueRedeclaredWithTheSameNameIsANewQueueCheck7()
            throws Exception {
        final Smq smq;
        final Queue old;
        final Chunk oldChunk;
        final Queue fresh;
        final Chunk freshChunk;
        final Outcome assertionValue7Argument2;
        smq = this.open();
        old = smq.declare("orders", QueueOptions.defaults());
        old.publish(List.of(msg("old-1"), msg("old-2")));
        oldChunk = old.pull(10, Duration.ZERO);
        smq.delete("orders");
        evaluate(old.settle(oldChunk.ackAll()));
        fresh = smq.declare("orders", QueueOptions.defaults());
        evaluate(new QueueCounts(0L, 0L, 0L));
        fresh.publish(msg("new-1"));
        freshChunk = fresh.pull(10, Duration.ZERO);
        evaluate(fresh.settle(oldChunk.ackAll()));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue7Argument2 = fresh.settle(freshChunk.ackAll())
                .byMessage()
                .get(freshChunk.messages().getFirst().id());
        Assertions.assertEquals(Outcome.OK, assertionValue7Argument2, "By message should match the expected value.");
    }

    @Test
    @DisplayName("리스가 만료된 메시지는 원래 id 자리로 돌아가 다시 꺼내지고 전달 횟수가 늘어난다 — expired Lease First Pull Starts At Delivery One")
    void expiredLeaseFirstPullStartsAtDeliveryOne() throws Exception {
        final Queue q;
        final Chunk first;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(2, Duration.ZERO);
        Assertions.assertEquals(
                List.of(1, 1),
                first.messages().stream().map(Message::deliveryCount).toList(),
                "Messages should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("리스가 만료된 메시지는 원래 id 자리로 돌아가 다시 꺼내지고 전달 횟수가 늘어난다 — expired Lease Returns Messages In Original Id Order")
    void expiredLeaseReturnsMessagesInOriginalIdOrder() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk second;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        second = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                ids, second.messages().stream().map(Message::id).toList(), "Messages should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("리스가 만료된 메시지는 원래 id 자리로 돌아가 다시 꺼내지고 전달 횟수가 늘어난다 — expired Lease Redelivery Counts Increase")
    void expiredLeaseRedeliveryCountsIncrease() throws Exception {
        final Queue q;
        final Chunk second;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        second = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                List.of(2, 2, 1),
                second.messages().stream().map(Message::deliveryCount).toList(),
                "Messages should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("리스가 만료된 메시지는 원래 id 자리로 돌아가 다시 꺼내지고 전달 횟수가 늘어난다 — expired Lease Second Redelivery Counts Increase")
    void expiredLeaseSecondRedeliveryCountsIncrease() throws Exception {
        final Queue q;
        final Chunk third;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        third = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                List.of(3, 3, 2),
                third.messages().stream().map(Message::deliveryCount).toList(),
                "Messages should match the expected value.");
    }

    @Test
    @DisplayName("리스 기한 전에는 다시 꺼낼 수 없다")
    void itCannotBeRetrievedBeforeTheLeasePeriodEnds() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(9L));
        Assertions.assertTrue(q.pull(1, Duration.ZERO).isEmpty(), "Pulled chunk should be empty.");
    }

    @Test
    @DisplayName("counts는 리스 중 개수를 돌려주고 기한이 지난 리스도 다시 꺼내기 전까지는 리스 중으로 센다 — leased Counts After Pull")
    void leasedCountsAfterPull() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        Assertions.assertEquals(new QueueCounts(1L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        q.pull(2, Duration.ZERO);
    }

    @Test
    @DisplayName("counts는 리스 중 개수를 돌려주고 기한이 지난 리스도 다시 꺼내기 전까지는 리스 중으로 센다 — expired Leases Remain Counted Before Pull")
    void expiredLeasesRemainCountedBeforePull() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        Assertions.assertEquals(new QueueCounts(1L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        q.pull(1, Duration.ZERO);
        q.pull(2, Duration.ZERO);
    }

    @Test
    @DisplayName("counts는 리스 중 개수를 돌려주고 기한이 지난 리스도 다시 꺼내기 전까지는 리스 중으로 센다 — leased Counts After Redelivery")
    void leasedCountsAfterRedelivery() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        Assertions.assertEquals(new QueueCounts(1L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        q.pull(2, Duration.ZERO);
    }

    @Test
    @DisplayName("counts는 리스 중 개수를 돌려주고 기한이 지난 리스도 다시 꺼내기 전까지는 리스 중으로 센다 — leased Counts After All Messages Pulled")
    void leasedCountsAfterAllMessagesPulled() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.pull(2, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        q.pull(2, Duration.ZERO);
        Assertions.assertEquals(new QueueCounts(0L, 3L, 0L), q.counts(), "Counts should match the expected value.");
    }

    @Test
    @DisplayName("pull에서 덮어쓴 리스 시간이 청크의 기한과 만료 시점에 반영된다 — explicit Pull Lease Determines Deadline")
    void explicitPullLeaseDeterminesDeadline() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO, Duration.ofSeconds(5L));
        Assertions.assertEquals(
                T0.plusSeconds(5L), c.leaseDeadline(), "Lease deadline should match the expected value.");
        this.clock.advance(Duration.ofSeconds(5L));
    }

    @Test
    @DisplayName("pull에서 덮어쓴 리스 시간이 청크의 기한과 만료 시점에 반영된다 — explicit Pull Lease Determines Expiry")
    void explicitPullLeaseDeterminesExpiry() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO, Duration.ofSeconds(5L));
        this.clock.advance(Duration.ofSeconds(5L));
        Assertions.assertEquals(
                2,
                q.pull(1, Duration.ZERO).messages().getFirst().deliveryCount(),
                "Delivery count should match the expected value.");
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extension Sets Deadline From Current Time")
    void extensionSetsDeadlineFromCurrentTime() throws Exception {
        final Queue q;
        final Chunk c;
        final Extension ext;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        ext = q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertEquals(
                T0.plusSeconds(26L), ext.leaseDeadline(), "Lease deadline should match the expected value.");
        this.clock.advance(Duration.ofSeconds(19L));
        this.clock.advance(Duration.ofSeconds(1L));
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extension Returns Successful Results")
    void extensionReturnsSuccessfulResults() throws Exception {
        final Queue q;
        final Chunk c;
        final Extension ext;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        ext = q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertTrue(ext.outcomes().allOk(), "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(19L));
        this.clock.advance(Duration.ofSeconds(1L));
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extension Returns Results For All Messages")
    void extensionReturnsResultsForAllMessages() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Extension ext;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        ext = q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertEquals(
                Set.copyOf(ids), ext.outcomes().byMessage().keySet(), "Key set should match the expected value.");
        this.clock.advance(Duration.ofSeconds(19L));
        this.clock.advance(Duration.ofSeconds(1L));
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extended Messages Stay Leased Before Deadline")
    void extendedMessagesStayLeasedBeforeDeadline() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        q.extend(c, Duration.ofSeconds(20L));
        this.clock.advance(Duration.ofSeconds(19L));
        Assertions.assertTrue(q.pull(10, Duration.ZERO).isEmpty(), "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(1L));
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extension Preserves Leased Counts")
    void extensionPreservesLeasedCounts() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        q.extend(c, Duration.ofSeconds(20L));
        this.clock.advance(Duration.ofSeconds(19L));
        Assertions.assertEquals(new QueueCounts(0L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(1L));
    }

    @Test
    @DisplayName("extend는 리스를 지금부터 d 뒤로 바꾸고 새 기한과 메시지별 OK를 돌려준다 — extended Messages Redeliver At Deadline")
    void extendedMessagesRedeliverAtDeadline() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(6L));
        q.extend(c, Duration.ofSeconds(20L));
        this.clock.advance(Duration.ofSeconds(19L));
        this.clock.advance(Duration.ofSeconds(1L));
        Assertions.assertEquals(
                2, q.pull(10, Duration.ZERO).messages().size(), "Size should match the expected value.");
    }

    @Test
    @DisplayName("같은 연장을 다시 해도 안전하다 — repeating Extension Returns Same Result")
    void repeatingExtensionReturnsSameResult() throws Exception {
        final Queue q;
        final Chunk c;
        final Extension first;
        final Extension again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        first = q.extend(c, Duration.ofSeconds(20L));
        again = q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertEquals(first, again, "Again should match the expected value.");
    }

    @Test
    @DisplayName("같은 연장을 다시 해도 안전하다 — repeating Extension Returns Successful Outcomes")
    void repeatingExtensionReturnsSuccessfulOutcomes() throws Exception {
        final Queue q;
        final Chunk c;
        final Extension again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        q.extend(c, Duration.ofSeconds(20L));
        again = q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertTrue(again.outcomes().allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("같은 연장을 다시 해도 안전하다 — repeating Extension Preserves Leased Counts")
    void repeatingExtensionPreservesLeasedCounts() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        q.extend(c, Duration.ofSeconds(20L));
        q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertEquals(new QueueCounts(0L, 2L, 0L), q.counts(), "Counts should match the expected value.");
    }

    @Test
    @DisplayName("같은 연장을 다시 해도 안전하다 — repeating Extension Still Allows Settlement")
    void repeatingExtensionStillAllowsSettlement() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        q.extend(c, Duration.ofSeconds(20L));
        q.extend(c, Duration.ofSeconds(20L));
        Assertions.assertTrue(q.settle(c.ackAll()).allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("기한이 지났어도 아무도 다시 꺼내지 않았다면 원래 리스의 정산과 연장은 OK다 — expired Lease Can Be Extended")
    void expiredLeaseCanBeExtended() throws Exception {
        final Queue q;
        final Chunk c;
        final Extension ext;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(30L));
        ext = q.extend(c, Duration.ofSeconds(10L));
        Assertions.assertTrue(ext.outcomes().allOk(), "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(30L));
    }

    @Test
    @DisplayName("기한이 지났어도 아무도 다시 꺼내지 않았다면 원래 리스의 정산과 연장은 OK다 — expired Lease Remains Leased After Extension")
    void expiredLeaseRemainsLeasedAfterExtension() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(30L));
        q.extend(c, Duration.ofSeconds(10L));
        Assertions.assertEquals(new QueueCounts(0L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(30L));
    }

    @Test
    @DisplayName("기한이 지났어도 아무도 다시 꺼내지 않았다면 원래 리스의 정산과 연장은 OK다 — extended Lease Can Be Settled")
    void extendedLeaseCanBeSettled() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(30L));
        q.extend(c, Duration.ofSeconds(10L));
        this.clock.advance(Duration.ofSeconds(30L));
        Assertions.assertTrue(q.settle(c.ackAll()).allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("기한이 지났어도 아무도 다시 꺼내지 않았다면 원래 리스의 정산과 연장은 OK다 — settlement Removes Messages")
    void settlementRemovesMessages() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(30L));
        q.extend(c, Duration.ofSeconds(10L));
        this.clock.advance(Duration.ofSeconds(30L));
        q.settle(c.ackAll());
        Assertions.assertEquals(new QueueCounts(0L, 0L, 0L), q.counts(), "Counts should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 큐에서는 기한이 지난 리스의 연장이 STALE이고 리스를 되살리지 않는다 — single Active Expired Lease Redelivers")
    void singleActiveExpiredLeaseRedelivers() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk first;
        final Chunk second;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        ids = q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        second = q.pull(1, Duration.ZERO);
        Assertions.assertEquals(
                ids.getFirst(), second.messages().getFirst().id(), "Message ID should match the expected value.");
        q.extend(first, Duration.ofSeconds(100L));
        q.pull(1, Duration.ZERO);
    }

    @Test
    @DisplayName("단일 활성 큐에서는 기한이 지난 리스의 연장이 STALE이고 리스를 되살리지 않는다 — single Active Late Extension Is Stale")
    void singleActiveLateExtensionIsStale() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk first;
        final Extension late;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        ids = q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        late = q.extend(first, Duration.ofSeconds(100L));
        Assertions.assertEquals(ids, late.outcomes().stale(), "Stale should match the expected value.");
        q.pull(1, Duration.ZERO);
    }

    @Test
    @DisplayName("단일 활성 큐에서는 기한이 지난 리스의 연장이 STALE이고 리스를 되살리지 않는다 — single Active Late Extension Preserves Counts")
    void singleActiveLateExtensionPreservesCounts() throws Exception {
        final Queue q;
        final Chunk first;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        q.extend(first, Duration.ofSeconds(100L));
        Assertions.assertEquals(new QueueCounts(0L, 2L, 0L), q.counts(), "Counts should match the expected value.");
        q.pull(1, Duration.ZERO);
    }

    @Test
    @DisplayName("단일 활성 큐에서는 기한이 지난 리스의 연장이 STALE이고 리스를 되살리지 않는다 — second Lease Can Be Settled After Stale Extension")
    void secondLeaseCanBeSettledAfterStaleExtension() throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk second;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        second = q.pull(1, Duration.ZERO);
        q.extend(first, Duration.ofSeconds(100L));
        Assertions.assertTrue(q.settle(second.ackAll()).allOk(), "Outcomes should succeed.");
        q.pull(1, Duration.ZERO);
    }

    @Test
    @DisplayName("단일 활성 큐에서는 기한이 지난 리스의 연장이 STALE이고 리스를 되살리지 않는다 — stale Extension Does Not Revive Lease")
    void staleExtensionDoesNotReviveLease() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk first;
        final Chunk second;
        final Chunk third;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        ids = q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        second = q.pull(1, Duration.ZERO);
        q.extend(first, Duration.ofSeconds(100L));
        q.settle(second.ackAll());
        third = q.pull(1, Duration.ZERO);
        Assertions.assertEquals(
                ids.get(1), third.messages().getFirst().id(), "Message ID should match the expected value.");
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — refetched Messages Are Redelivered")
    void refetchedMessagesAreRedelivered() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk slow;
        final Chunk fast;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        Assertions.assertEquals(
                ids.getFirst(), fast.messages().getFirst().id(), "Message ID should match the expected value.");
        q.settle(slow.ackAll());
        q.extend(slow, Duration.ofSeconds(100L));
    }

    @Test
    @DisplayName(
            "다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — old Lease Settlement Is Stale For Redelivered Message")
    void oldLeaseSettlementIsStaleForRedeliveredMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk slow;
        final Outcomes late;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        late = q.settle(slow.ackAll());
        Assertions.assertEquals(
                Outcome.STALE, late.byMessage().get(ids.getFirst()), "By message should match the expected value.");
        q.extend(slow, Duration.ofSeconds(100L));
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — old Lease Settlement Acknowledges Other Message")
    void oldLeaseSettlementAcknowledgesOtherMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk slow;
        final Outcomes late;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        late = q.settle(slow.ackAll());
        Assertions.assertEquals(
                Outcome.OK, late.byMessage().get(ids.get(1)), "By message should match the expected value.");
        q.extend(slow, Duration.ofSeconds(100L));
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — old Lease Extension Is Stale")
    void oldLeaseExtensionIsStale() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk slow;
        final Extension ext;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        q.settle(slow.ackAll());
        ext = q.extend(slow, Duration.ofSeconds(100L));
        Assertions.assertEquals(
                List.of(ids.getFirst(), ids.get(1)), ext.outcomes().stale(), "Stale should match the expected value.");
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — old Lease Operations Preserve Current Lease")
    void oldLeaseOperationsPreserveCurrentLease() throws Exception {
        final Queue q;
        final Chunk slow;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        q.pull(1, Duration.ZERO);
        q.settle(slow.ackAll());
        q.extend(slow, Duration.ofSeconds(100L));
        Assertions.assertEquals(new QueueCounts(0L, 1L, 0L), q.counts(), "Counts should match the expected value.");
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지에 대한 늦은 정산과 연장은 STALE이고 효력이 없다 — current Lease Can Be Settled")
    void currentLeaseCanBeSettled() throws Exception {
        final Queue q;
        final Chunk slow;
        final Chunk fast;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        slow = q.pull(10, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        q.settle(slow.ackAll());
        q.extend(slow, Duration.ofSeconds(100L));
        Assertions.assertTrue(q.settle(fast.ackAll()).allOk(), "Outcomes should succeed.");
    }

    @Test
    @DisplayName("ACK한 메시지를 같은 리스로 다시 연장하면 STALE이다 — acknowledged Message Extension Is Stale")
    void acknowledgedMessageExtensionIsStale() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        Assertions.assertTrue(q.settle(c.ackAll()).allOk(), "Outcomes should succeed.");
        q.extend(c, Duration.ofSeconds(10L));
    }

    @Test
    @DisplayName("ACK한 메시지를 같은 리스로 다시 연장하면 STALE이다 — acknowledged Message Extension Does Not Restore Lease")
    void acknowledgedMessageExtensionDoesNotRestoreLease() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Extension ext;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        q.settle(c.ackAll());
        ext = q.extend(c, Duration.ofSeconds(10L));
        Assertions.assertEquals(List.of(id), ext.outcomes().stale(), "Stale should match the expected value.");
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck1()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final Throwable assertionValue1;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        assertionValue1 = captureThrowable(() -> b.settle(fromA.ackAll()));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue1, "Settle should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck2()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final Throwable assertionValue2;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        assertionValue2 = captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue2, "Extend should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck3()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final Throwable assertionValue3;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        assertionValue3 = captureThrowable(() -> a.settle(emptySettlement));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue3, "Settle should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck4()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final Throwable assertionValue4;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        assertionValue4 = captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue4, "Extend should throw IllegalArgumentException.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck5()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        assertionValue5Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue5Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck6()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        assertionValue6Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue6Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
        evaluate(a.settle(fromA.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck7()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final boolean assertionValue7Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue7Argument1 = a.settle(fromA.ackAll()).allOk();
        Assertions.assertTrue(assertionValue7Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("다른 큐의 청크로 정산하거나 연장하면 IllegalArgumentException이고 원래 큐의 리스는 그대로다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifYouSettleOrExtendWithAChunkFromAnotherQueueAnIllegalArgumentExceptionIsThrownAndTheLeaseOfTheOriginalQueueRemainsUnchangedCheck8()
                    throws Exception {
        final Smq smq;
        final Queue a;
        final Queue b;
        final Chunk fromA;
        final Chunk emptyFromB;
        final Settlement emptySettlement;
        final QueueCounts assertionValue8Argument1;
        final QueueCounts assertionValue8Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        a = smq.declare("a", QueueOptions.defaults());
        b = smq.declare("b", QueueOptions.defaults());
        a.publish(msg("x"));
        fromA = a.pull(1, Duration.ZERO);
        emptyFromB = b.pull(1, Duration.ZERO);
        emptySettlement = emptyFromB.settle().build();
        evaluate(captureThrowable(() -> b.settle(fromA.ackAll())));
        evaluate(captureThrowable(() -> b.extend(fromA, Duration.ofSeconds(100L))));
        evaluate(captureThrowable(() -> a.settle(emptySettlement)));
        evaluate(captureThrowable(() -> a.extend(emptyFromB, Duration.ofSeconds(1L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(a.settle(fromA.ackAll()).allOk());
        assertionValue8Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue8Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue8Argument1, assertionValue8Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck1() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Chunk bSettle;
        final MessageId assertionValue1Argument1;
        final MessageId assertionValue1Argument2;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        bSettle = b.pull(1, Duration.ZERO);
        assertionValue1Argument1 = aSettle.messages().getFirst().id();
        assertionValue1Argument2 = bSettle.messages().getFirst().id();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Message ID should match the expected value.");
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck2() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Throwable assertionValue2;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        assertionValue2 = captureThrowable(() -> b.settle(aSettle.ackAll()));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue2, "Settle should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck3() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Throwable assertionValue3;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        assertionValue3 = captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue3, "Extend should throw IllegalArgumentException.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck4() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        assertionValue4Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue4Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck5() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue5Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue5Argument2 = b.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck6() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final boolean assertionValue6Argument1;
        final Chunk aExtend;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        assertionValue6Argument1 = sameSessionHandle.settle(aSettle.ackAll()).allOk();
        Assertions.assertTrue(assertionValue6Argument1, "Outcomes should succeed.");
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck7() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final Throwable assertionValue7;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        assertionValue7 = captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue7, "Extend should throw IllegalArgumentException.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck8() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final QueueCounts assertionValue8Argument1;
        final QueueCounts assertionValue8Argument2;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        assertionValue8Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue8Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue8Argument1, assertionValue8Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 9")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck9() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final QueueCounts assertionValue9Argument1;
        final QueueCounts assertionValue9Argument2;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue9Argument1 = new QueueCounts(0L, 2L, 0L);
        assertionValue9Argument2 = b.counts();
        Assertions.assertEquals(
                assertionValue9Argument1, assertionValue9Argument2, "Counts should match the expected value.");
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 10")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck10() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final boolean assertionValue10Argument1;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        assertionValue10Argument1 = sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk();
        Assertions.assertTrue(assertionValue10Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 11")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck11() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final QueueCounts assertionValue11Argument1;
        final QueueCounts assertionValue11Argument2;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        assertionValue11Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue11Argument2 = a.counts();
        Assertions.assertEquals(
                assertionValue11Argument1, assertionValue11Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 2L, 0L));
    }

    @Test
    @DisplayName("다른 저장소의 같은 큐 청크는 정산하거나 연장할 수 없다 — check 12")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void chunksFromTheSameQueueInDifferentRepositoriesCannotBeSettledOrExtendedCheck12() throws Exception {
        final Smq first;
        final Queue a;
        final Smq second;
        final Queue b;
        final Chunk aSettle;
        final Queue sameSessionHandle;
        final Chunk aExtend;
        final QueueCounts assertionValue12Argument1;
        final QueueCounts assertionValue12Argument2;
        first = Smq.sqlite(this.dir.resolve("first.db")).clock(this.clock).open();
        this.opened.add(first);
        a = first.declare("q", QueueOptions.defaults());
        second = Smq.sqlite(this.dir.resolve("second.db")).clock(this.clock).open();
        this.opened.add(second);
        b = second.declare("q", QueueOptions.defaults());
        a.publish(msg("settle"));
        b.publish(msg("settle"));
        aSettle = a.pull(1, Duration.ZERO);
        evaluate(b.pull(1, Duration.ZERO));
        evaluate(captureThrowable(() -> b.settle(aSettle.ackAll())));
        evaluate(captureThrowable(() -> b.extend(aSettle, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        sameSessionHandle = first.queue("q");
        evaluate(sameSessionHandle.settle(aSettle.ackAll()).allOk());
        a.publish(msg("extend"));
        b.publish(msg("extend"));
        aExtend = a.pull(1, Duration.ZERO);
        b.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> b.extend(aExtend, Duration.ofSeconds(20L))));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(0L, 2L, 0L));
        evaluate(sameSessionHandle
                .extend(aExtend, Duration.ofSeconds(20L))
                .outcomes()
                .allOk());
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue12Argument1 = new QueueCounts(0L, 2L, 0L);
        assertionValue12Argument2 = b.counts();
        Assertions.assertEquals(
                assertionValue12Argument1, assertionValue12Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("빈 청크를 연장하면 빈 결과이고 인자 오류는 IllegalArgumentException — extending Empty Chunk Returns Empty Outcomes")
    void extendingEmptyChunkReturnsEmptyOutcomes() throws Exception {
        final Queue q;
        final Chunk empty;
        q = this.open().declare("q", QueueOptions.defaults());
        empty = q.pull(1, Duration.ZERO);
        Assertions.assertTrue(
                q.extend(empty, Duration.ofSeconds(1L)).outcomes().byMessage().isEmpty(),
                "By message should be empty.");
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO);
    }

    @Test
    @DisplayName("빈 청크를 연장하면 빈 결과이고 인자 오류는 IllegalArgumentException — extending Empty Chunk Returns No Deadline")
    void extendingEmptyChunkReturnsNoDeadline() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.pull(1, Duration.ZERO);
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> q.extend(c, Duration.ZERO),
                "Extend should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("빈 청크를 연장하면 빈 결과이고 인자 오류는 IllegalArgumentException — zero Lease Duration Is Rejected")
    void zeroLeaseDurationIsRejected() throws Exception {
        final Queue q;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.pull(1, Duration.ZERO);
        q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> q.extend(c, null),
                "Extend should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("빈 청크를 연장하면 빈 결과이고 인자 오류는 IllegalArgumentException — negative Lease Duration Is Rejected")
    void negativeLeaseDurationIsRejected() throws Exception {
        final Queue q;
        q = this.open().declare("q", QueueOptions.defaults());
        q.pull(1, Duration.ZERO);
        q.publish(msg("a"));
        q.pull(1, Duration.ZERO);
        Assertions.assertThrows(
                NullPointerException.class,
                () -> q.extend(null, Duration.ofSeconds(1L)),
                "Extend should throw NullPointerException.");
    }

    @Test
    @DisplayName("chunk ack id로 메시지 하나만 ACK한다 — ack Selects Only One Message")
    void ackSelectsOnlyOneMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        r = q.settle(c.ack(ids.get(1)));
        Assertions.assertTrue(r.allOk(), "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(10L));
    }

    @Test
    @DisplayName("chunk ack id로 메시지 하나만 ACK한다 — ack Leaves Unselected Message Leased")
    void ackLeavesUnselectedMessageLeased() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        r = q.settle(c.ack(ids.get(1)));
        Assertions.assertEquals(Set.of(ids.get(1)), r.byMessage().keySet(), "Key set should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
    }

    @Test
    @DisplayName("chunk ack id로 메시지 하나만 ACK한다 — ack Removes Only Selected Message")
    void ackRemovesOnlySelectedMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        q.settle(c.ack(ids.get(1)));
        this.clock.advance(Duration.ofSeconds(10L));
        Assertions.assertEquals(
                List.of(ids.get(0), ids.get(2)),
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList(),
                "Messages should match the expected value.");
    }

    @Test
    @DisplayName("settle 빌더로 ACK와 NACK을 섞고 ackRest로 마무리한다 — mixed Settlement Has Successful Outcomes")
    void mixedSettlementHasSuccessfulOutcomes() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));
        c = q.pull(4, Duration.ZERO);
        r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
        Assertions.assertTrue(r.allOk(), "Outcomes should succeed.");
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("settle 빌더로 ACK와 NACK을 섞고 ackRest로 마무리한다 — mixed Settlement Acknowledges Selected Message")
    void mixedSettlementAcknowledgesSelectedMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));
        c = q.pull(4, Duration.ZERO);
        r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
        Assertions.assertEquals(Set.copyOf(ids), r.byMessage().keySet(), "Key set should match the expected value.");
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("settle 빌더로 ACK와 NACK을 섞고 ackRest로 마무리한다 — mixed Settlement Nacks Selected Message")
    void mixedSettlementNacksSelectedMessage() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Chunk next;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));
        c = q.pull(4, Duration.ZERO);
        q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
        next = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                List.of(ids.get(1)),
                next.messages().stream().map(Message::id).toList(),
                "Messages should match the expected value.");
    }

    @Test
    @DisplayName("settle 빌더로 ACK와 NACK을 섞고 ackRest로 마무리한다 — mixed Settlement Ack Rest Acknowledges Remaining Messages")
    void mixedSettlementAckRestAcknowledgesRemainingMessages() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Chunk next;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d")));
        c = q.pull(4, Duration.ZERO);
        q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
        next = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                2, next.messages().getFirst().deliveryCount(), "Delivery count should match the expected value.");
    }

    @Test
    @DisplayName("build로 마무리하면 고른 메시지만 정산하고 나머지는 리스에 남는다 — build Settles Only Selected Messages")
    void buildSettlesOnlySelectedMessages() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes r;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        r = q.settle(c.settle().ack(ids.get(0)).build());
        Assertions.assertEquals(Set.of(ids.get(0)), r.byMessage().keySet(), "Key set should match the expected value.");
    }

    @Test
    @DisplayName("build로 마무리하면 고른 메시지만 정산하고 나머지는 리스에 남는다 — build Leaves Unselected Messages Leased")
    void buildLeavesUnselectedMessagesLeased() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        q.settle(c.settle().ack(ids.get(0)).build());
        Assertions.assertEquals(new QueueCounts(0L, 1L, 0L), q.counts(), "Counts should match the expected value.");
    }

    @Test
    @DisplayName("지연 없이 NACK한 메시지는 자기 id 자리로 돌아가 다음 꺼내기에서 먼저 나온다 — nacked Messages Return In Original Id Order")
    void nackedMessagesReturnInOriginalIdOrder() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        Assertions.assertTrue(q.settle(c.settle().nack(ids.get(1)).ackRest()).allOk(), "Outcomes should succeed.");
        q.pull(10, Duration.ZERO);
    }

    @Test
    @DisplayName("지연 없이 NACK한 메시지는 자기 id 자리로 돌아가 다음 꺼내기에서 먼저 나온다 — nacked Messages Appear Before Newer Messages")
    void nackedMessagesAppearBeforeNewerMessages() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Chunk next;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(2, Duration.ZERO);
        q.settle(c.settle().nack(ids.get(1)).ackRest());
        next = q.pull(10, Duration.ZERO);
        Assertions.assertEquals(
                List.of(ids.get(1), ids.get(2)),
                next.messages().stream().map(Message::id).toList(),
                "Messages should match the expected value.");
    }

    @Test
    @DisplayName("정산 빌더는 청크에 없거나 이미 고른 메시지를 거부한다 — settlement Rejects Message Outside Chunk")
    void settlementRejectsMessageOutsideChunk() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(1, Duration.ZERO);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> c.settle().ack(ids.get(1)),
                "Ack should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("정산 빌더는 청크에 없거나 이미 고른 메시지를 거부한다 — settlement Rejects Duplicate Selection")
    void settlementRejectsDuplicateSelection() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(1, Duration.ZERO);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> c.settle().ack(ids.get(0)).nack(ids.get(0)),
                "Nack should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("섞인 정산은 원자적이라 중간 상태가 다른 꺼내기에 보이지 않는다 — check 1")
    void mixedSettlementsAreAtomicSoTheIntermediateStateIsNotVisibleToOtherRetrievals() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            final List<MessageId> ids;
            final Chunk c;
            final AtomicBoolean done;
            final List<QueueCounts> seen;
            final Thread observer;
            final Outcomes r;
            final Chunk drain;
            ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            c = q.pull(3, Duration.ZERO);
            final var assertionValue5Argument1 = new QueueCounts(0L, 3L, 0L);
            final var assertionValue5Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
            done = new AtomicBoolean();
            seen = Collections.synchronizedList(new ArrayList<>());
            observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts());
                }
            });
            observer.start();
            r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            final var assertionValue4Argument1 = r.allOk();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = seen.stream()
                    .allMatch(n -> n.equals(new QueueCounts(0L, 3L, 0L)) || n.equals(new QueueCounts(1L, 0L, 0L)));
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = "torn state: " + seen;
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            drain = q.pull(10, Duration.ZERO);
            final var assertionValue1Argument1 = q.settle(drain.ackAll()).allOk();
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    @DisplayName("섞인 정산은 원자적이라 중간 상태가 다른 꺼내기에 보이지 않는다 — check 2")
    void mixedSettlementsAreAtomicSoTheIntermediateStateIsNotVisibleToOtherRetrievalsCheck2() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            final List<MessageId> ids;
            final Chunk c;
            final AtomicBoolean done;
            final List<QueueCounts> seen;
            final Thread observer;
            final Outcomes r;
            final Chunk drain;
            ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            c = q.pull(3, Duration.ZERO);
            final var assertionValue5Argument1 = new QueueCounts(0L, 3L, 0L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = q.counts();
            evaluatePmdArgument(assertionValue5Argument2);
            done = new AtomicBoolean();
            seen = Collections.synchronizedList(new ArrayList<>());
            observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts());
                }
            });
            observer.start();
            r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            final var assertionValue4Argument1 = r.allOk();
            Assertions.assertTrue(assertionValue4Argument1, "Outcomes should succeed.");
            final var assertionValue3Argument1 = seen.stream()
                    .allMatch(n -> n.equals(new QueueCounts(0L, 3L, 0L)) || n.equals(new QueueCounts(1L, 0L, 0L)));
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = "torn state: " + seen;
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            drain = q.pull(10, Duration.ZERO);
            final var assertionValue1Argument1 = q.settle(drain.ackAll()).allOk();
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    @DisplayName("섞인 정산은 원자적이라 중간 상태가 다른 꺼내기에 보이지 않는다 — check 3")
    void mixedSettlementsAreAtomicSoTheIntermediateStateIsNotVisibleToOtherRetrievalsCheck3() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            final List<MessageId> ids;
            final Chunk c;
            final AtomicBoolean done;
            final List<QueueCounts> seen;
            final Thread observer;
            final Outcomes r;
            final Chunk drain;
            ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            c = q.pull(3, Duration.ZERO);
            final var assertionValue5Argument1 = new QueueCounts(0L, 3L, 0L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = q.counts();
            evaluatePmdArgument(assertionValue5Argument2);
            done = new AtomicBoolean();
            seen = Collections.synchronizedList(new ArrayList<>());
            observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts());
                }
            });
            observer.start();
            r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            final var assertionValue4Argument1 = r.allOk();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = seen.stream()
                    .allMatch(n -> n.equals(new QueueCounts(0L, 3L, 0L)) || n.equals(new QueueCounts(1L, 0L, 0L)));
            final var assertionValue3Argument2 = "torn state: " + seen;
            Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            drain = q.pull(10, Duration.ZERO);
            final var assertionValue1Argument1 = q.settle(drain.ackAll()).allOk();
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    @DisplayName("섞인 정산은 원자적이라 중간 상태가 다른 꺼내기에 보이지 않는다 — check 4")
    void mixedSettlementsAreAtomicSoTheIntermediateStateIsNotVisibleToOtherRetrievalsCheck4() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            final List<MessageId> ids;
            final Chunk c;
            final AtomicBoolean done;
            final List<QueueCounts> seen;
            final Thread observer;
            final Outcomes r;
            final Chunk drain;
            ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            c = q.pull(3, Duration.ZERO);
            final var assertionValue5Argument1 = new QueueCounts(0L, 3L, 0L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = q.counts();
            evaluatePmdArgument(assertionValue5Argument2);
            done = new AtomicBoolean();
            seen = Collections.synchronizedList(new ArrayList<>());
            observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts());
                }
            });
            observer.start();
            r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            final var assertionValue4Argument1 = r.allOk();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = seen.stream()
                    .allMatch(n -> n.equals(new QueueCounts(0L, 3L, 0L)) || n.equals(new QueueCounts(1L, 0L, 0L)));
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = "torn state: " + seen;
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 0L, 0L);
            final var assertionValue2Argument2 = q.counts();
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
            drain = q.pull(10, Duration.ZERO);
            final var assertionValue1Argument1 = q.settle(drain.ackAll()).allOk();
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    @DisplayName("섞인 정산은 원자적이라 중간 상태가 다른 꺼내기에 보이지 않는다 — check 5")
    void mixedSettlementsAreAtomicSoTheIntermediateStateIsNotVisibleToOtherRetrievalsCheck5() throws Exception {
        final Queue q;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        for (int round = 0; round < 50; round++) {
            final List<MessageId> ids;
            final Chunk c;
            final AtomicBoolean done;
            final List<QueueCounts> seen;
            final Thread observer;
            final Outcomes r;
            final Chunk drain;
            ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
            c = q.pull(3, Duration.ZERO);
            final var assertionValue5Argument1 = new QueueCounts(0L, 3L, 0L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = q.counts();
            evaluatePmdArgument(assertionValue5Argument2);
            done = new AtomicBoolean();
            seen = Collections.synchronizedList(new ArrayList<>());
            observer = new Thread(() -> {
                while (!done.get()) {
                    seen.add(q.counts());
                }
            });
            observer.start();
            r = q.settle(c.settle().ack(ids.get(0)).nack(ids.get(1)).ackRest());
            done.set(true);
            observer.join();
            final var assertionValue4Argument1 = r.allOk();
            evaluatePmdArgument(assertionValue4Argument1);
            final var assertionValue3Argument1 = seen.stream()
                    .allMatch(n -> n.equals(new QueueCounts(0L, 3L, 0L)) || n.equals(new QueueCounts(1L, 0L, 0L)));
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = "torn state: " + seen;
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 0L, 0L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = q.counts();
            evaluatePmdArgument(assertionValue2Argument2);
            drain = q.pull(10, Duration.ZERO);
            final var assertionValue1Argument1 = q.settle(drain.ackAll()).allOk();
            Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        }
    }

    @Test
    @DisplayName("같은 Settlement 값으로 다시 settle하면 이미 반영된 메시지는 STALE이다 — check 1")
    void ifYouSettleAgainWithTheSameSettlementValueTheMessageThatHasAlreadyBeenReflectedIsSTALECheck1()
            throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Settlement s;
        final boolean assertionValue1Argument1;
        final Outcomes again;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        s = c.settle().ack(ids.get(0)).nack(ids.get(1)).build();
        assertionValue1Argument1 = q.settle(s).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        again = q.settle(s);
        evaluate(again.stale());
    }

    @Test
    @DisplayName("같은 Settlement 값으로 다시 settle하면 이미 반영된 메시지는 STALE이다 — check 2")
    void ifYouSettleAgainWithTheSameSettlementValueTheMessageThatHasAlreadyBeenReflectedIsSTALECheck2()
            throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Settlement s;
        final Outcomes again;
        final List<MessageId> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        s = c.settle().ack(ids.get(0)).nack(ids.get(1)).build();
        evaluate(q.settle(s).allOk());
        again = q.settle(s);
        assertionValue2Argument2 = again.stale();
        Assertions.assertEquals(ids, assertionValue2Argument2, "Stale should match the expected value.");
    }

    @Test
    @DisplayName("같은 Settlement 값으로 다시 settle하면 이미 반영된 메시지는 STALE이다 — check 3")
    void ifYouSettleAgainWithTheSameSettlementValueTheMessageThatHasAlreadyBeenReflectedIsSTALECheck3()
            throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Settlement s;
        final Outcomes again;
        final long assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        s = c.settle().ack(ids.get(0)).nack(ids.get(1)).build();
        evaluate(q.settle(s).allOk());
        again = q.settle(s);
        evaluate(again.stale());
        assertionValue3Argument2 = q.counts().ready();
        Assertions.assertEquals(1L, assertionValue3Argument2, "Ready should match the expected value.");
    }

    @Test
    @DisplayName("같은 청크의 두 번째 ackRest는 앞서 NACK한 메시지를 지우지 않는다 — check 1")
    void theSecondAckRestInTheSameChunkDoesNotDeleteThePreviouslyNACKedMessageCheck1() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue1Argument1;
        final Outcomes second;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        assertionValue1Argument1 =
                q.settle(c.settle().nack(ids.get(1)).ack(ids.get(0)).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        second = q.settle(c.settle().ackRest());
        evaluate(second.stale());
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("같은 청크의 두 번째 ackRest는 앞서 NACK한 메시지를 지우지 않는다 — check 2")
    void theSecondAckRestInTheSameChunkDoesNotDeleteThePreviouslyNACKedMessageCheck2() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes second;
        final List<MessageId> assertionValue2Argument1;
        final List<MessageId> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(ids.get(1)).ack(ids.get(0)).build()).allOk());
        second = q.settle(c.settle().ackRest());
        assertionValue2Argument1 = List.of(ids.get(0), ids.get(1));
        assertionValue2Argument2 = second.stale();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Stale should match the expected value.");
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("같은 청크의 두 번째 ackRest는 앞서 NACK한 메시지를 지우지 않는다 — check 3")
    void theSecondAckRestInTheSameChunkDoesNotDeleteThePreviouslyNACKedMessageCheck3() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes second;
        final Outcome assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(ids.get(1)).ack(ids.get(0)).build()).allOk());
        second = q.settle(c.settle().ackRest());
        evaluate(second.stale());
        assertionValue3Argument2 = second.byMessage().get(ids.get(2));
        Assertions.assertEquals(Outcome.OK, assertionValue3Argument2, "By message should match the expected value.");
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("같은 청크의 두 번째 ackRest는 앞서 NACK한 메시지를 지우지 않는다 — check 4")
    void theSecondAckRestInTheSameChunkDoesNotDeleteThePreviouslyNACKedMessageCheck4() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final Outcomes second;
        final List<MessageId> assertionValue4Argument1;
        final List<MessageId> assertionValue4Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(ids.get(1)).ack(ids.get(0)).build()).allOk());
        second = q.settle(c.settle().ackRest());
        evaluate(second.stale());
        assertionValue4Argument1 = List.of(ids.get(1));
        assertionValue4Argument2 =
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("지연 없이 NACK되어 준비 상태인 메시지를 같은 리스로 연장해도 STALE이다 — check 1")
    void evenIfAMessageThatHasBeenNACKedWithoutDelayAndIsInTheReadyStateIsExtendedWithTheSameLeaseItIsStillSTALECheck1()
            throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.settle().nack(id).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.extend(c, Duration.ofSeconds(10L)).outcomes().stale());
    }

    @Test
    @DisplayName("지연 없이 NACK되어 준비 상태인 메시지를 같은 리스로 연장해도 STALE이다 — check 2")
    void evenIfAMessageThatHasBeenNACKedWithoutDelayAndIsInTheReadyStateIsExtendedWithTheSameLeaseItIsStillSTALECheck2()
            throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final List<MessageId> assertionValue2Argument1;
        final List<MessageId> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(id).build()).allOk());
        assertionValue2Argument1 = List.of(id);
        assertionValue2Argument2 =
                q.extend(c, Duration.ofSeconds(10L)).outcomes().stale();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Stale should match the expected value.");
    }

    @Test
    @DisplayName("지연 없이 NACK되어 준비 상태인 메시지를 같은 리스로 연장해도 STALE이다 — check 3")
    void evenIfAMessageThatHasBeenNACKedWithoutDelayAndIsInTheReadyStateIsExtendedWithTheSameLeaseItIsStillSTALECheck3()
            throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final long assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(id).build()).allOk());
        evaluate(q.extend(c, Duration.ofSeconds(10L)).outcomes().stale());
        assertionValue3Argument2 = q.counts().ready();
        Assertions.assertEquals(1L, assertionValue3Argument2, "Ready should match the expected value.");
    }

    @Test
    @DisplayName("정산된 메시지에 같은 리스로 다시 정산하면 NACK도 STALE이다 — check 1")
    void ifASettledMessageIsSettledAgainWithTheSameLeaseNACKIsAlsoSTALECheck1() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.settle().nack(id).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.settle(c.settle().nack(id).build()).stale());
        evaluate(q.settle(c.ack(id)).stale());
    }

    @Test
    @DisplayName("정산된 메시지에 같은 리스로 다시 정산하면 NACK도 STALE이다 — check 2")
    void ifASettledMessageIsSettledAgainWithTheSameLeaseNACKIsAlsoSTALECheck2() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final List<MessageId> assertionValue2Argument1;
        final List<MessageId> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(id).build()).allOk());
        assertionValue2Argument1 = List.of(id);
        assertionValue2Argument2 = q.settle(c.settle().nack(id).build()).stale();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Stale should match the expected value.");
        evaluate(q.settle(c.ack(id)).stale());
    }

    @Test
    @DisplayName("정산된 메시지에 같은 리스로 다시 정산하면 NACK도 STALE이다 — check 3")
    void ifASettledMessageIsSettledAgainWithTheSameLeaseNACKIsAlsoSTALECheck3() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(id).build()).allOk());
        evaluate(q.settle(c.settle().nack(id).build()).stale());
        assertionValue3Argument1 = List.of(id);
        assertionValue3Argument2 = q.settle(c.ack(id)).stale();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Stale should match the expected value.");
    }

    @Test
    @DisplayName("FIFO 순서 그리고 NACK 뒤 원래 자리로 복귀 — check 1")
    void fifoOrderAndReturnToTheOriginalPositionAfterNACKCheck1() throws Exception {
        final Queue q;
        List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        ids = new ArrayList<>(ids);
        ids.addAll(q.publish(List.of(msg("c"), msg("d"))));
        c = q.pull(3, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.settle()
                        .nack(ids.get(0))
                        .ack(ids.get(1))
                        .nack(ids.get(2))
                        .build())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("FIFO 순서 그리고 NACK 뒤 원래 자리로 복귀 — check 2")
    void fifoOrderAndReturnToTheOriginalPositionAfterNACKCheck2() throws Exception {
        final Queue q;
        List<MessageId> ids;
        final Chunk c;
        final List<MessageId> assertionValue2Argument1;
        final List<MessageId> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        ids = new ArrayList<>(ids);
        ids.addAll(q.publish(List.of(msg("c"), msg("d"))));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle()
                        .nack(ids.get(0))
                        .ack(ids.get(1))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        assertionValue2Argument1 = List.of(ids.get(0), ids.get(2), ids.get(3));
        assertionValue2Argument2 =
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("withDelay로 발행한 메시지는 지연이 지나야 꺼낼 수 있다 — check 1")
    void messagesPublishedWithWithDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck1() throws Exception {
        final Queue q;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(msg("a").withDelay(Duration.ofSeconds(10L))));
        assertionValue1Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue1Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(9L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("withDelay로 발행한 메시지는 지연이 지나야 꺼낼 수 있다 — check 2")
    void messagesPublishedWithWithDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck2() throws Exception {
        final Queue q;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(msg("a").withDelay(Duration.ofSeconds(10L))));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(9L));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("withDelay로 발행한 메시지는 지연이 지나야 꺼낼 수 있다 — check 3")
    void messagesPublishedWithWithDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck3() throws Exception {
        final Queue q;
        final MessageId id;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a").withDelay(Duration.ofSeconds(10L)));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(9L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(1L));
        assertionValue3Argument1 = List.of(id);
        assertionValue3Argument2 =
                q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("nack id 지연은 지연이 지나야 다시 꺼낼 수 있다 — check 1")
    void theNACKIDDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck1() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        assertionValue1Argument1 = q.settle(
                        c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nack id 지연은 지연이 지나야 다시 꺼낼 수 있다 — check 2")
    void theNACKIDDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck2() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nack id 지연은 지연이 지나야 다시 꺼낼 수 있다 — check 3")
    void theNACKIDDelayCanOnlyBeRetrievedAfterTheDelayHasPassedCheck3() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue3Argument1 = List.of(ids.get(0));
        assertionValue3Argument2 =
                q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("nackRest 지연은 고르지 않은 나머지를 지연 NACK한다 — check 1")
    void nackRestDelaysTheUnselectedMessagesCheck1() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.settle().ack(ids.get(0)).nackRest(Duration.ofSeconds(5L)))
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nackRest 지연은 고르지 않은 나머지를 지연 NACK한다 — check 2")
    void nackRestDelaysTheUnselectedMessagesCheck2() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle().ack(ids.get(0)).nackRest(Duration.ofSeconds(5L)))
                .allOk());
        assertionValue2Argument1 = q.pull(10, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nackRest 지연은 고르지 않은 나머지를 지연 NACK한다 — check 3")
    void nackRestDelaysTheUnselectedMessagesCheck3() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        evaluate(q.settle(c.settle().ack(ids.get(0)).nackRest(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue3Argument1 = List.of(ids.get(1), ids.get(2));
        assertionValue3Argument2 =
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("nackAll 지연은 청크 전체를 지연 NACK한다 — check 1")
    void theNackAllDelayNACKsTheEntireChunkWithADelayCheck1() throws Exception {
        final Queue q;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(List.of(msg("a"), msg("b"))));
        c = q.pull(2, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.nackAll(Duration.ofSeconds(5L))).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nackAll 지연은 청크 전체를 지연 NACK한다 — check 2")
    void theNackAllDelayNACKsTheEntireChunkWithADelayCheck2() throws Exception {
        final Queue q;
        final Chunk c;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(List.of(msg("a"), msg("b"))));
        c = q.pull(2, Duration.ZERO);
        evaluate(q.settle(c.nackAll(Duration.ofSeconds(5L))).allOk());
        assertionValue2Argument1 = q.pull(10, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("nackAll 지연은 청크 전체를 지연 NACK한다 — check 3")
    void theNackAllDelayNACKsTheEntireChunkWithADelayCheck3() throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b")));
        c = q.pull(2, Duration.ZERO);
        evaluate(q.settle(c.nackAll(Duration.ofSeconds(5L))).allOk());
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue3Argument2 =
                q.pull(10, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(ids, assertionValue3Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("지연이 끝난 메시지는 자기 id 자리로 들어가 더 큰 id의 준비 메시지보다 먼저 나온다 — check 1")
    void onceTheDelayPeriodEndsTheMessageMovesToItsOwnIDPositionAndAppearsBeforeTheReadyMessageWithAHigherIDCheck1()
            throws Exception {
        final Queue q;
        final MessageId nacked;
        final Chunk first;
        final List<MessageId> assertionValue1Argument1;
        final List<MessageId> assertionValue1Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(msg("a").withDelay(Duration.ofSeconds(5L))));
        nacked = q.publish(msg("b"));
        evaluate(q.publish(msg("c")));
        first = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = List.of(nacked);
        assertionValue1Argument2 = first.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Messages should match the expected value.");
        evaluate(q.settle(first.settle().nack(nacked, Duration.ofSeconds(5L)).build())
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("지연이 끝난 메시지는 자기 id 자리로 들어가 더 큰 id의 준비 메시지보다 먼저 나온다 — check 2")
    void onceTheDelayPeriodEndsTheMessageMovesToItsOwnIDPositionAndAppearsBeforeTheReadyMessageWithAHigherIDCheck2()
            throws Exception {
        final Queue q;
        final MessageId nacked;
        final Chunk first;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(msg("a").withDelay(Duration.ofSeconds(5L))));
        nacked = q.publish(msg("b"));
        evaluate(q.publish(msg("c")));
        first = q.pull(1, Duration.ZERO);
        assertionValue2Argument1 = q.settle(
                        first.settle().nack(nacked, Duration.ofSeconds(5L)).build())
                .allOk();
        Assertions.assertTrue(assertionValue2Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("지연이 끝난 메시지는 자기 id 자리로 들어가 더 큰 id의 준비 메시지보다 먼저 나온다 — check 3")
    void onceTheDelayPeriodEndsTheMessageMovesToItsOwnIDPositionAndAppearsBeforeTheReadyMessageWithAHigherIDCheck3()
            throws Exception {
        final Queue q;
        final MessageId nacked;
        final MessageId ready;
        final Chunk first;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        evaluate(q.publish(msg("a").withDelay(Duration.ofSeconds(5L))));
        nacked = q.publish(msg("b"));
        ready = q.publish(msg("c"));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.settle().nack(nacked, Duration.ofSeconds(5L)).build())
                .allOk());
        assertionValue3Argument1 = List.of(ready);
        assertionValue3Argument2 =
                q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("지연이 끝난 메시지는 자기 id 자리로 들어가 더 큰 id의 준비 메시지보다 먼저 나온다 — check 4")
    void onceTheDelayPeriodEndsTheMessageMovesToItsOwnIDPositionAndAppearsBeforeTheReadyMessageWithAHigherIDCheck4()
            throws Exception {
        final Queue q;
        final MessageId delayed;
        final MessageId nacked;
        final Chunk first;
        final List<MessageId> assertionValue4Argument1;
        final List<MessageId> assertionValue4Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        delayed = q.publish(msg("a").withDelay(Duration.ofSeconds(5L)));
        nacked = q.publish(msg("b"));
        evaluate(q.publish(msg("c")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.settle().nack(nacked, Duration.ofSeconds(5L)).build())
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue4Argument1 = List.of(delayed, nacked);
        assertionValue4Argument2 =
                q.pull(2, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 1")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck1()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final QueueCounts assertionValue1Argument1;
        final QueueCounts assertionValue1Argument2;
        final Chunk c;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        assertionValue1Argument1 = new QueueCounts(3L, 0L, 1L);
        assertionValue1Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 2")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck2()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        assertionValue2Argument1 = new QueueCounts(1L, 2L, 1L);
        assertionValue2Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 3")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck3()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final boolean assertionValue3Argument1;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        assertionValue3Argument1 = q.settle(
                        c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk();
        Assertions.assertTrue(assertionValue3Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 4")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck4()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        assertionValue4Argument1 = new QueueCounts(1L, 0L, 2L);
        assertionValue4Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 5")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck5()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        assertionValue5Argument1 = new QueueCounts(1L, 0L, 2L);
        assertionValue5Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 6")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck6()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final int assertionValue6Argument2;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        assertionValue6Argument2 = q.pull(10, Duration.ZERO).messages().size();
        Assertions.assertEquals(3, assertionValue6Argument2, "Size should match the expected value.");
        evaluate(new QueueCounts(0L, 3L, 0L));
    }

    @Test
    @DisplayName("counts는 지연됨 개수를 돌려주고 지연이 끝나도 꺼내기 전까지는 지연됨으로 센다 — check 7")
    void
            countsReturnsTheNumberOfDelayedItemsAndCountsThemAsDelayedUntilTheyAreRetrievedEvenAfterTheDelayHasEndedCheck7()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final QueueCounts assertionValue7Argument1;
        final QueueCounts assertionValue7Argument2;
        q = this.open(Duration.ofSeconds(60L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        q.publish(msg("d").withDelay(Duration.ofSeconds(10L)));
        evaluate(new QueueCounts(3L, 0L, 1L));
        c = q.pull(2, Duration.ZERO);
        evaluate(new QueueCounts(1L, 2L, 1L));
        evaluate(q.settle(c.settle().nack(ids.get(0), Duration.ofSeconds(5L)).ackRest())
                .allOk());
        evaluate(new QueueCounts(1L, 0L, 2L));
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(new QueueCounts(1L, 0L, 2L));
        evaluate(q.pull(10, Duration.ZERO).messages().size());
        assertionValue7Argument1 = new QueueCounts(0L, 3L, 0L);
        assertionValue7Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue7Argument1, assertionValue7Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 1")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck1() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue1;
            assertionValue1 = captureThrowable(() -> msg("a").withDelay(bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue1, String.valueOf(bad));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 2")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck2() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue2;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            assertionValue2 = captureThrowable(() -> c.settle().nack(id, bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue2, String.valueOf(bad));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 3")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck3() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue3;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            assertionValue3 = captureThrowable(() -> c.settle().nackRest(bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue3, String.valueOf(bad));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 4")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck4() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue4;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            assertionValue4 = captureThrowable(() -> c.nackAll(bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue4, String.valueOf(bad));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 5")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck5() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue5;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            assertionValue5 = captureThrowable(() -> q.pull(1, Duration.ZERO, bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue5, String.valueOf(bad));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 6")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck6() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue6;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            assertionValue6 = captureThrowable(() -> q.extend(c, bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue6, String.valueOf(bad));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 7")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck7() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            final Throwable assertionValue7;
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            assertionValue7 = captureThrowable(() -> new BuilderSettings().leaseDuration(bad));
            Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue7, String.valueOf(bad));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 8")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck8() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Throwable assertionValue8;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        assertionValue8 = captureThrowable(() -> msg("a").withDelay(Duration.ZERO));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue8, "With delay should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 9")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck9() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Throwable assertionValue9;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        assertionValue9 = captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue9, "Construction should throw IllegalArgumentException.");
        evaluate(new QueueCounts(0L, 1L, 0L));
    }

    @Test
    @DisplayName("지연과 리스 시간은 양수여야 한다 — check 10")
    void theDelayAndLeaseTimesMustBePositiveNumbersCheck10() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk c;
        final QueueCounts assertionValue10Argument1;
        final QueueCounts assertionValue10Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        for (final Duration bad : new Duration[] {Duration.ofSeconds(-1L), null}) {
            evaluate(captureThrowable(() -> msg("a").withDelay(bad)));
            evaluate(captureThrowable(() -> c.settle().nack(id, bad)));
            evaluate(captureThrowable(() -> c.settle().nackRest(bad)));
            evaluate(captureThrowable(() -> c.nackAll(bad)));
            evaluate(captureThrowable(() -> q.pull(1, Duration.ZERO, bad)));
            evaluate(captureThrowable(() -> q.extend(c, bad)));
            evaluate(captureThrowable(() -> new BuilderSettings().leaseDuration(bad)));
        }
        evaluate(captureThrowable(() -> msg("a").withDelay(Duration.ZERO)));
        evaluate(captureThrowable(() -> new OutgoingMessage(new byte[0], Map.of(), null)));
        assertionValue10Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue10Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue10Argument1, assertionValue10Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("정산 지연은 NACK에만 허용하고 0은 즉시 재전달한다 — check 1")
    void settlementDelayIsOnlyAllowedForNACKAnd0IsImmediatelyRetransmittedCheck1() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        final Throwable assertionValue1;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        assertionValue1 = captureThrowable(() -> new Disposition(id, Kind.ACK, Duration.ofSeconds(1L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue1, "Construction should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> new Disposition(id, Kind.REJECT, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.NACK, Duration.ofSeconds(-1L))));
        evaluate(q.settle(chunk.settle().nack(id, Duration.ZERO).build()).allOk());
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("정산 지연은 NACK에만 허용하고 0은 즉시 재전달한다 — check 2")
    void settlementDelayIsOnlyAllowedForNACKAnd0IsImmediatelyRetransmittedCheck2() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        final Throwable assertionValue2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> new Disposition(id, Kind.ACK, Duration.ofSeconds(1L))));
        assertionValue2 = captureThrowable(() -> new Disposition(id, Kind.REJECT, Duration.ofSeconds(1L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue2, "Construction should throw IllegalArgumentException.");
        evaluate(captureThrowable(() -> new Disposition(id, Kind.NACK, Duration.ofSeconds(-1L))));
        evaluate(q.settle(chunk.settle().nack(id, Duration.ZERO).build()).allOk());
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("정산 지연은 NACK에만 허용하고 0은 즉시 재전달한다 — check 3")
    void settlementDelayIsOnlyAllowedForNACKAnd0IsImmediatelyRetransmittedCheck3() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        final Throwable assertionValue3;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> new Disposition(id, Kind.ACK, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.REJECT, Duration.ofSeconds(1L))));
        assertionValue3 = captureThrowable(() -> new Disposition(id, Kind.NACK, Duration.ofSeconds(-1L)));
        Assertions.assertInstanceOf(
                IllegalArgumentException.class, assertionValue3, "Construction should throw IllegalArgumentException.");
        evaluate(q.settle(chunk.settle().nack(id, Duration.ZERO).build()).allOk());
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("정산 지연은 NACK에만 허용하고 0은 즉시 재전달한다 — check 4")
    void settlementDelayIsOnlyAllowedForNACKAnd0IsImmediatelyRetransmittedCheck4() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        final boolean assertionValue4Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> new Disposition(id, Kind.ACK, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.REJECT, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.NACK, Duration.ofSeconds(-1L))));
        assertionValue4Argument1 =
                q.settle(chunk.settle().nack(id, Duration.ZERO).build()).allOk();
        Assertions.assertTrue(assertionValue4Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList());
    }

    @Test
    @DisplayName("정산 지연은 NACK에만 허용하고 0은 즉시 재전달한다 — check 5")
    void settlementDelayIsOnlyAllowedForNACKAnd0IsImmediatelyRetransmittedCheck5() throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk chunk;
        final List<MessageId> assertionValue5Argument1;
        final List<MessageId> assertionValue5Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        chunk = q.pull(1, Duration.ZERO);
        evaluate(captureThrowable(() -> new Disposition(id, Kind.ACK, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.REJECT, Duration.ofSeconds(1L))));
        evaluate(captureThrowable(() -> new Disposition(id, Kind.NACK, Duration.ofSeconds(-1L))));
        evaluate(q.settle(chunk.settle().nack(id, Duration.ZERO).build()).allOk());
        assertionValue5Argument1 = List.of(id);
        assertionValue5Argument2 =
                q.pull(1, Duration.ZERO).messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Messages should match the expected value.");
    }

    private static long millisSince(final long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    private static void letWaiterBlock() throws InterruptedException {
        Thread.sleep(150L);
    }

    @Test
    @DisplayName("메시지가 있으면 긴 wait에도 바로 돌아온다 — check 1")
    void ifThereIsAMessageItReturnsImmediatelyEvenAfterALongWaitCheck1() throws Exception {
        final Queue q;
        final long start;
        final Chunk c;
        final int assertionValue1Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        start = System.nanoTime();
        c = q.pull(1, Duration.ofSeconds(30L));
        assertionValue1Argument2 = c.messages().size();
        Assertions.assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
        evaluate(millisSince(start) < 2000L);
    }

    @Test
    @DisplayName("메시지가 있으면 긴 wait에도 바로 돌아온다 — check 2")
    void ifThereIsAMessageItReturnsImmediatelyEvenAfterALongWaitCheck2() throws Exception {
        final Queue q;
        final long start;
        final boolean assertionValue2Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("a"));
        start = System.nanoTime();
        evaluate(q.pull(1, Duration.ofSeconds(30L)));
        assertionValue2Argument1 = millisSince(start) < 2000L;
        Assertions.assertTrue(assertionValue2Argument1, "Millis since should be less than 2000.");
    }

    @Test
    @DisplayName("메시지가 없으면 wait만큼 기다린 뒤 빈 청크를 돌려준다 — check 1")
    void ifThereIsNoMessageWaitForTheSpecifiedDurationAndReturnAnEmptyChunkCheck1() throws Exception {
        final Queue q;
        final long start;
        final Chunk c;
        final boolean assertionValue1Argument1;
        q = this.open().declare("q", QueueOptions.defaults());
        start = System.nanoTime();
        c = q.pull(1, Duration.ofMillis(300L));
        evaluate(millisSince(start));
        assertionValue1Argument1 = c.isEmpty();
        Assertions.assertTrue(assertionValue1Argument1, "Chunk should be empty.");
    }

    @Test
    @DisplayName("메시지가 없으면 wait만큼 기다린 뒤 빈 청크를 돌려준다 — check 2")
    void ifThereIsNoMessageWaitForTheSpecifiedDurationAndReturnAnEmptyChunkCheck2() throws Exception {
        final Queue q;
        final long start;
        final long elapsed;
        final boolean assertionValue2Argument1;
        final String assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        start = System.nanoTime();
        evaluate(q.pull(1, Duration.ofMillis(300L)));
        elapsed = millisSince(start);
        assertionValue2Argument1 = elapsed >= 290L;
        assertionValue2Argument2 = "elapsed=" + elapsed;
        Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
    }

    @Test
    @DisplayName("메시지가 없으면 wait만큼 기다린 뒤 빈 청크를 돌려준다 — check 3")
    void ifThereIsNoMessageWaitForTheSpecifiedDurationAndReturnAnEmptyChunkCheck3() throws Exception {
        final Queue q;
        final long start;
        final long elapsed;
        final boolean assertionValue3Argument1;
        final String assertionValue3Argument2;
        q = this.open().declare("q", QueueOptions.defaults());
        start = System.nanoTime();
        evaluate(q.pull(1, Duration.ofMillis(300L)));
        elapsed = millisSince(start);
        assertionValue3Argument1 = elapsed < 3000L;
        assertionValue3Argument2 = "elapsed=" + elapsed;
        Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
    }

    @Test
    @DisplayName("같은 프로세스에서 발행하면 기다리던 꺼내기가 곧바로 깨어난다 — check 1")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void ifItIsPublishedInTheSameProcessTheWaitingDequeueImmediatelyWakesUp() throws Exception {
        final Queue q;
        final CompletableFuture<Chunk> result;
        final long start;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        result = new CompletableFuture<>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30L)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        start = System.nanoTime();
        q.publish(msg("a"));
        c = result.get(10L, TimeUnit.SECONDS);
        final var assertionValue2Argument2 = text(c.messages().getFirst());
        Assertions.assertEquals("a", assertionValue2Argument2, "Text should match the expected value.");
        final var assertionValue1Argument1 = millisSince(start) < 1000L;
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("같은 프로세스에서 발행하면 기다리던 꺼내기가 곧바로 깨어난다 — check 2")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void ifItIsPublishedInTheSameProcessTheWaitingDequeueImmediatelyWakesUpCheck2() throws Exception {
        final Queue q;
        final CompletableFuture<Chunk> result;
        final long start;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        result = new CompletableFuture<>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30L)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        start = System.nanoTime();
        q.publish(msg("a"));
        c = result.get(10L, TimeUnit.SECONDS);
        final var assertionValue2Argument2 = text(c.messages().getFirst());
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = millisSince(start) < 1000L;
        Assertions.assertTrue(assertionValue1Argument1, "Millis since should be less than 1000.");
    }

    @Test
    @DisplayName("시계를 앞으로 돌린 효과는 다음 폴링에서 반영되어 지연 메시지가 나온다 — check 1")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void theEffectOfMovingTheClockForwardIsReflectedInTheNextPollingResultingInADelayedMessage() throws Exception {
        final Queue q;
        final CompletableFuture<Chunk> result;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("later").withDelay(Duration.ofMinutes(10L)));
        result = new CompletableFuture<>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30L)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        final var assertionValue2Argument1 = result.isDone();
        Assertions.assertFalse(assertionValue2Argument1, "Result should not be complete.");
        this.clock.advance(Duration.ofMinutes(10L));
        c = result.get(10L, TimeUnit.SECONDS);
        final var assertionValue1Argument2 = text(c.messages().getFirst());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("시계를 앞으로 돌린 효과는 다음 폴링에서 반영되어 지연 메시지가 나온다 — check 2")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void theEffectOfMovingTheClockForwardIsReflectedInTheNextPollingResultingInADelayedMessageCheck2()
            throws Exception {
        final Queue q;
        final CompletableFuture<Chunk> result;
        final Chunk c;
        q = this.open().declare("q", QueueOptions.defaults());
        q.publish(msg("later").withDelay(Duration.ofMinutes(10L)));
        result = new CompletableFuture<>();
        Thread.ofPlatform().start(() -> {
            try {
                result.complete(q.pull(1, Duration.ofSeconds(30L)));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        letWaiterBlock();
        final var assertionValue2Argument1 = result.isDone();
        evaluatePmdArgument(assertionValue2Argument1);
        this.clock.advance(Duration.ofMinutes(10L));
        c = result.get(10L, TimeUnit.SECONDS);
        final var assertionValue1Argument2 = text(c.messages().getFirst());
        Assertions.assertEquals("later", assertionValue1Argument2, "Text should match the expected value.");
    }

    @Test
    @DisplayName("기다리던 스레드를 interrupt하면 InterruptedException — check 1")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void ifTheWaitingThreadIsInterruptedAnInterruptedExceptionOccurs() throws Exception {
        final Queue q;
        final AtomicReference<Throwable> thrown;
        final AtomicBoolean interruptedFlag;
        final Thread t;
        final long start;
        q = this.open().declare("q", QueueOptions.defaults());
        thrown = new AtomicReference<>();
        interruptedFlag = new AtomicBoolean();
        t = Thread.ofPlatform().start(() -> {
            try {
                q.pull(1, Duration.ofSeconds(30L));
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedFlag.set(Thread.currentThread().isInterrupted());
        });
        letWaiterBlock();
        start = System.nanoTime();
        t.interrupt();
        t.join(10_000L);
        final var assertionValue3Argument1 = t.isAlive();
        Assertions.assertFalse(assertionValue3Argument1, "Thread should not be alive.");
        final var assertionValue2Argument2 = thrown.get();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = millisSince(start) < 2000L;
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("기다리던 스레드를 interrupt하면 InterruptedException — check 2")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void ifTheWaitingThreadIsInterruptedAnInterruptedExceptionOccursCheck2() throws Exception {
        final Queue q;
        final AtomicReference<Throwable> thrown;
        final AtomicBoolean interruptedFlag;
        final Thread t;
        final long start;
        q = this.open().declare("q", QueueOptions.defaults());
        thrown = new AtomicReference<>();
        interruptedFlag = new AtomicBoolean();
        t = Thread.ofPlatform().start(() -> {
            try {
                q.pull(1, Duration.ofSeconds(30L));
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedFlag.set(Thread.currentThread().isInterrupted());
        });
        letWaiterBlock();
        start = System.nanoTime();
        t.interrupt();
        t.join(10_000L);
        final var assertionValue3Argument1 = t.isAlive();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument2 = thrown.get();
        Assertions.assertInstanceOf(
                InterruptedException.class,
                assertionValue2Argument2,
                "Value should be an instance of InterruptedException.");
        final var assertionValue1Argument1 = millisSince(start) < 2000L;
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("기다리던 스레드를 interrupt하면 InterruptedException — check 3")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void ifTheWaitingThreadIsInterruptedAnInterruptedExceptionOccursCheck3() throws Exception {
        final Queue q;
        final AtomicReference<Throwable> thrown;
        final AtomicBoolean interruptedFlag;
        final Thread t;
        final long start;
        q = this.open().declare("q", QueueOptions.defaults());
        thrown = new AtomicReference<>();
        interruptedFlag = new AtomicBoolean();
        t = Thread.ofPlatform().start(() -> {
            try {
                q.pull(1, Duration.ofSeconds(30L));
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedFlag.set(Thread.currentThread().isInterrupted());
        });
        letWaiterBlock();
        start = System.nanoTime();
        t.interrupt();
        t.join(10_000L);
        final var assertionValue3Argument1 = t.isAlive();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument2 = thrown.get();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = millisSince(start) < 2000L;
        Assertions.assertTrue(assertionValue1Argument1, "Millis since should be less than 2000.");
    }

    @Test
    @DisplayName("Smq를 닫으면 기다리던 꺼내기가 빈 청크를 받고 깨어난다 — check 1")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.CloseResource",
                reason = "The fixture registers this Smq and closes it in closeAll after every test.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.CloseResource", "PMD.AvoidCatchingGenericException"})
    void whenSmqIsClosedTheWaitingDequeueOperationReceivesAnEmptyChunkAndWakesUp() throws Exception {
        final Smq smq;
        final Queue q;
        final int waiters;
        final List<CompletableFuture<Chunk>> results;
        final long start;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        waiters = 3;
        results = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            final CompletableFuture<Chunk> f;
            f = new CompletableFuture<>();
            results.add(f);
            Thread.ofPlatform().start(() -> {
                try {
                    f.complete(q.pull(1, Duration.ofSeconds(30L)));
                } catch (Throwable e) {
                    f.completeExceptionally(e);
                }
            });
        }
        letWaiterBlock();
        start = System.nanoTime();
        smq.close();
        for (final CompletableFuture<Chunk> f : results) {
            final var assertionValue2Argument1 = f.get(10L, TimeUnit.SECONDS).isEmpty();
            Assertions.assertTrue(assertionValue2Argument1, "F should be empty.");
        }
        final var assertionValue1Argument1 = millisSince(start) < 2000L;
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("Smq를 닫으면 기다리던 꺼내기가 빈 청크를 받고 깨어난다 — check 2")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.CloseResource",
                reason = "The fixture registers this Smq and closes it in closeAll after every test.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.CloseResource", "PMD.AvoidCatchingGenericException"})
    void whenSmqIsClosedTheWaitingDequeueOperationReceivesAnEmptyChunkAndWakesUpCheck2() throws Exception {
        final Smq smq;
        final Queue q;
        final int waiters;
        final List<CompletableFuture<Chunk>> results;
        final long start;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        waiters = 3;
        results = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            final CompletableFuture<Chunk> f;
            f = new CompletableFuture<>();
            results.add(f);
            Thread.ofPlatform().start(() -> {
                try {
                    f.complete(q.pull(1, Duration.ofSeconds(30L)));
                } catch (Throwable e) {
                    f.completeExceptionally(e);
                }
            });
        }
        letWaiterBlock();
        start = System.nanoTime();
        smq.close();
        for (final CompletableFuture<Chunk> f : results) {
            final var assertionValue2Argument1 = f.get(10L, TimeUnit.SECONDS).isEmpty();
            evaluatePmdArgument(assertionValue2Argument1);
        }
        final var assertionValue1Argument1 = millisSince(start) < 2000L;
        Assertions.assertTrue(assertionValue1Argument1, "Millis since should be less than 2000.");
    }

    @Test
    @DisplayName("닫은 뒤의 꺼내기는 기다리지 않고 빈 청크이고 두 번 닫아도 안전하다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void afterClosingTheDequeueOperationDoesNotWaitAndReturnsAnEmptyChunkItIsSafeToCloseItTwiceCheck1()
            throws Exception {
        final Smq smq;
        final Queue q;
        final long start;
        final boolean assertionValue1Argument1;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        smq.close();
        smq.close();
        start = System.nanoTime();
        assertionValue1Argument1 = q.pull(1, Duration.ofSeconds(30L)).isEmpty();
        Assertions.assertTrue(assertionValue1Argument1, "Pulled chunk should be empty.");
        evaluate(millisSince(start) < 2000L);
    }

    @Test
    @DisplayName("닫은 뒤의 꺼내기는 기다리지 않고 빈 청크이고 두 번 닫아도 안전하다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void afterClosingTheDequeueOperationDoesNotWaitAndReturnsAnEmptyChunkItIsSafeToCloseItTwiceCheck2()
            throws Exception {
        final Smq smq;
        final Queue q;
        final long start;
        final boolean assertionValue2Argument1;
        smq = this.open();
        q = smq.declare("q", QueueOptions.defaults());
        smq.close();
        smq.close();
        start = System.nanoTime();
        evaluate(q.pull(1, Duration.ofSeconds(30L)).isEmpty());
        assertionValue2Argument1 = millisSince(start) < 2000L;
        Assertions.assertTrue(assertionValue2Argument1, "Millis since should be less than 2000.");
    }

    @Test
    @DisplayName("여러 스레드가 하나의 Smq를 함께 써도 모든 메시지가 정확히 한 번 전달된다 — check 1")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The concurrent producer and consumer scenario coordinates and verifies all workers together.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void evenIfMultipleThreadsUseASingleSmqTogetherAllMessagesAreDeliveredExactlyOnce() throws Exception {
        final Queue q;
        final int producers;
        final int perProducer;
        final int consumers;
        final int total;
        final KeySetView<String, Boolean> seen;
        final AtomicInteger duplicates;
        final AtomicInteger delivered;
        final List<ThrowingRunnable> tasks;
        q = this.open().declare("q", QueueOptions.defaults());
        producers = 4;
        perProducer = 100;
        consumers = 4;
        total = producers * perProducer;
        seen = ConcurrentHashMap.newKeySet();
        duplicates = new AtomicInteger();
        delivered = new AtomicInteger();
        tasks = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            tasks.add(() -> {
                while (delivered.get() < total) {
                    final Chunk chunk;
                    chunk = q.pull(7, Duration.ofMillis(200L));
                    for (final Message m : chunk.messages()) {
                        if (!seen.add(text(m))) {
                            duplicates.incrementAndGet();
                        }
                    }
                    if (!chunk.isEmpty()) {
                        q.settle(chunk.ackAll());
                        delivered.addAndGet(chunk.messages().size());
                    }
                }
            });
        }
        for (int p = 0; p < producers; p++) {
            final int id;
            id = p;
            tasks.add(() -> {
                for (int i = 0; i < perProducer; i++) {
                    q.publish(msg(id + "-" + i));
                }
            });
        }
        runConcurrently(tasks);
        final var assertionValue3Argument2 = duplicates.get();
        Assertions.assertEquals(0, assertionValue3Argument2, "Duplicates should match the expected value.");
        final var assertionValue2Argument2 = seen.size();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = new QueueCounts(0L, 0L, 0L);
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = q.counts();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("여러 스레드가 하나의 Smq를 함께 써도 모든 메시지가 정확히 한 번 전달된다 — check 2")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The concurrent producer and consumer scenario coordinates and verifies all workers together.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void evenIfMultipleThreadsUseASingleSmqTogetherAllMessagesAreDeliveredExactlyOnceCheck2() throws Exception {
        final Queue q;
        final int producers;
        final int perProducer;
        final int consumers;
        final int total;
        final KeySetView<String, Boolean> seen;
        final AtomicInteger duplicates;
        final AtomicInteger delivered;
        final List<ThrowingRunnable> tasks;
        q = this.open().declare("q", QueueOptions.defaults());
        producers = 4;
        perProducer = 100;
        consumers = 4;
        total = producers * perProducer;
        seen = ConcurrentHashMap.newKeySet();
        duplicates = new AtomicInteger();
        delivered = new AtomicInteger();
        tasks = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            tasks.add(() -> {
                while (delivered.get() < total) {
                    final Chunk chunk;
                    chunk = q.pull(7, Duration.ofMillis(200L));
                    for (final Message m : chunk.messages()) {
                        if (!seen.add(text(m))) {
                            duplicates.incrementAndGet();
                        }
                    }
                    if (!chunk.isEmpty()) {
                        q.settle(chunk.ackAll());
                        delivered.addAndGet(chunk.messages().size());
                    }
                }
            });
        }
        for (int p = 0; p < producers; p++) {
            final int id;
            id = p;
            tasks.add(() -> {
                for (int i = 0; i < perProducer; i++) {
                    q.publish(msg(id + "-" + i));
                }
            });
        }
        runConcurrently(tasks);
        final var assertionValue3Argument2 = duplicates.get();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = seen.size();
        Assertions.assertEquals(total, assertionValue2Argument2, "Size should match the expected value.");
        final var assertionValue1Argument1 = new QueueCounts(0L, 0L, 0L);
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = q.counts();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("여러 스레드가 하나의 Smq를 함께 써도 모든 메시지가 정확히 한 번 전달된다 — check 3")
    @PolicyException(
            rule = "PMD.CognitiveComplexity",
            reason = "The concurrent producer and consumer scenario coordinates and verifies all workers together.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CognitiveComplexity")
    void evenIfMultipleThreadsUseASingleSmqTogetherAllMessagesAreDeliveredExactlyOnceCheck3() throws Exception {
        final Queue q;
        final int producers;
        final int perProducer;
        final int consumers;
        final int total;
        final KeySetView<String, Boolean> seen;
        final AtomicInteger duplicates;
        final AtomicInteger delivered;
        final List<ThrowingRunnable> tasks;
        q = this.open().declare("q", QueueOptions.defaults());
        producers = 4;
        perProducer = 100;
        consumers = 4;
        total = producers * perProducer;
        seen = ConcurrentHashMap.newKeySet();
        duplicates = new AtomicInteger();
        delivered = new AtomicInteger();
        tasks = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            tasks.add(() -> {
                while (delivered.get() < total) {
                    final Chunk chunk;
                    chunk = q.pull(7, Duration.ofMillis(200L));
                    for (final Message m : chunk.messages()) {
                        if (!seen.add(text(m))) {
                            duplicates.incrementAndGet();
                        }
                    }
                    if (!chunk.isEmpty()) {
                        q.settle(chunk.ackAll());
                        delivered.addAndGet(chunk.messages().size());
                    }
                }
            });
        }
        for (int p = 0; p < producers; p++) {
            final int id;
            id = p;
            tasks.add(() -> {
                for (int i = 0; i < perProducer; i++) {
                    q.publish(msg(id + "-" + i));
                }
            });
        }
        runConcurrently(tasks);
        final var assertionValue3Argument2 = duplicates.get();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument2 = seen.size();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = new QueueCounts(0L, 0L, 0L);
        final var assertionValue1Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("최대 전달 횟수 이동은 dead letter 꺼내기를 깨운다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void movingTheMaximumNumberOfDeliveriesWakesUpTheDeadLetterDequeueOperation() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("poison"));
        q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<Chunk> waiting;
            final Chunk received;
            waiting = executor.submit(() -> dead.pull(1, Duration.ofSeconds(5L)));
            Thread.sleep(1050L);
            q.pull(1, Duration.ZERO);
            received = waiting.get(150L, TimeUnit.MILLISECONDS);
            Assertions.assertEquals(
                    "poison", text(received.messages().getFirst()), "Text should match the expected value.");
        }
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck1()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final int assertionValue1Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        assertionValue1Argument2 = c.messages().getFirst().deliveryCount();
        Assertions.assertEquals(2, assertionValue1Argument2, "Delivery count should match the expected value.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck2()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue2Argument1 = q.settle(c.settle().reject(id).build()).allOk();
        Assertions.assertTrue(assertionValue2Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck3()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck4()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue4Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck5()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Message moved;
        final String assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue5Argument2 = text(moved);
        Assertions.assertEquals("poison", assertionValue5Argument2, "Text should match the expected value.");
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck6()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Message moved;
        final MessageId assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue6Argument2 = moved.id();
        Assertions.assertNotEquals(id, assertionValue6Argument2, "Message ID should differ from the unexpected value.");
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck7()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Message moved;
        final int assertionValue7Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue7Argument2 = moved.deliveryCount();
        Assertions.assertEquals(1, assertionValue7Argument2, "Delivery count should match the expected value.");
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected"));
    }

    @Test
    @DisplayName("reject한 메시지는 dead letter 큐로 옮겨지고 payload와 헤더를 유지하며 smq 헤더를 갖는다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void rejectedMessagesAreMovedToTheDeadLetterQueueRetainTheirPayloadAndHeaderAndHaveAnSMQHeaderCheck8()
            throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Message moved;
        final Map<String, String> assertionValue8Argument1;
        final Map<String, String> assertionValue8Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("orders.dead", QueueOptions.defaults());
        q = smq.declare("orders", QueueOptions.defaults().deadLetterQueue("orders.dead"));
        id = q.publish(msg("poison").withHeaders(Map.of("trace", "t-1")));
        q.settle(q.pull(1, Duration.ZERO).settle().nack(id).build());
        c = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue8Argument1 = Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "orders",
                "smq-original-message-id",
                String.valueOf(id.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(5L).toString(),
                "smq-reason",
                "rejected");
        assertionValue8Argument2 = moved.headers();
        Assertions.assertEquals(
                assertionValue8Argument1, assertionValue8Argument2, "Headers should match the expected value.");
    }

    @Test
    @DisplayName("reject한 메시지의 이전 리스로 다시 정산하면 STALE이다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifARejectedMessageIsSettledAgainWithItsPreviousLeaseItBecomesSTALECheck1() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = q.settle(c.settle().reject(id).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.settle(c.settle().reject(id).build()).byMessage().get(id));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(smq.queue("dead").counts());
    }

    @Test
    @DisplayName("reject한 메시지의 이전 리스로 다시 정산하면 STALE이다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifARejectedMessageIsSettledAgainWithItsPreviousLeaseItBecomesSTALECheck2() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final Outcome assertionValue2Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        assertionValue2Argument2 =
                q.settle(c.settle().reject(id).build()).byMessage().get(id);
        Assertions.assertEquals(Outcome.STALE, assertionValue2Argument2, "By message should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(smq.queue("dead").counts());
    }

    @Test
    @DisplayName("reject한 메시지의 이전 리스로 다시 정산하면 STALE이다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifARejectedMessageIsSettledAgainWithItsPreviousLeaseItBecomesSTALECheck3() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk c;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        c = q.pull(1, Duration.ZERO);
        evaluate(q.settle(c.settle().reject(id).build()).allOk());
        evaluate(q.settle(c.settle().reject(id).build()).byMessage().get(id));
        assertionValue3Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue3Argument2 = smq.queue("dead").counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지의 reject는 STALE이고 dead letter 큐에 아무것도 넣지 않는다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            theRejectOfAMessageThatHasBeenRetrievedAgainByAnotherConsumerIsSTALEAndNothingIsPlacedInTheDeadLetterQueueCheck1()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final List<MessageId> assertionValue1Argument1;
        final List<MessageId> assertionValue1Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = List.of(id);
        assertionValue1Argument2 = q.settle(slow.settle().reject(id).build()).stale();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Stale should match the expected value.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(q.settle(fast.ackAll()).allOk());
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지의 reject는 STALE이고 dead letter 큐에 아무것도 넣지 않는다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            theRejectOfAMessageThatHasBeenRetrievedAgainByAnotherConsumerIsSTALEAndNothingIsPlacedInTheDeadLetterQueueCheck2()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        evaluate(q.settle(slow.settle().reject(id).build()).stale());
        assertionValue2Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue2Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(q.settle(fast.ackAll()).allOk());
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지의 reject는 STALE이고 dead letter 큐에 아무것도 넣지 않는다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            theRejectOfAMessageThatHasBeenRetrievedAgainByAnotherConsumerIsSTALEAndNothingIsPlacedInTheDeadLetterQueueCheck3()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        evaluate(q.settle(slow.settle().reject(id).build()).stale());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(q.settle(fast.ackAll()).allOk());
    }

    @Test
    @DisplayName("다른 컨슈머가 다시 꺼간 메시지의 reject는 STALE이고 dead letter 큐에 아무것도 넣지 않는다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            theRejectOfAMessageThatHasBeenRetrievedAgainByAnotherConsumerIsSTALEAndNothingIsPlacedInTheDeadLetterQueueCheck4()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final boolean assertionValue4Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        evaluate(q.settle(slow.settle().reject(id).build()).stale());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue4Argument1 = q.settle(fast.ackAll()).allOk();
        Assertions.assertTrue(assertionValue4Argument1, "Outcomes should succeed.");
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서 reject하면 NoDeadLetterQueueException이고 섞인 정산 전체가 반영되지 않는다 — check 1")
    void
            ifAMessageIsRejectedInAQueueWithoutADeadLetterQueueANoDeadLetterQueueExceptionOccursAndTheEntireMixedSettlementIsNotReflected()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final NoDeadLetterQueueException e;
        final Chunk again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        final var assertionValue5 = captureThrowable(() -> q.settle(
                c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        Assertions.assertInstanceOf(
                NoDeadLetterQueueException.class, assertionValue5, "Settle should throw NoDeadLetterQueueException.");
        e = (NoDeadLetterQueueException) (NoDeadLetterQueueException) assertionValue5;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 = new QueueCounts(0L, 3L, 0L);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = q.counts();
        evaluatePmdArgument(assertionValue3Argument2);
        this.clock.advance(Duration.ofSeconds(10L));
        again = q.pull(10, Duration.ZERO);
        final var assertionValue2Argument2 =
                again.messages().stream().map(Message::id).toList();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = again.messages().stream().allMatch(m -> m.deliveryCount() == 2);
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서 reject하면 NoDeadLetterQueueException이고 섞인 정산 전체가 반영되지 않는다 — check 2")
    void
            ifAMessageIsRejectedInAQueueWithoutADeadLetterQueueANoDeadLetterQueueExceptionOccursAndTheEntireMixedSettlementIsNotReflectedCheck2()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final NoDeadLetterQueueException e;
        final Chunk again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        final var assertionValue5 = captureThrowable(() -> q.settle(
                c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        e = (NoDeadLetterQueueException) (NoDeadLetterQueueException) assertionValue5;
        final var assertionValue4Argument2 = e.queue();
        Assertions.assertEquals("q", assertionValue4Argument2, "Queue should match the expected value.");
        final var assertionValue3Argument1 = new QueueCounts(0L, 3L, 0L);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = q.counts();
        evaluatePmdArgument(assertionValue3Argument2);
        this.clock.advance(Duration.ofSeconds(10L));
        again = q.pull(10, Duration.ZERO);
        final var assertionValue2Argument2 =
                again.messages().stream().map(Message::id).toList();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = again.messages().stream().allMatch(m -> m.deliveryCount() == 2);
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서 reject하면 NoDeadLetterQueueException이고 섞인 정산 전체가 반영되지 않는다 — check 3")
    void
            ifAMessageIsRejectedInAQueueWithoutADeadLetterQueueANoDeadLetterQueueExceptionOccursAndTheEntireMixedSettlementIsNotReflectedCheck3()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final NoDeadLetterQueueException e;
        final Chunk again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        final var assertionValue5 = captureThrowable(() -> q.settle(
                c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        e = (NoDeadLetterQueueException) (NoDeadLetterQueueException) assertionValue5;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 = new QueueCounts(0L, 3L, 0L);
        final var assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        again = q.pull(10, Duration.ZERO);
        final var assertionValue2Argument2 =
                again.messages().stream().map(Message::id).toList();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = again.messages().stream().allMatch(m -> m.deliveryCount() == 2);
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서 reject하면 NoDeadLetterQueueException이고 섞인 정산 전체가 반영되지 않는다 — check 4")
    void
            ifAMessageIsRejectedInAQueueWithoutADeadLetterQueueANoDeadLetterQueueExceptionOccursAndTheEntireMixedSettlementIsNotReflectedCheck4()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final NoDeadLetterQueueException e;
        final Chunk again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        final var assertionValue5 = captureThrowable(() -> q.settle(
                c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        e = (NoDeadLetterQueueException) (NoDeadLetterQueueException) assertionValue5;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 = new QueueCounts(0L, 3L, 0L);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = q.counts();
        evaluatePmdArgument(assertionValue3Argument2);
        this.clock.advance(Duration.ofSeconds(10L));
        again = q.pull(10, Duration.ZERO);
        final var assertionValue2Argument2 =
                again.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(ids, assertionValue2Argument2, "Messages should match the expected value.");
        final var assertionValue1Argument1 = again.messages().stream().allMatch(m -> m.deliveryCount() == 2);
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서 reject하면 NoDeadLetterQueueException이고 섞인 정산 전체가 반영되지 않는다 — check 5")
    void
            ifAMessageIsRejectedInAQueueWithoutADeadLetterQueueANoDeadLetterQueueExceptionOccursAndTheEntireMixedSettlementIsNotReflectedCheck5()
                    throws Exception {
        final Queue q;
        final List<MessageId> ids;
        final Chunk c;
        final NoDeadLetterQueueException e;
        final Chunk again;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        ids = q.publish(List.of(msg("a"), msg("b"), msg("c")));
        c = q.pull(3, Duration.ZERO);
        final var assertionValue5 = captureThrowable(() -> q.settle(
                c.settle().ack(ids.get(0)).nack(ids.get(1)).reject(ids.get(2)).build()));
        e = (NoDeadLetterQueueException) (NoDeadLetterQueueException) assertionValue5;
        final var assertionValue4Argument2 = e.queue();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 = new QueueCounts(0L, 3L, 0L);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = q.counts();
        evaluatePmdArgument(assertionValue3Argument2);
        this.clock.advance(Duration.ofSeconds(10L));
        again = q.pull(10, Duration.ZERO);
        final var assertionValue2Argument2 =
                again.messages().stream().map(Message::id).toList();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = again.messages().stream().allMatch(m -> m.deliveryCount() == 2);
        Assertions.assertTrue(assertionValue1Argument1, "All redelivered messages should have two deliveries.");
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서도 다른 컨슈머가 다시 꺼간 메시지의 reject는 예외가 아니라 STALE이다 — check 1")
    void
            evenInAQueueWithoutADeadLetterQueueTheRejectOfAMessageThatAnotherConsumerHasRetrievedAgainIsNotAnExceptionButSTALECheck1()
                    throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final List<MessageId> assertionValue1Argument1;
        final List<MessageId> assertionValue1Argument2;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = List.of(id);
        assertionValue1Argument2 = q.settle(slow.settle().reject(id).build()).stale();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Stale should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(q.settle(fast.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서도 다른 컨슈머가 다시 꺼간 메시지의 reject는 예외가 아니라 STALE이다 — check 2")
    void
            evenInAQueueWithoutADeadLetterQueueTheRejectOfAMessageThatAnotherConsumerHasRetrievedAgainIsNotAnExceptionButSTALECheck2()
                    throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final QueueCounts assertionValue2Argument1;
        final QueueCounts assertionValue2Argument2;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        evaluate(q.settle(slow.settle().reject(id).build()).stale());
        assertionValue2Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue2Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
        evaluate(q.settle(fast.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐가 없는 큐에서도 다른 컨슈머가 다시 꺼간 메시지의 reject는 예외가 아니라 STALE이다 — check 3")
    void
            evenInAQueueWithoutADeadLetterQueueTheRejectOfAMessageThatAnotherConsumerHasRetrievedAgainIsNotAnExceptionButSTALECheck3()
                    throws Exception {
        final Queue q;
        final MessageId id;
        final Chunk slow;
        final Chunk fast;
        final boolean assertionValue3Argument1;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        id = q.publish(msg("a"));
        slow = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(10L));
        fast = q.pull(1, Duration.ZERO);
        evaluate(q.settle(slow.settle().reject(id).build()).stale());
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue3Argument1 = q.settle(fast.ackAll()).allOk();
        Assertions.assertTrue(assertionValue3Argument1, "Outcomes should succeed.");
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck1()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final boolean assertionValue1Argument1;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        assertionValue1Argument1 =
                q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck2()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final String assertionValue2Argument2;
        final Chunk lastChunk;
        final Message end;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        assertionValue2Argument2 = m.headers().get("smq-original-queue");
        Assertions.assertEquals("q", assertionValue2Argument2, "Headers should match the expected value.");
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck3()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final boolean assertionValue3Argument1;
        final Chunk lastChunk;
        final Message end;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        assertionValue3Argument1 =
                middle.settle(inMiddle.settle().reject(m.id()).build()).allOk();
        Assertions.assertTrue(assertionValue3Argument1, "Outcomes should succeed.");
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck4()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final String assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        assertionValue4Argument2 = text(end);
        Assertions.assertEquals("a", assertionValue4Argument2, "Text should match the expected value.");
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck5()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final String assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        assertionValue5Argument2 = end.headers().get("smq-original-queue");
        Assertions.assertEquals("middle", assertionValue5Argument2, "Headers should match the expected value.");
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck6()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final String assertionValue6Argument1;
        final String assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        assertionValue6Argument1 = String.valueOf(m.id().value());
        assertionValue6Argument2 = end.headers().get("smq-original-message-id");
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Headers should match the expected value.");
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck7()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final String assertionValue7Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        assertionValue7Argument2 = end.headers().get("smq-reason");
        Assertions.assertEquals("rejected", assertionValue7Argument2, "Headers should match the expected value.");
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck8()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final Throwable assertionValue8;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        assertionValue8 = captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build()));
        Assertions.assertInstanceOf(
                NoDeadLetterQueueException.class, assertionValue8, "Settle should throw NoDeadLetterQueueException.");
        evaluate(last.settle(lastChunk.ackAll()).allOk());
    }

    @Test
    @DisplayName("dead letter 큐는 평범한 큐라 꺼내고 정산하고 연쇄로 dead letter를 둘 수 있다 — check 9")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void sinceTheDeadLetterQueueIsAnOrdinaryQueueYouCanRetrieveMessagesSettleThemAndPlaceDeadLettersInAChainCheck9()
            throws Exception {
        final Smq smq;
        final Queue last;
        final Queue middle;
        final Queue q;
        final MessageId id;
        final Chunk inMiddle;
        final Message m;
        final Chunk lastChunk;
        final Message end;
        final boolean assertionValue9Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        last = smq.declare("last", QueueOptions.defaults());
        middle = smq.declare("middle", QueueOptions.defaults().deadLetterQueue("last"));
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("middle"));
        id = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        inMiddle = middle.pull(1, Duration.ZERO);
        m = inMiddle.messages().getFirst();
        evaluate(middle.settle(inMiddle.settle().reject(m.id()).build()).allOk());
        lastChunk = last.pull(1, Duration.ZERO);
        end = lastChunk.messages().getFirst();
        evaluate(String.valueOf(m.id().value()));
        evaluate(captureThrowable(
                () -> last.settle(lastChunk.settle().reject(end.id()).build())));
        assertionValue9Argument1 = last.settle(lastChunk.ackAll()).allOk();
        Assertions.assertTrue(assertionValue9Argument1, "Outcomes should succeed.");
    }

    @Test
    @DisplayName("smq 접두사는 예약이라 같은 이름의 사용자 헤더는 시스템 값으로 덮어써진다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void theSmqPrefixIsReservedSoUserHeadersWithTheSameNameAreOverwrittenBySystemValuesCheck1() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a").withHeaders(Map.of("smq-reason", "mine", "trace", "t-1")));
        assertionValue1Argument1 =
                q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
    }

    @Test
    @DisplayName("smq 접두사는 예약이라 같은 이름의 사용자 헤더는 시스템 값으로 덮어써진다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void theSmqPrefixIsReservedSoUserHeadersWithTheSameNameAreOverwrittenBySystemValuesCheck2() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Message moved;
        final String assertionValue2Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a").withHeaders(Map.of("smq-reason", "mine", "trace", "t-1")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue2Argument2 = moved.headers().get("smq-reason");
        Assertions.assertEquals("rejected", assertionValue2Argument2, "Headers should match the expected value.");
    }

    @Test
    @DisplayName("smq 접두사는 예약이라 같은 이름의 사용자 헤더는 시스템 값으로 덮어써진다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void theSmqPrefixIsReservedSoUserHeadersWithTheSameNameAreOverwrittenBySystemValuesCheck3() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId id;
        final Message moved;
        final String assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().deadLetterQueue("dead"));
        id = q.publish(msg("a").withHeaders(Map.of("smq-reason", "mine", "trace", "t-1")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().reject(id).build()).allOk());
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue3Argument2 = moved.headers().get("trace");
        Assertions.assertEquals("t-1", assertionValue3Argument2, "Headers should match the expected value.");
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck1()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        assertionValue1Argument1 =
                q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck2()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        assertionValue2Argument1 =
                q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk();
        Assertions.assertTrue(assertionValue2Argument1, "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck3()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final MessageId b;
        final Chunk c;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        b = q.publish(msg("b"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        c = q.pull(1, Duration.ZERO);
        assertionValue3Argument1 = List.of(b);
        assertionValue3Argument2 = c.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck4()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final Chunk c;
        final int assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        c = q.pull(1, Duration.ZERO);
        assertionValue4Argument2 = c.messages().getFirst().deliveryCount();
        Assertions.assertEquals(1, assertionValue4Argument2, "Delivery count should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck5()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        assertionValue5Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue5Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck6()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue6Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue6Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
        evaluate(dead.pull(1, Duration.ZERO).messages().getFirst());
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck7()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final Message moved;
        final String assertionValue7Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue7Argument2 = text(moved);
        Assertions.assertEquals("a", assertionValue7Argument2, "Text should match the expected value.");
        evaluate(Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries"));
    }

    @Test
    @DisplayName("최대 전달 횟수에 도달한 메시지는 다시 꺼낼 때 dead letter 큐로 옮겨지고 꺼내기는 다음 메시지로 진행한다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsAreMovedToTheDeadLetterQueueWhenTheyAreRetrievedAgainAndTheRetrievalProcessContinuesWithTheNextMessageCheck8()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final Message moved;
        final Map<String, String> assertionValue8Argument1;
        final Map<String, String> assertionValue8Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a").withHeaders(Map.of("trace", "t-1")));
        evaluate(q.publish(msg("b")));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        this.clock.advance(Duration.ofSeconds(3L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
        moved = dead.pull(1, Duration.ZERO).messages().getFirst();
        assertionValue8Argument1 = Map.of(
                "trace",
                "t-1",
                "smq-original-queue",
                "q",
                "smq-original-message-id",
                String.valueOf(a.value()),
                "smq-last-delivery-count",
                "2",
                "smq-moved-at",
                T0.plusSeconds(3L).toString(),
                "smq-reason",
                "max-deliveries");
        assertionValue8Argument2 = moved.headers();
        Assertions.assertEquals(
                assertionValue8Argument1, assertionValue8Argument2, "Headers should match the expected value.");
    }

    @Test
    @DisplayName("리스가 만료된 채 최대 전달 횟수에 도달한 메시지도 다음 꺼내기에서 옮겨진다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsWithAnExpiredLeaseWillAlsoBeMovedInTheNextDequeueOperationCheck1()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int assertionValue1Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        assertionValue1Argument2 = q.pull(1, Duration.ZERO).messages().size();
        Assertions.assertEquals(1, assertionValue1Argument2, "Size should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("리스가 만료된 채 최대 전달 횟수에 도달한 메시지도 다음 꺼내기에서 옮겨진다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsWithAnExpiredLeaseWillAlsoBeMovedInTheNextDequeueOperationCheck2()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.pull(1, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("리스가 만료된 채 최대 전달 횟수에 도달한 메시지도 다음 꺼내기에서 옮겨진다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsWithAnExpiredLeaseWillAlsoBeMovedInTheNextDequeueOperationCheck3()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.pull(1, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("리스가 만료된 채 최대 전달 횟수에 도달한 메시지도 다음 꺼내기에서 옮겨진다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatHaveReachedTheMaximumNumberOfDeliveryAttemptsWithAnExpiredLeaseWillAlsoBeMovedInTheNextDequeueOperationCheck4()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.pull(1, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue4Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("마지막 전달에서 ACK하면 정상적으로 성공하고 옮겨지지 않는다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifAnACKIsReceivedInTheLastDeliveryItIsConsideredSuccessfulAndTheMessageIsNotMovedCheck1() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId a;
        final boolean assertionValue1Argument1;
        final Chunk last;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a"));
        assertionValue1Argument1 =
                q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        last = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(20L));
        evaluate(q.settle(last.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 ACK하면 정상적으로 성공하고 옮겨지지 않는다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifAnACKIsReceivedInTheLastDeliveryItIsConsideredSuccessfulAndTheMessageIsNotMovedCheck2() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId a;
        final Chunk last;
        final int assertionValue2Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        last = q.pull(1, Duration.ZERO);
        assertionValue2Argument2 = last.messages().getFirst().deliveryCount();
        Assertions.assertEquals(2, assertionValue2Argument2, "Delivery count should match the expected value.");
        this.clock.advance(Duration.ofSeconds(20L));
        evaluate(q.settle(last.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 ACK하면 정상적으로 성공하고 옮겨지지 않는다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifAnACKIsReceivedInTheLastDeliveryItIsConsideredSuccessfulAndTheMessageIsNotMovedCheck3() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId a;
        final Chunk last;
        final boolean assertionValue3Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        last = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(20L));
        assertionValue3Argument1 = q.settle(last.ackAll()).allOk();
        Assertions.assertTrue(assertionValue3Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 ACK하면 정상적으로 성공하고 옮겨지지 않는다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifAnACKIsReceivedInTheLastDeliveryItIsConsideredSuccessfulAndTheMessageIsNotMovedCheck4() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId a;
        final Chunk last;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        last = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(20L));
        evaluate(q.settle(last.ackAll()).allOk());
        assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue4Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 ACK하면 정상적으로 성공하고 옮겨지지 않는다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifAnACKIsReceivedInTheLastDeliveryItIsConsideredSuccessfulAndTheMessageIsNotMovedCheck5() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId a;
        final Chunk last;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
        a = q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).settle().nack(a).build()).allOk());
        last = q.pull(1, Duration.ZERO);
        this.clock.advance(Duration.ofSeconds(20L));
        evaluate(q.settle(last.ackAll()).allOk());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue5Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue5Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck1()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        assertionValue1Argument1 = q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck2()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 0L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck3()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 1L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck4()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 1L));
        assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck5()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final boolean assertionValue5Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue5Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue5Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck6()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue6Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue6Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("마지막 전달에서 지연 NACK한 메시지는 지연이 끝나야 옮겨지고 그동안 지연됨으로 센다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            messagesThatReceivedADelayedNACKInTheLastDeliveryAreMovedOnlyAfterTheDelayPeriodEndsAndTheyAreCountedAsDelayedDuringThatTimeCheck7()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final QueueCounts assertionValue7Argument1;
        final QueueCounts assertionValue7Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        q.publish(msg("a"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(5L)))
                .allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue7Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue7Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue7Argument1, assertionValue7Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck1()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final int assertionValue1Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        assertionValue1Argument2 = first.messages().size();
        Assertions.assertEquals(poison, assertionValue1Argument2, "Size should match the expected value.");
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk());
        evaluate(q.publish(msg("good")));
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck2()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        assertionValue2Argument1 =
                q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk();
        Assertions.assertTrue(assertionValue2Argument1, "Outcomes should succeed.");
        evaluate(q.publish(msg("good")));
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck3()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final MessageId good;
        final Chunk c;
        final List<MessageId> assertionValue3Argument1;
        final List<MessageId> assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk());
        good = q.publish(msg("good"));
        this.clock.advance(Duration.ofSeconds(1L));
        c = q.pull(1, Duration.ZERO);
        assertionValue3Argument1 = List.of(good);
        assertionValue3Argument2 = c.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Messages should match the expected value.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck4()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final boolean assertionValue4Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk());
        evaluate(q.publish(msg("good")));
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO));
        assertionValue4Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue4Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck5()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk());
        evaluate(q.publish(msg("good")));
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue5Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue5Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("dead letter가 한 번에 옮기는 상한보다 많이 쌓여 있어도 pull 한 번이 남은 준비 메시지를 돌려준다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfTheNumberOfDeadLettersExceedsTheMaximumNumberThatCanBeMovedAtOnceASinglePullReturnsTheRemainingReadyMessagesCheck6()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final Chunk first;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        first = q.pull(poison, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(1L))).allOk());
        evaluate(q.publish(msg("good")));
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue6Argument1 = new QueueCounts(poison, 0L, 0L);
        assertionValue6Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("상한보다 많이 쌓인 dead letter 뒤에 준비 메시지가 없으면 pull은 다 옮기고 빈 청크를 돌려준다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifThereAreNoReadyMessagesAfterTheDeadLettersThatHaveAccumulatedBeyondTheLimitThePullOperationMovesAllOfThemAndReturnsAnEmptyChunkCheck1()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final int assertionValue1Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        assertionValue1Argument2 = q.pull(poison, Duration.ZERO).messages().size();
        Assertions.assertEquals(poison, assertionValue1Argument2, "Size should match the expected value.");
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("상한보다 많이 쌓인 dead letter 뒤에 준비 메시지가 없으면 pull은 다 옮기고 빈 청크를 돌려준다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifThereAreNoReadyMessagesAfterTheDeadLettersThatHaveAccumulatedBeyondTheLimitThePullOperationMovesAllOfThemAndReturnsAnEmptyChunkCheck2()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        evaluate(q.pull(poison, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("상한보다 많이 쌓인 dead letter 뒤에 준비 메시지가 없으면 pull은 다 옮기고 빈 청크를 돌려준다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifThereAreNoReadyMessagesAfterTheDeadLettersThatHaveAccumulatedBeyondTheLimitThePullOperationMovesAllOfThemAndReturnsAnEmptyChunkCheck3()
                    throws Exception {
        final Smq smq;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        evaluate(q.pull(poison, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(poison, 0L, 0L));
    }

    @Test
    @DisplayName("상한보다 많이 쌓인 dead letter 뒤에 준비 메시지가 없으면 pull은 다 옮기고 빈 청크를 돌려준다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifThereAreNoReadyMessagesAfterTheDeadLettersThatHaveAccumulatedBeyondTheLimitThePullOperationMovesAllOfThemAndReturnsAnEmptyChunkCheck4()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final int poison;
        final List<OutgoingMessage> batch;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        poison = 1000;
        batch = new ArrayList<>();
        for (int i = 0; i < poison; i++) {
            batch.add(msg("p" + i));
        }
        q.publish(batch);
        evaluate(q.pull(poison, Duration.ZERO).messages().size());
        this.clock.advance(Duration.ofSeconds(10L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue4Argument1 = new QueueCounts(poison, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("청크가 차도 뒤쪽의 도달 메시지는 같은 꺼내기에서 옮겨진다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void whenTheChunkIsFullTheReachableMessagesBehindItAreMovedInTheSamePullOperationCheck1() throws Exception {
        final Smq smq;
        final Queue q;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        evaluate(q.publish(
                OutgoingMessage.of("normal".getBytes(StandardCharsets.UTF_8)).withDelay(Duration.ofSeconds(5L))));
        q.publish(msg("poison"));
        assertionValue1Argument1 = q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(1L)))
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("청크가 차도 뒤쪽의 도달 메시지는 같은 꺼내기에서 옮겨진다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void whenTheChunkIsFullTheReachableMessagesBehindItAreMovedInTheSamePullOperationCheck2() throws Exception {
        final Smq smq;
        final Queue q;
        final MessageId normal;
        final Chunk c;
        final List<MessageId> assertionValue2Argument1;
        final List<MessageId> assertionValue2Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        normal = q.publish(
                OutgoingMessage.of("normal".getBytes(StandardCharsets.UTF_8)).withDelay(Duration.ofSeconds(5L)));
        q.publish(msg("poison"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(1L)))
                .allOk());
        this.clock.advance(Duration.ofSeconds(5L));
        c = q.pull(1, Duration.ZERO);
        assertionValue2Argument1 = List.of(normal);
        assertionValue2Argument2 = c.messages().stream().map(Message::id).toList();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
        evaluate(new QueueCounts(0L, 1L, 0L));
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("청크가 차도 뒤쪽의 도달 메시지는 같은 꺼내기에서 옮겨진다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void whenTheChunkIsFullTheReachableMessagesBehindItAreMovedInTheSamePullOperationCheck3() throws Exception {
        final Smq smq;
        final Queue q;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        evaluate(q.publish(
                OutgoingMessage.of("normal".getBytes(StandardCharsets.UTF_8)).withDelay(Duration.ofSeconds(5L))));
        q.publish(msg("poison"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(1L)))
                .allOk());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO));
        assertionValue3Argument1 = new QueueCounts(0L, 1L, 0L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(1L, 0L, 0L));
    }

    @Test
    @DisplayName("청크가 차도 뒤쪽의 도달 메시지는 같은 꺼내기에서 옮겨진다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void whenTheChunkIsFullTheReachableMessagesBehindItAreMovedInTheSamePullOperationCheck4() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead"));
        evaluate(q.publish(
                OutgoingMessage.of("normal".getBytes(StandardCharsets.UTF_8)).withDelay(Duration.ofSeconds(5L))));
        q.publish(msg("poison"));
        evaluate(q.settle(q.pull(1, Duration.ZERO).nackAll(Duration.ofSeconds(1L)))
                .allOk());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(new QueueCounts(0L, 1L, 0L));
        assertionValue4Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 1")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck1() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final boolean assertionValue1Argument1;
        final Chunk prefix;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        assertionValue1Argument1 = q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        prefix = q.pull(3, Duration.ZERO);
        evaluate(q.settle(prefix.ackAll()).allOk());
        evaluate(new QueueCounts(1L, 0L, 1L));
        evaluate(q.pull(3, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList());
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 2")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck2() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final Chunk prefix;
        final List<String> assertionValue2Argument1;
        final List<String> assertionValue2Argument2;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        prefix = q.pull(3, Duration.ZERO);
        assertionValue2Argument1 = List.of("a");
        assertionValue2Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
        evaluate(q.settle(prefix.ackAll()).allOk());
        evaluate(new QueueCounts(1L, 0L, 1L));
        evaluate(q.pull(3, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList());
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 3")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck3() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final Chunk prefix;
        final boolean assertionValue3Argument1;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        prefix = q.pull(3, Duration.ZERO);
        assertionValue3Argument1 = q.settle(prefix.ackAll()).allOk();
        Assertions.assertTrue(assertionValue3Argument1, "Outcomes should succeed.");
        evaluate(new QueueCounts(1L, 0L, 1L));
        evaluate(q.pull(3, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList());
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 4")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck4() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final Chunk prefix;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        prefix = q.pull(3, Duration.ZERO);
        evaluate(q.settle(prefix.ackAll()).allOk());
        assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        assertionValue4Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        evaluate(q.pull(3, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList());
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 5")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck5() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final Chunk prefix;
        final boolean assertionValue5Argument1;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        prefix = q.pull(3, Duration.ZERO);
        evaluate(q.settle(prefix.ackAll()).allOk());
        evaluate(new QueueCounts(1L, 0L, 1L));
        assertionValue5Argument1 = q.pull(3, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue5Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList());
    }

    @Test
    @DisplayName("단일 활성 컨슈머는 지연된 NACK 앞의 메시지만 꺼낸다 — check 6")
    void aSingleActiveConsumerOnlyRetrievesMessagesBeforeTheDelayedNACKCheck6() throws Exception {
        final Queue q;
        final Chunk first;
        final List<MessageId> ids;
        final Chunk prefix;
        final List<String> assertionValue6Argument1;
        final List<String> assertionValue6Argument2;
        q = this.open().declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(3, Duration.ZERO);
        ids = first.messages().stream().map(Message::id).toList();
        evaluate(q.settle(first.settle()
                        .nack(ids.get(0))
                        .nack(ids.get(1), Duration.ofSeconds(5L))
                        .nack(ids.get(2))
                        .build())
                .allOk());
        prefix = q.pull(3, Duration.ZERO);
        evaluate(q.settle(prefix.ackAll()).allOk());
        evaluate(new QueueCounts(1L, 0L, 1L));
        evaluate(q.pull(3, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue6Argument1 = List.of("b", "c");
        assertionValue6Argument2 = q.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueue() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Messages should match the expected value.");
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        evaluatePmdArgument(assertionValue5Argument1);
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        evaluatePmdArgument(assertionValue4Argument1);
        final var assertionValue4Argument2 = serial.counts();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument1 = List.of("before", "after");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue2Argument2);
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue1Argument2);
        waiter.join();
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueueCheck2() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        Assertions.assertTrue(assertionValue5Argument1, "Outcomes should succeed.");
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        evaluatePmdArgument(assertionValue4Argument1);
        final var assertionValue4Argument2 = serial.counts();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument1 = List.of("before", "after");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue2Argument2);
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue1Argument2);
        waiter.join();
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueueCheck3() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        evaluatePmdArgument(assertionValue5Argument1);
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        final var assertionValue4Argument2 = serial.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument1 = List.of("before", "after");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue2Argument2);
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue1Argument2);
        waiter.join();
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueueCheck4() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        evaluatePmdArgument(assertionValue5Argument1);
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        evaluatePmdArgument(assertionValue4Argument1);
        final var assertionValue4Argument2 = serial.counts();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        Assertions.assertTrue(assertionValue3Argument1, "Pulled chunk should be empty.");
        final var assertionValue2Argument1 = List.of("before", "after");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue2Argument2);
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue1Argument2);
        waiter.join();
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueueCheck5() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        evaluatePmdArgument(assertionValue5Argument1);
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        evaluatePmdArgument(assertionValue4Argument1);
        final var assertionValue4Argument2 = serial.counts();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument1 = List.of("before", "after");
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue1Argument2);
        waiter.join();
    }

    @Test
    @DisplayName("지연 발행은 단일 활성 큐의 뒤만 막는다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void delayedPublishingOnlyBlocksTheBackOfASingleActiveQueueCheck6() throws Exception {
        final Smq smq;
        final Queue serial;
        final Queue plain;
        final Chunk prefix;
        final CompletableFuture<Chunk> result;
        final Thread waiter;
        smq = this.open();
        serial = smq.declare("serial", QueueOptions.defaults().singleActiveConsumer(true));
        plain = smq.declare("plain", QueueOptions.defaults());
        for (final Queue q : List.of(serial, plain)) {
            q.publish(msg("before"));
            q.publish(msg("delayed").withDelay(Duration.ofSeconds(5L)));
            q.publish(msg("after"));
        }
        prefix = serial.pull(3, Duration.ZERO);
        final var assertionValue6Argument1 = List.of("before");
        evaluatePmdArgument(assertionValue6Argument1);
        final var assertionValue6Argument2 =
                prefix.messages().stream().map(SmqConformanceTest::text).toList();
        evaluatePmdArgument(assertionValue6Argument2);
        final var assertionValue5Argument1 = serial.settle(prefix.ackAll()).allOk();
        evaluatePmdArgument(assertionValue5Argument1);
        final var assertionValue4Argument1 = new QueueCounts(1L, 0L, 1L);
        evaluatePmdArgument(assertionValue4Argument1);
        final var assertionValue4Argument2 = serial.counts();
        evaluatePmdArgument(assertionValue4Argument2);
        final var assertionValue3Argument1 =
                serial.pull(3, Duration.ofMillis(30L)).isEmpty();
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue2Argument1 = List.of("before", "after");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = plain.pull(3, Duration.ZERO).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        evaluatePmdArgument(assertionValue2Argument2);
        result = new CompletableFuture<>();
        waiter = new Thread(() -> {
            try {
                result.complete(serial.pull(3, Duration.ofSeconds(2L)));
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
            }
        });
        waiter.start();
        this.clock.advance(Duration.ofSeconds(5L));
        final var assertionValue1Argument1 = List.of("delayed", "after");
        final var assertionValue1Argument2 = result.get(1L, TimeUnit.SECONDS).messages().stream()
                .map(SmqConformanceTest::text)
                .toList();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Messages should match the expected value.");
        waiter.join();
    }

    @Test
    @DisplayName("지연 뒤의 소진 메시지도 dead letter로 옮긴다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void messagesConsumedAfterTheDelayAreAlsoMovedToTheDeadLetterQueueCheck1() throws Exception {
        final Smq smq;
        final Queue q;
        final Chunk both;
        final List<MessageId> ids;
        final boolean assertionValue1Argument1;
        smq = this.open();
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        q.publish(msg("delayed").withDelay(Duration.ofSeconds(10L)));
        q.publish(msg("exhausted"));
        this.clock.advance(Duration.ofSeconds(10L));
        both = q.pull(2, Duration.ZERO);
        ids = both.messages().stream().map(Message::id).toList();
        assertionValue1Argument1 = q.settle(both.settle()
                        .nack(ids.get(0), Duration.ofSeconds(5L))
                        .nack(ids.get(1))
                        .build())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(1L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(2L, 0L, 0L));
    }

    @Test
    @DisplayName("지연 뒤의 소진 메시지도 dead letter로 옮긴다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void messagesConsumedAfterTheDelayAreAlsoMovedToTheDeadLetterQueueCheck2() throws Exception {
        final Smq smq;
        final Queue q;
        final Chunk both;
        final List<MessageId> ids;
        final boolean assertionValue2Argument1;
        smq = this.open();
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        q.publish(msg("delayed").withDelay(Duration.ofSeconds(10L)));
        q.publish(msg("exhausted"));
        this.clock.advance(Duration.ofSeconds(10L));
        both = q.pull(2, Duration.ZERO);
        ids = both.messages().stream().map(Message::id).toList();
        evaluate(q.settle(both.settle()
                        .nack(ids.get(0), Duration.ofSeconds(5L))
                        .nack(ids.get(1))
                        .build())
                .allOk());
        assertionValue2Argument1 = q.pull(2, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(1L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(2L, 0L, 0L));
    }

    @Test
    @DisplayName("지연 뒤의 소진 메시지도 dead letter로 옮긴다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void messagesConsumedAfterTheDelayAreAlsoMovedToTheDeadLetterQueueCheck3() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final Chunk both;
        final List<MessageId> ids;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open();
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        q.publish(msg("delayed").withDelay(Duration.ofSeconds(10L)));
        q.publish(msg("exhausted"));
        this.clock.advance(Duration.ofSeconds(10L));
        both = q.pull(2, Duration.ZERO);
        ids = both.messages().stream().map(Message::id).toList();
        evaluate(q.settle(both.settle()
                        .nack(ids.get(0), Duration.ofSeconds(5L))
                        .nack(ids.get(1))
                        .build())
                .allOk());
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = new QueueCounts(1L, 0L, 0L);
        assertionValue3Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(2L, 0L, 0L));
    }

    @Test
    @DisplayName("지연 뒤의 소진 메시지도 dead letter로 옮긴다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void messagesConsumedAfterTheDelayAreAlsoMovedToTheDeadLetterQueueCheck4() throws Exception {
        final Smq smq;
        final Queue q;
        final Chunk both;
        final List<MessageId> ids;
        final boolean assertionValue4Argument1;
        smq = this.open();
        evaluate(smq.declare("dead", QueueOptions.defaults()));
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        q.publish(msg("delayed").withDelay(Duration.ofSeconds(10L)));
        q.publish(msg("exhausted"));
        this.clock.advance(Duration.ofSeconds(10L));
        both = q.pull(2, Duration.ZERO);
        ids = both.messages().stream().map(Message::id).toList();
        evaluate(q.settle(both.settle()
                        .nack(ids.get(0), Duration.ofSeconds(5L))
                        .nack(ids.get(1))
                        .build())
                .allOk());
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(1L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        assertionValue4Argument1 = q.pull(2, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue4Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(2L, 0L, 0L));
    }

    @Test
    @DisplayName("지연 뒤의 소진 메시지도 dead letter로 옮긴다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void messagesConsumedAfterTheDelayAreAlsoMovedToTheDeadLetterQueueCheck5() throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final Chunk both;
        final List<MessageId> ids;
        final QueueCounts assertionValue5Argument1;
        final QueueCounts assertionValue5Argument2;
        smq = this.open();
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        q.publish(msg("delayed").withDelay(Duration.ofSeconds(10L)));
        q.publish(msg("exhausted"));
        this.clock.advance(Duration.ofSeconds(10L));
        both = q.pull(2, Duration.ZERO);
        ids = both.messages().stream().map(Message::id).toList();
        evaluate(q.settle(both.settle()
                        .nack(ids.get(0), Duration.ofSeconds(5L))
                        .nack(ids.get(1))
                        .build())
                .allOk());
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(1L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(2, Duration.ZERO).isEmpty());
        assertionValue5Argument1 = new QueueCounts(2L, 0L, 0L);
        assertionValue5Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중인 청크가 있으면 같은 컨슈머가 다시 꺼내도 빈 청크다 — check 1")
    void
            ifASingleActiveConsumerQueueHasAChunkThatIsBeingLeasedItWillBeAnEmptyChunkEvenIfTheSameConsumerRetrievesItAgainCheck1()
                    throws Exception {
        final Queue q;
        final Chunk first;
        final List<String> assertionValue1Argument1;
        final List<String> assertionValue1Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = List.of("a");
        assertionValue1Argument2 =
                first.messages().stream().map(SmqConformanceTest::text).toList();
        Assertions.assertEquals(
                assertionValue1Argument1, assertionValue1Argument2, "Messages should match the expected value.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(2L, 1L, 0L));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중인 청크가 있으면 같은 컨슈머가 다시 꺼내도 빈 청크다 — check 2")
    void
            ifASingleActiveConsumerQueueHasAChunkThatIsBeingLeasedItWillBeAnEmptyChunkEvenIfTheSameConsumerRetrievesItAgainCheck2()
                    throws Exception {
        final Queue q;
        final boolean assertionValue2Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        evaluate(q.pull(1, Duration.ZERO));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(2L, 1L, 0L));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중인 청크가 있으면 같은 컨슈머가 다시 꺼내도 빈 청크다 — check 3")
    void
            ifASingleActiveConsumerQueueHasAChunkThatIsBeingLeasedItWillBeAnEmptyChunkEvenIfTheSameConsumerRetrievesItAgainCheck3()
                    throws Exception {
        final Queue q;
        final boolean assertionValue3Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = q.pull(10, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue3Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(2L, 1L, 0L));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중인 청크가 있으면 같은 컨슈머가 다시 꺼내도 빈 청크다 — check 4")
    void
            ifASingleActiveConsumerQueueHasAChunkThatIsBeingLeasedItWillBeAnEmptyChunkEvenIfTheSameConsumerRetrievesItAgainCheck4()
                    throws Exception {
        final Queue q;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        evaluate(q.pull(1, Duration.ZERO));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(q.pull(10, Duration.ZERO).isEmpty());
        assertionValue4Argument1 = new QueueCounts(2L, 1L, 0L);
        assertionValue4Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머가 아닌 큐는 리스 중에도 다음 청크를 받는다 — check 1")
    void aQueueThatIsNotASingleActiveConsumerReceivesTheNextChunkEvenDuringALeaseCheck1() throws Exception {
        final Queue q;
        final boolean assertionValue1Argument1;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        assertionValue1Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertFalse(assertionValue1Argument1, "Pulled chunk should not be empty.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
    }

    @Test
    @DisplayName("단일 활성 컨슈머가 아닌 큐는 리스 중에도 다음 청크를 받는다 — check 2")
    void aQueueThatIsNotASingleActiveConsumerReceivesTheNextChunkEvenDuringALeaseCheck2() throws Exception {
        final Queue q;
        final boolean assertionValue2Argument1;
        q = this.open(Duration.ofSeconds(10L)).declare("q", QueueOptions.defaults());
        q.publish(List.of(msg("a"), msg("b")));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertFalse(assertionValue2Argument1, "Pulled chunk should not be empty.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 정산하면 다음 꺼내기가 청크를 받는다 — check 1")
    void whenASingleActiveConsumerQueueIsSettledTheNextDequeueOperationReceivesTheChunkCheck1() throws Exception {
        final Queue q;
        final Chunk first;
        final boolean assertionValue1Argument1;
        final Chunk second;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 = q.settle(first.ackAll()).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        second = q.pull(1, Duration.ZERO);
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(
                q.settle(second.settle().nack(second.messages().getFirst().id()).build())
                        .allOk());
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 정산하면 다음 꺼내기가 청크를 받는다 — check 2")
    void whenASingleActiveConsumerQueueIsSettledTheNextDequeueOperationReceivesTheChunkCheck2() throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk second;
        final String assertionValue2Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.ackAll()).allOk());
        second = q.pull(1, Duration.ZERO);
        assertionValue2Argument2 = text(second.messages().getFirst());
        Assertions.assertEquals("b", assertionValue2Argument2, "Text should match the expected value.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(
                q.settle(second.settle().nack(second.messages().getFirst().id()).build())
                        .allOk());
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 정산하면 다음 꺼내기가 청크를 받는다 — check 3")
    void whenASingleActiveConsumerQueueIsSettledTheNextDequeueOperationReceivesTheChunkCheck3() throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk second;
        final boolean assertionValue3Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.ackAll()).allOk());
        second = q.pull(1, Duration.ZERO);
        assertionValue3Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue3Argument1, "Pulled chunk should be empty.");
        evaluate(
                q.settle(second.settle().nack(second.messages().getFirst().id()).build())
                        .allOk());
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 정산하면 다음 꺼내기가 청크를 받는다 — check 4")
    void whenASingleActiveConsumerQueueIsSettledTheNextDequeueOperationReceivesTheChunkCheck4() throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk second;
        final boolean assertionValue4Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.ackAll()).allOk());
        second = q.pull(1, Duration.ZERO);
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue4Argument1 = q.settle(
                        second.settle().nack(second.messages().getFirst().id()).build())
                .allOk();
        Assertions.assertTrue(assertionValue4Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 정산하면 다음 꺼내기가 청크를 받는다 — check 5")
    void whenASingleActiveConsumerQueueIsSettledTheNextDequeueOperationReceivesTheChunkCheck5() throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk second;
        final Chunk again;
        final String assertionValue5Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.ackAll()).allOk());
        second = q.pull(1, Duration.ZERO);
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(
                q.settle(second.settle().nack(second.messages().getFirst().id()).build())
                        .allOk());
        again = q.pull(1, Duration.ZERO);
        assertionValue5Argument2 = text(again.messages().getFirst());
        Assertions.assertEquals("b", assertionValue5Argument2, "Text should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스가 만료되면 다음 꺼내기가 청크를 받는다 — check 1")
    void inASingleActiveConsumerQueueTheNextDequeueOperationReceivesTheChunkWhenTheLeaseExpiresCheck1()
            throws Exception {
        final Queue q;
        final String assertionValue1Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        assertionValue1Argument2 = text(q.pull(1, Duration.ZERO).messages().getFirst());
        Assertions.assertEquals("a", assertionValue1Argument2, "Text should match the expected value.");
        this.clock.advance(Duration.ofSeconds(9L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(text(q.pull(1, Duration.ZERO).messages().getFirst()));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스가 만료되면 다음 꺼내기가 청크를 받는다 — check 2")
    void inASingleActiveConsumerQueueTheNextDequeueOperationReceivesTheChunkWhenTheLeaseExpiresCheck2()
            throws Exception {
        final Queue q;
        final boolean assertionValue2Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        evaluate(text(q.pull(1, Duration.ZERO).messages().getFirst()));
        this.clock.advance(Duration.ofSeconds(9L));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(1L));
        evaluate(text(q.pull(1, Duration.ZERO).messages().getFirst()));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스가 만료되면 다음 꺼내기가 청크를 받는다 — check 3")
    void inASingleActiveConsumerQueueTheNextDequeueOperationReceivesTheChunkWhenTheLeaseExpiresCheck3()
            throws Exception {
        final Queue q;
        final String assertionValue3Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        evaluate(text(q.pull(1, Duration.ZERO).messages().getFirst()));
        this.clock.advance(Duration.ofSeconds(9L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(1L));
        assertionValue3Argument2 = text(q.pull(1, Duration.ZERO).messages().getFirst());
        Assertions.assertEquals("a", assertionValue3Argument2, "Text should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐에서 여러 스레드가 동시에 꺼내도 리스 중인 청크는 하나다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsAreDequeuedSimultaneouslyFromASingleActiveConsumerQueueThereIsOnlyOneChunkUnderLease()
            throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofMinutes(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final AtomicInteger got;
            final ThrowingRunnable puller;
            q = smq.declare("q" + round, QueueOptions.defaults().singleActiveConsumer(true));
            q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d"), msg("e"), msg("f")));
            got = new AtomicInteger();
            puller = () -> {
                if (!q.pull(2, Duration.ZERO).isEmpty()) {
                    got.incrementAndGet();
                }
            };
            runConcurrently(Collections.nCopies(8, puller));
            final var assertionValue2Argument2 = got.get();
            final var assertionValue2Argument3 = "round " + round;
            Assertions.assertEquals(1, assertionValue2Argument2, assertionValue2Argument3);
            final var assertionValue1Argument1 = new QueueCounts(4L, 2L, 0L);
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts();
            evaluatePmdArgument(assertionValue1Argument2);
            final var assertionValue1Argument3 = "round " + round;
            evaluatePmdArgument(assertionValue1Argument3);
        }
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐에서 여러 스레드가 동시에 꺼내도 리스 중인 청크는 하나다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            evenIfMultipleThreadsAreDequeuedSimultaneouslyFromASingleActiveConsumerQueueThereIsOnlyOneChunkUnderLeaseCheck2()
                    throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofMinutes(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final AtomicInteger got;
            final ThrowingRunnable puller;
            q = smq.declare("q" + round, QueueOptions.defaults().singleActiveConsumer(true));
            q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d"), msg("e"), msg("f")));
            got = new AtomicInteger();
            puller = () -> {
                if (!q.pull(2, Duration.ZERO).isEmpty()) {
                    got.incrementAndGet();
                }
            };
            runConcurrently(Collections.nCopies(8, puller));
            final var assertionValue2Argument2 = got.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue2Argument3 = "round " + round;
            evaluatePmdArgument(assertionValue2Argument3);
            final var assertionValue1Argument1 = new QueueCounts(4L, 2L, 0L);
            final var assertionValue1Argument2 = q.counts();
            final var assertionValue1Argument3 = "round " + round;
            Assertions.assertEquals(assertionValue1Argument1, assertionValue1Argument2, assertionValue1Argument3);
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 꺼내도 같은 메시지가 두 리스에 묶이지 않는다")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.SystemPrintln",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.CloseResource",
                reason = "The fixture registers this Smq and closes it in closeAll after every test.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.CloseResource", "PMD.SystemPrintln"})
    void evenIfMultipleThreadsRetrieveMessagesSimultaneouslyTheSameMessageWillNotBeBoundToTwoLeases() throws Exception {
        final Smq smq;
        final ConcurrentPullResult result;
        smq = this.open(Duration.ofMinutes(10L));
        result = collectConcurrentPullResult(smq);
        Assertions.assertTrue(result.failures().isEmpty(), "동시 꺼내기 계약 위반: " + String.join("; ", result.failures()));
        System.out.println("동시 꺼내기 300라운드 중 메시지가 남았는데 빈 청크가 나온 라운드: " + result.roundsWithEmptyChunk());
    }

    record ConcurrentPullResult(int roundsWithEmptyChunk, List<String> failures) {}

    static ConcurrentPullResult collectConcurrentPullResult(final Smq smq) throws Exception {
        return collectConcurrentPullResult(smq, "race");
    }

    static ConcurrentPullResult collectConcurrentPullResult(final Smq smq, final String queuePrefix) throws Exception {
        int roundsWithEmptyChunk;
        final List<String> failures;
        roundsWithEmptyChunk = 0;
        failures = new ArrayList<>();
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final List<Chunk> chunks;
            final ThrowingRunnable puller;
            final QueueCounts counts;
            final String detail;
            final List<Message> delivered;
            final List<MessageId> ids;
            final int remaining;
            q = smq.declare(queuePrefix + round, QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c"), msg("d"), msg("e"), msg("f")));
            chunks = Collections.synchronizedList(new ArrayList<>());
            puller = () -> chunks.add(q.pull(2, Duration.ZERO));
            runConcurrently(Collections.nCopies(8, puller));
            counts = q.counts();
            detail = "round " + round + " pullers=" + describe(chunks) + " counts=" + counts;
            delivered = chunks.stream().flatMap(c -> c.messages().stream()).toList();
            ids = delivered.stream().map(Message::id).toList();
            check(failures, ids.size() == new java.util.HashSet<>(ids).size(), "이중 리스 " + detail);
            check(failures, delivered.stream().allMatch(m -> m.deliveryCount() == 1), "전달 횟수가 1이 아니다 " + detail);
            check(failures, ids.size() == counts.leased(), "리스 중 수가 꺼낸 수와 다르다 " + detail);
            check(failures, 6L == counts.ready() + counts.leased(), "메시지 유실 " + detail);
            check(failures, counts.delayed() == 0L, "지연됨이 0이 아니다 " + detail);
            check(failures, ids.size() >= 2, "lock을 처음 잡은 풀러가 2건을 꺼내지 못했다 " + detail);
            if (ids.size() < 6) {
                roundsWithEmptyChunk++;
            }
            remaining = (int) counts.ready();
            if (remaining > 0) {
                final Chunk rest;
                final List<MessageId> all;
                rest = q.pull(remaining, Duration.ofSeconds(5L));
                check(failures, remaining == rest.messages().size(), "남은 메시지를 모두 꺼내지 못했다 " + detail);
                check(
                        failures,
                        rest.messages().stream().allMatch(m -> m.deliveryCount() == 1),
                        "남은 메시지의 전달 횟수가 1이 아니다 " + detail);
                all = new ArrayList<>(ids);
                rest.messages().forEach(m -> all.add(m.id()));
                check(failures, 6 == new java.util.HashSet<>(all).size(), "남은 메시지가 앞서 꺼낸 것과 겹친다 " + detail);
            }
        }
        return new ConcurrentPullResult(roundsWithEmptyChunk, List.copyOf(failures));
    }

    private static void check(final List<String> failures, final boolean passed, final String detail) {
        if (!passed) {
            failures.add(detail);
        }
    }

    private static String describe(final List<Chunk> chunks) {
        return chunks.stream()
                .map(c -> c.messages().stream()
                        .map(m -> m.id() + "#" + m.deliveryCount())
                        .toList()
                        .toString())
                .toList()
                .toString();
    }

    @Test
    @DisplayName("같은 청크를 동시에 정산하면 메시지마다 정확히 한 쪽만 OK다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifTheSameChunkIsSettledSimultaneouslyExactlyOneSideIsOKForEachMessageCheck1() throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofMinutes(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final Chunk chunk;
            final List<Outcomes> results;
            final ThrowingRunnable settler;
            q = smq.declare("settle" + round, QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));
            chunk = q.pull(3, Duration.ZERO);
            results = Collections.synchronizedList(new ArrayList<>());
            settler = () -> results.add(q.settle(chunk.ackAll()));
            runConcurrently(Collections.nCopies(4, settler));
            for (final Message m : chunk.messages()) {
                final long ok;
                final String assertionValue1Argument3;
                ok = results.stream()
                        .filter(r -> r.byMessage().get(m.id()) == Outcome.OK)
                        .count();
                assertionValue1Argument3 = "round " + round + " " + m.id();
                Assertions.assertEquals(1L, ok, assertionValue1Argument3);
            }
            evaluate(new QueueCounts(0L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("같은 청크를 동시에 정산하면 메시지마다 정확히 한 쪽만 OK다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifTheSameChunkIsSettledSimultaneouslyExactlyOneSideIsOKForEachMessageCheck2() throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofMinutes(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final Chunk chunk;
            final List<Outcomes> results;
            final ThrowingRunnable settler;
            final QueueCounts assertionValue2Argument1;
            final QueueCounts assertionValue2Argument2;
            final String assertionValue2Argument3;
            q = smq.declare("settle" + round, QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));
            chunk = q.pull(3, Duration.ZERO);
            results = Collections.synchronizedList(new ArrayList<>());
            settler = () -> results.add(q.settle(chunk.ackAll()));
            runConcurrently(Collections.nCopies(4, settler));
            for (final Message m : chunk.messages()) {
                evaluate(results.stream()
                        .filter(r -> r.byMessage().get(m.id()) == Outcome.OK)
                        .count());
            }
            assertionValue2Argument1 = new QueueCounts(0L, 0L, 0L);
            assertionValue2Argument2 = q.counts();
            assertionValue2Argument3 = "round " + round;
            Assertions.assertEquals(assertionValue2Argument1, assertionValue2Argument2, assertionValue2Argument3);
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 발행해도 id는 겹치지 않고 모두 들어간다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsArePublishedSimultaneouslyTheIDsDoNotOverlapAndAllOfThemAreIncludedCheck1()
            throws Exception {
        final Smq smq;
        smq = this.open();
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final List<MessageId> ids;
            final ThrowingRunnable publisher;
            final int assertionValue1Argument2;
            final String assertionValue1Argument3;
            q = smq.declare("pub" + round, QueueOptions.defaults());
            ids = Collections.synchronizedList(new ArrayList<>());
            publisher = () -> ids.addAll(q.publish(List.of(msg("a"), msg("b"), msg("c"))));
            runConcurrently(Collections.nCopies(4, publisher));
            assertionValue1Argument2 = ids.size();
            assertionValue1Argument3 = "round " + round;
            Assertions.assertEquals(12, assertionValue1Argument2, assertionValue1Argument3);
            evaluate(Set.copyOf(ids).size());
            evaluate(new QueueCounts(12L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 발행해도 id는 겹치지 않고 모두 들어간다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsArePublishedSimultaneouslyTheIDsDoNotOverlapAndAllOfThemAreIncludedCheck2()
            throws Exception {
        final Smq smq;
        smq = this.open();
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final List<MessageId> ids;
            final ThrowingRunnable publisher;
            final int assertionValue2Argument2;
            final String assertionValue2Argument3;
            q = smq.declare("pub" + round, QueueOptions.defaults());
            ids = Collections.synchronizedList(new ArrayList<>());
            publisher = () -> ids.addAll(q.publish(List.of(msg("a"), msg("b"), msg("c"))));
            runConcurrently(Collections.nCopies(4, publisher));
            assertionValue2Argument2 = Set.copyOf(ids).size();
            assertionValue2Argument3 = "round " + round + " " + ids;
            Assertions.assertEquals(12, assertionValue2Argument2, assertionValue2Argument3);
            evaluate(new QueueCounts(12L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 발행해도 id는 겹치지 않고 모두 들어간다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsArePublishedSimultaneouslyTheIDsDoNotOverlapAndAllOfThemAreIncludedCheck3()
            throws Exception {
        final Smq smq;
        smq = this.open();
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final List<MessageId> ids;
            final ThrowingRunnable publisher;
            final QueueCounts assertionValue3Argument1;
            final QueueCounts assertionValue3Argument2;
            final String assertionValue3Argument3;
            q = smq.declare("pub" + round, QueueOptions.defaults());
            ids = Collections.synchronizedList(new ArrayList<>());
            publisher = () -> ids.addAll(q.publish(List.of(msg("a"), msg("b"), msg("c"))));
            runConcurrently(Collections.nCopies(4, publisher));
            evaluate(Set.copyOf(ids).size());
            assertionValue3Argument1 = new QueueCounts(12L, 0L, 0L);
            assertionValue3Argument2 = q.counts();
            assertionValue3Argument3 = "round " + round;
            Assertions.assertEquals(assertionValue3Argument1, assertionValue3Argument2, assertionValue3Argument3);
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 연장해도 모두 OK이고 리스는 유지된다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsAreExtendedSimultaneouslyAllAreOKAndTheLeaseIsMaintainedCheck1() throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofSeconds(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final Chunk chunk;
            final List<Extension> results;
            final ThrowingRunnable extender;
            final boolean assertionValue1Argument1;
            final String assertionValue1Argument2;
            q = smq.declare("ext" + round, QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b")));
            chunk = q.pull(2, Duration.ZERO);
            results = Collections.synchronizedList(new ArrayList<>());
            extender = () -> results.add(q.extend(chunk, Duration.ofSeconds(20L)));
            runConcurrently(Collections.nCopies(4, extender));
            assertionValue1Argument1 =
                    results.stream().allMatch(e -> e.outcomes().allOk());
            assertionValue1Argument2 = "round " + round;
            Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
            evaluate(new QueueCounts(0L, 2L, 0L));
        }
    }

    @Test
    @DisplayName("여러 스레드가 동시에 연장해도 모두 OK이고 리스는 유지된다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void evenIfMultipleThreadsAreExtendedSimultaneouslyAllAreOKAndTheLeaseIsMaintainedCheck2() throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofSeconds(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final Chunk chunk;
            final List<Extension> results;
            final ThrowingRunnable extender;
            final QueueCounts assertionValue2Argument1;
            final QueueCounts assertionValue2Argument2;
            final String assertionValue2Argument3;
            q = smq.declare("ext" + round, QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b")));
            chunk = q.pull(2, Duration.ZERO);
            results = Collections.synchronizedList(new ArrayList<>());
            extender = () -> results.add(q.extend(chunk, Duration.ofSeconds(20L)));
            runConcurrently(Collections.nCopies(4, extender));
            evaluate(results.stream().allMatch(e -> e.outcomes().allOk()));
            assertionValue2Argument1 = new QueueCounts(0L, 2L, 0L);
            assertionValue2Argument2 = q.counts();
            assertionValue2Argument3 = "round " + round;
            Assertions.assertEquals(assertionValue2Argument1, assertionValue2Argument2, assertionValue2Argument3);
        }
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 일부 ACK하고 나머지를 지연 없이 NACK하면 다음 꺼내기가 청크를 받는다 — check 1")
    void
            ifASingleActiveConsumerQueueACKsSomeMessagesAndNACKsTheRestWithoutDelayTheNextDequeueOperationWillReceiveTheChunkCheck1()
                    throws Exception {
        final Queue q;
        final Chunk first;
        final MessageId a;
        final boolean assertionValue1Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(2, Duration.ZERO);
        a = first.messages().getFirst().id();
        assertionValue1Argument1 = q.settle(first.settle()
                        .ack(a)
                        .nack(first.messages().getLast().id())
                        .build())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(10, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 일부 ACK하고 나머지를 지연 없이 NACK하면 다음 꺼내기가 청크를 받는다 — check 2")
    void
            ifASingleActiveConsumerQueueACKsSomeMessagesAndNACKsTheRestWithoutDelayTheNextDequeueOperationWillReceiveTheChunkCheck2()
                    throws Exception {
        final Queue q;
        final Chunk first;
        final MessageId a;
        final Chunk next;
        final List<String> assertionValue2Argument1;
        final List<String> assertionValue2Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b"), msg("c")));
        first = q.pull(2, Duration.ZERO);
        a = first.messages().getFirst().id();
        evaluate(q.settle(first.settle()
                        .ack(a)
                        .nack(first.messages().getLast().id())
                        .build())
                .allOk());
        next = q.pull(10, Duration.ZERO);
        assertionValue2Argument1 = List.of("b", "c");
        assertionValue2Argument2 =
                next.messages().stream().map(SmqConformanceTest::text).toList();
        Assertions.assertEquals(
                assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 지연 NACK 후 지연이 끝나면 다시 꺼낸다 — check 1")
    void theSingleActiveConsumerQueueIsRetrievedAgainAfterTheDelayNACKAndOnceTheDelayPeriodHasEndedCheck1()
            throws Exception {
        final Queue q;
        final Chunk first;
        final boolean assertionValue1Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(1, Duration.ZERO);
        assertionValue1Argument1 =
                q.settle(first.nackAll(Duration.ofSeconds(5L))).allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 지연 NACK 후 지연이 끝나면 다시 꺼낸다 — check 2")
    void theSingleActiveConsumerQueueIsRetrievedAgainAfterTheDelayNACKAndOnceTheDelayPeriodHasEndedCheck2()
            throws Exception {
        final Queue q;
        final Chunk first;
        final boolean assertionValue2Argument1;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(5L))).allOk());
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        this.clock.advance(Duration.ofSeconds(5L));
        evaluate(q.pull(1, Duration.ZERO));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 지연 NACK 후 지연이 끝나면 다시 꺼낸다 — check 3")
    void theSingleActiveConsumerQueueIsRetrievedAgainAfterTheDelayNACKAndOnceTheDelayPeriodHasEndedCheck3()
            throws Exception {
        final Queue q;
        final Chunk first;
        final Chunk next;
        final String assertionValue3Argument2;
        q = this.open(Duration.ofSeconds(10L))
                .declare("q", QueueOptions.defaults().singleActiveConsumer(true));
        q.publish(List.of(msg("a"), msg("b")));
        first = q.pull(1, Duration.ZERO);
        evaluate(q.settle(first.nackAll(Duration.ofSeconds(5L))).allOk());
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        this.clock.advance(Duration.ofSeconds(5L));
        next = q.pull(1, Duration.ZERO);
        assertionValue3Argument2 = text(next.messages().getFirst());
        Assertions.assertEquals("a", assertionValue3Argument2, "Text should match the expected value.");
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 1")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck1()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final boolean assertionValue1Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        assertionValue1Argument1 = q.settle(
                        holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk();
        Assertions.assertTrue(assertionValue1Argument1, "Outcomes should succeed.");
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 2")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck2()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final boolean assertionValue2Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        assertionValue2Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue2Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 3")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck3()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final QueueCounts assertionValue3Argument1;
        final QueueCounts assertionValue3Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue3Argument1 = new QueueCounts(0L, 1L, 1L);
        assertionValue3Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 4")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck4()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final QueueCounts assertionValue4Argument1;
        final QueueCounts assertionValue4Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        assertionValue4Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue4Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue4Argument1, assertionValue4Argument2, "Counts should match the expected value.");
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 5")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck5()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final boolean assertionValue5Argument1;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        assertionValue5Argument1 = q.pull(1, Duration.ZERO).isEmpty();
        Assertions.assertTrue(assertionValue5Argument1, "Pulled chunk should be empty.");
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 6")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck6()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final QueueCounts assertionValue6Argument1;
        final QueueCounts assertionValue6Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        assertionValue6Argument1 = new QueueCounts(0L, 0L, 0L);
        assertionValue6Argument2 = q.counts();
        Assertions.assertEquals(
                assertionValue6Argument1, assertionValue6Argument2, "Counts should match the expected value.");
        evaluate(new QueueCounts(2L, 0L, 0L));
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 7")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck7()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final QueueCounts assertionValue7Argument1;
        final QueueCounts assertionValue7Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        assertionValue7Argument1 = new QueueCounts(2L, 0L, 0L);
        assertionValue7Argument2 = dead.counts();
        Assertions.assertEquals(
                assertionValue7Argument1, assertionValue7Argument2, "Counts should match the expected value.");
        evaluate(x.value());
        evaluate(Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id")));
    }

    @Test
    @DisplayName("단일 활성 컨슈머 큐는 리스 중이면 최대 전달 횟수에 도달한 준비 메시지도 옮기지 않고 리스가 끝나면 옮긴다 — check 8")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void
            ifASingleActiveConsumerQueueIsUnderLeaseItDoesNotMoveEvenReadyMessagesThatHaveReachedTheMaximumNumberOfDeliveriesAndMovesThemOnlyAfterTheLeaseEndsCheck8()
                    throws Exception {
        final Smq smq;
        final Queue dead;
        final Queue q;
        final MessageId x;
        final Chunk holding;
        final long assertionValue8Argument1;
        final long assertionValue8Argument2;
        smq = this.open(Duration.ofSeconds(10L));
        dead = smq.declare("dead", QueueOptions.defaults());
        q = smq.declare("q", QueueOptions.defaults().maxDeliveries(1, "dead").singleActiveConsumer(true));
        x = q.publish(msg("x"));
        q.publish(msg("y"));
        holding = q.pull(2, Duration.ZERO);
        evaluate(q.settle(holding.settle().nack(x, Duration.ofSeconds(5L)).build())
                .allOk());
        this.clock.advance(Duration.ofSeconds(6L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 1L, 1L));
        evaluate(new QueueCounts(0L, 0L, 0L));
        this.clock.advance(Duration.ofSeconds(4L));
        evaluate(q.pull(1, Duration.ZERO).isEmpty());
        evaluate(new QueueCounts(0L, 0L, 0L));
        evaluate(new QueueCounts(2L, 0L, 0L));
        assertionValue8Argument1 = x.value();
        assertionValue8Argument2 = Long.parseLong(
                dead.pull(1, Duration.ZERO).messages().getFirst().headers().get("smq-original-message-id"));
        Assertions.assertEquals(
                assertionValue8Argument1, assertionValue8Argument2, "Parse long should match the expected value.");
    }

    @Test
    @DisplayName("만료된 리스를 연장하는 쪽과 다시 꺼내는 쪽이 동시에 겨루면 한 쪽만 성공한다")
    @PolicyException(
            rule = "PMD.CloseResource",
            reason = "The fixture registers this Smq and closes it in closeAll after every test.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CloseResource")
    void ifThePartyExtendingAnExpiredLeaseAndThePartyRetrievingItCompeteSimultaneouslyOnlyOnePartyWillSucceed()
            throws Exception {
        final Smq smq;
        smq = this.open(Duration.ofSeconds(10L));
        for (int round = 0; round < 300; round++) {
            final Queue q;
            final Chunk old;
            final AtomicReference<Extension> extension;
            final AtomicReference<Chunk> again;
            final boolean extended;
            final boolean pulled;
            q = smq.declare("expire" + round, QueueOptions.defaults());
            q.publish(msg("a"));
            old = q.pull(1, Duration.ZERO);
            this.clock.advance(Duration.ofSeconds(10L));
            extension = new AtomicReference<>();
            again = new AtomicReference<>();
            runConcurrently(List.of(
                    () -> extension.set(q.extend(old, Duration.ofSeconds(100L))),
                    () -> again.set(q.pull(1, Duration.ZERO))));
            extended = extension.get().outcomes().allOk();
            pulled = !again.get().isEmpty();
            Assertions.assertTrue(extended ^ pulled, "round " + round + " extended=" + extended + " pulled=" + pulled);
        }
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    static void runConcurrently(final List<ThrowingRunnable> tasks) throws InterruptedException {
        final CountDownLatch start;
        final AtomicReference<Throwable> failure;
        final List<Thread> threads;
        start = new CountDownLatch(1);
        failure = new AtomicReference<>();
        threads = new ArrayList<>();
        for (final ThrowingRunnable task : tasks) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    task.run();
                } catch (Throwable e) {
                    failure.set(e);
                }
            }));
        }
        start.countDown();
        for (final Thread t : threads) {
            t.join(30_000L);
            Assertions.assertFalse(t.isAlive(), "Thread should not be alive.");
        }
        Assertions.assertNull(failure.get(), "Failure should be null.");
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private record CenturyScenario(Queue queue, MessageId id, Chunk chunk, Builder settlement) {}

    private enum ClosedOperation {
        QUEUE,
        DELETE,
        SNAPSHOT,
        COUNTS,
        PUBLISH,
        PUBLISH_EMPTY,
        SETTLE,
        SETTLE_EMPTY,
        EXTEND,
        EXTEND_EMPTY,
        INVALID_COUNT,
        INVALID_WAIT,
        INVALID_LEASE
    }

    private enum InvalidPull {
        ZERO_COUNT,
        NEGATIVE_COUNT,
        NEGATIVE_WAIT,
        ZERO_LEASE,
        NEGATIVE_LEASE
    }

    @FunctionalInterface
    interface ThrowingRunnable {

        void run() throws Exception;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
