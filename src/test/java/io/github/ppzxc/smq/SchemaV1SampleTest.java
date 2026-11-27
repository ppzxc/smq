package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaV1SampleTest {
    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir
    Path dir;

    static final long IDS_M1 = 3L;
    static final long IDS_M3 = 5L;
    static final long IDS_M4 = 6L;

    private static String text(final Message m) {
        return new String(m.payload(), StandardCharsets.UTF_8);
    }

    private static List<String> texts(final Chunk chunk) {
        return chunk.messages().stream().map(SchemaV1SampleTest::text).toList();
    }

    private static Stream<Arguments> sampleQueueOptions() {
        return Stream.of(
                Arguments.of(new Object[] {"v1-dead", QueueOptions.defaults()}),
                Arguments.of(new Object[] {"v1-orders", QueueOptions.defaults().maxDeliveries(3, "v1-dead")}),
                Arguments.of(new Object[] {"v1-serial", QueueOptions.defaults().singleActiveConsumer(true)}));
    }

    @ParameterizedTest
    @MethodSource("sampleQueueOptions")
    @DisplayName("견본을 열면 큐 선언 옵션이 보존된다")
    void samplePreservesQueueOptions(final String queueName, final QueueOptions expected) throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            Assertions.assertEquals(
                    expected, smq.queue(queueName).options(), "The sample should preserve queue options.");
        }
    }

    @ParameterizedTest
    @CsvSource({"v1-dead, 1, 0, 0", "v1-orders, 2, 2, 1", "v1-serial, 1, 1, 0"})
    @DisplayName("견본을 열면 큐의 상태별 개수가 보존된다")
    void samplePreservesQueueCounts(final String queueName, final long ready, final long leased, final long delayed)
            throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            Assertions.assertEquals(
                    new QueueCounts(ready, leased, delayed),
                    smq.queue(queueName).counts(),
                    "The sample should preserve queue counts.");
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 1")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck1()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final List<String> assertionValue1Argument1;
            final List<String> assertionValue1Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            assertionValue1Argument1 = List.of("m3", "m4");
            assertionValue1Argument2 = texts(ready);
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Texts should match the expected value.");
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 2")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck2()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final List<Integer> assertionValue2Argument1;
            final List<Integer> assertionValue2Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            assertionValue2Argument1 = List.of(1, 1);
            assertionValue2Argument2 =
                    ready.messages().stream().map(Message::deliveryCount).toList();
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "Messages should match the expected value.");
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 3")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck3()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Map<String, String> assertionValue3Argument1;
            final Map<String, String> assertionValue3Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            assertionValue3Argument1 = Map.of("kind", "ready");
            assertionValue3Argument2 = ready.messages().getFirst().headers();
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Headers should match the expected value.");
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 4")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck4()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final List<MessageId> assertionValue4Argument1;
            final List<MessageId> assertionValue4Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            assertionValue4Argument1 = List.of(new MessageId(5L), new MessageId(6L));
            assertionValue4Argument2 =
                    ready.messages().stream().map(Message::id).toList();
            Assertions.assertEquals(
                    assertionValue4Argument1, assertionValue4Argument2, "Messages should match the expected value.");
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 5")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck5()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final boolean assertionValue5Argument1;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            assertionValue5Argument1 = orders.settle(ready.ackAll()).allOk();
            Assertions.assertTrue(assertionValue5Argument1, "Outcomes should succeed.");
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 6")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck6()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final boolean assertionValue6Argument1;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            assertionValue6Argument1 = orders.pull(10, Duration.ZERO).isEmpty();
            Assertions.assertTrue(assertionValue6Argument1, "Pulled chunk should be empty.");
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 7")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck7()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Chunk redelivered;
            final List<String> assertionValue7Argument1;
            final List<String> assertionValue7Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10L));
            assertionValue7Argument1 = List.of("m1", "m2");
            assertionValue7Argument2 = texts(redelivered);
            Assertions.assertEquals(
                    assertionValue7Argument1, assertionValue7Argument2, "Texts should match the expected value.");
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 8")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck8()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Chunk redelivered;
            final List<Integer> assertionValue8Argument1;
            final List<Integer> assertionValue8Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10L));
            assertionValue8Argument1 = List.of(3, 2);
            assertionValue8Argument2 =
                    redelivered.messages().stream().map(Message::deliveryCount).toList();
            Assertions.assertEquals(
                    assertionValue8Argument1, assertionValue8Argument2, "Messages should match the expected value.");
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 9")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck9()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Chunk redelivered;
            final Map<String, String> assertionValue9Argument1;
            final Map<String, String> assertionValue9Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10L));
            assertionValue9Argument1 = Map.of("kind", "retried", "n", "1");
            assertionValue9Argument2 = redelivered.messages().getFirst().headers();
            Assertions.assertEquals(
                    assertionValue9Argument1, assertionValue9Argument2, "Headers should match the expected value.");
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 10")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck10()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Chunk redelivered;
            final MessageId assertionValue10Argument1;
            final MessageId assertionValue10Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10L));
            assertionValue10Argument1 = new MessageId(3L);
            assertionValue10Argument2 = redelivered.messages().getFirst().id();
            Assertions.assertEquals(
                    assertionValue10Argument1,
                    assertionValue10Argument2,
                    "Message ID should match the expected value.");
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 11")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck11()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Chunk redelivered;
            final Instant assertionValue11Argument1;
            final Instant assertionValue11Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            redelivered = orders.pull(10, Duration.ZERO, Duration.ofHours(10L));
            evaluate(new MessageId(3L));
            assertionValue11Argument1 = T0.minusNanos(2L);
            assertionValue11Argument2 = redelivered.messages().getFirst().publishedAt();
            Assertions.assertEquals(
                    assertionValue11Argument1,
                    assertionValue11Argument2,
                    "Published at should match the expected value.");
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 12")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck12()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final QueueCounts assertionValue12Argument1;
            final QueueCounts assertionValue12Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            assertionValue12Argument1 = new QueueCounts(0L, 2L, 1L);
            assertionValue12Argument2 = orders.counts();
            Assertions.assertEquals(
                    assertionValue12Argument1, assertionValue12Argument2, "Counts should match the expected value.");
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 13")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck13()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final QueueCounts assertionValue13Argument1;
            final QueueCounts assertionValue13Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            assertionValue13Argument1 = new QueueCounts(0L, 2L, 1L);
            assertionValue13Argument2 = orders.counts();
            Assertions.assertEquals(
                    assertionValue13Argument1, assertionValue13Argument2, "Counts should match the expected value.");
            evaluate(orders.pull(10, Duration.ZERO).messages().getFirst());
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 14")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck14()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Message delayed;
            final String assertionValue14Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            delayed = orders.pull(10, Duration.ZERO).messages().getFirst();
            assertionValue14Argument2 = text(delayed);
            Assertions.assertEquals("m5", assertionValue14Argument2, "Text should match the expected value.");
        }
    }

    @Test
    @DisplayName("견본을 열면 메시지 id 전달 횟수 헤더가 보존되고 리스 기한에 재전달된다 — check 15")
    void whenTheSampleIsOpenedTheMessageIDForwardingCountHeaderIsPreservedAndReForwardedAtTheLeaseExpirationCheck15()
            throws Exception {
        final Path copy;
        final MutableClock clock;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("smq.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE message SET published_at = published_at - 2, available_at = published_at - 1\nWHERE id = ?\n"); ) {
            ps.setLong(1, 3L);
            ps.executeUpdate();
        }

        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(copy).clock(clock).open()) {
            final Queue orders;
            final Chunk ready;
            final Message delayed;
            final int assertionValue15Argument2;
            orders = smq.queue("v1-orders");
            ready = orders.pull(10, Duration.ZERO, Duration.ofMinutes(1L));
            evaluate(List.of(new MessageId(5L), new MessageId(6L)));
            evaluate(orders.settle(ready.ackAll()).allOk());
            clock.advance(Duration.ofHours(1L).minusSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofSeconds(1L));
            evaluate(orders.pull(10, Duration.ZERO, Duration.ofHours(10L)));
            evaluate(new MessageId(3L));
            evaluate(T0.minusNanos(2L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            clock.advance(Duration.ofHours(1L));
            evaluate(new QueueCounts(0L, 2L, 1L));
            delayed = orders.pull(10, Duration.ZERO).messages().getFirst();
            assertionValue15Argument2 = delayed.deliveryCount();
            Assertions.assertEquals(1, assertionValue15Argument2, "Delivery count should match the expected value.");
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 1")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck1() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final List<String> assertionValue1Argument1;
            final List<String> assertionValue1Argument2;
            final Queue serial;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            assertionValue1Argument1 = List.of("poison");
            assertionValue1Argument2 = texts(dead);
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Texts should match the expected value.");
            evaluate(dead.messages().getFirst().headers());
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 2")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck2() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Map<String, String> h;
            final String assertionValue2Argument2;
            final Queue serial;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            h = dead.messages().getFirst().headers();
            assertionValue2Argument2 = h.get("kind");
            Assertions.assertEquals("poison", assertionValue2Argument2, "H should match the expected value.");
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 3")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck3() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Map<String, String> h;
            final String assertionValue3Argument2;
            final Queue serial;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            h = dead.messages().getFirst().headers();
            assertionValue3Argument2 = h.get("smq-original-queue");
            Assertions.assertEquals("v1-orders", assertionValue3Argument2, "H should match the expected value.");
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 4")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck4() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Map<String, String> h;
            final String assertionValue4Argument2;
            final Queue serial;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            h = dead.messages().getFirst().headers();
            assertionValue4Argument2 = h.get("smq-reason");
            Assertions.assertEquals("rejected", assertionValue4Argument2, "H should match the expected value.");
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 5")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck5() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Queue serial;
            final boolean assertionValue5Argument1;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            evaluate(dead.messages().getFirst().headers());
            serial = smq.queue("v1-serial");
            assertionValue5Argument1 = serial.pull(10, Duration.ZERO).isEmpty();
            Assertions.assertTrue(assertionValue5Argument1, "Pulled chunk should be empty.");
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 6")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck6() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Queue serial;
            final List<String> assertionValue6Argument1;
            final List<String> assertionValue6Argument2;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            evaluate(dead.messages().getFirst().headers());
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            assertionValue6Argument1 = List.of("s1", "s2");
            assertionValue6Argument2 = texts(serial.pull(10, Duration.ZERO));
            Assertions.assertEquals(
                    assertionValue6Argument1, assertionValue6Argument2, "Texts should match the expected value.");
            evaluate(captureThrowable(() -> smq.delete("v1-dead")));
        }
    }

    @Test
    @DisplayName("견본의 dead letter 큐와 단일 활성 컨슈머 큐가 그대로 동작한다 — check 7")
    void theDeadLetterQueueOfTheSampleAndTheSingleActiveConsumerQueueOperateAsTheyAreCheck7() throws Exception {
        final MutableClock clock;
        clock = new MutableClock(T0);

        try (Smq smq = Smq.sqlite(StorageFactory.copyV1Sample(this.dir.resolve("smq.db")))
                .clock(clock)
                .open()) {
            final Chunk dead;
            final Queue serial;
            final Throwable assertionValue7;
            dead = smq.queue("v1-dead").pull(10, Duration.ZERO);
            evaluate(dead.messages().getFirst().headers());
            serial = smq.queue("v1-serial");
            evaluate(serial.pull(10, Duration.ZERO).isEmpty());
            clock.advance(Duration.ofHours(1L));
            evaluate(texts(serial.pull(10, Duration.ZERO)));
            assertionValue7 = captureThrowable(() -> smq.delete("v1-dead"));
            Assertions.assertInstanceOf(
                    QueueInUseException.class, assertionValue7, "Delete should throw QueueInUseException.");
        }
    }

    @Test
    @DisplayName("user version을 높인 복사본을 열면 IncompatibleStorageException이다")
    void openingACopyWithAnIncreasedUserVersionResultsInAnIncompatibleStorageException() throws Exception {
        final Path copy;
        copy = StorageFactory.copyV1Sample(this.dir.resolve("future.db"));

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + copy);
                Statement s = c.createStatement(); ) {
            s.execute("PRAGMA user_version = 99");
        }

        Assertions.assertThrows(
                IncompatibleStorageException.class,
                () -> Smq.sqlite(copy).open(),
                "Open should throw IncompatibleStorageException.");
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }
}
