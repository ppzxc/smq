package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.PolicyExceptions;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Disposition.Kind;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class SqliteStorageCountersTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private static final Duration LEASE = Duration.ofSeconds(10L);

    @TempDir
    Path dir;

    private static NewMessage message(final int i, final Duration delay) {
        return new NewMessage(new byte[] {(byte) i}, Map.of(), delay);
    }

    private static boolean matchesStored(final SqliteStorage s, final String queue) {
        return ExactCounts.stored(s, queue).equals(s.counts(queue));
    }

    @Test
    void pullPromotesAllDueDelaysEvenWhenTheChunkIsSmaller() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Duration delay;
            final PullResult result;
            s.declare("q", QueueOptions.defaults());
            delay = Duration.ofSeconds(5L);
            s.publish("q", List.of(message(1, delay), message(2, delay), message(3, LEASE)), InstantSource.fixed(T0));
            final var assertionValue5Argument1 = new QueueCounts(0L, 0L, 3L);
            final var assertionValue5Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
            result = s.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(delay)));
            final var assertionValue4Argument2 = result.messages().size();
            evaluatePmdArgument(assertionValue4Argument2);
            final var assertionValue3Argument2 = result.messages().getFirst().payload()[0];
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 1L, 1L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = matchesStored(s, "q");
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    void pullPromotesAllDueDelaysEvenWhenTheChunkIsSmallerCheck2() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Duration delay;
            final PullResult result;
            s.declare("q", QueueOptions.defaults());
            delay = Duration.ofSeconds(5L);
            s.publish("q", List.of(message(1, delay), message(2, delay), message(3, LEASE)), InstantSource.fixed(T0));
            final var assertionValue5Argument1 = new QueueCounts(0L, 0L, 3L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue5Argument2);
            result = s.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(delay)));
            final var assertionValue4Argument2 = result.messages().size();
            Assertions.assertEquals(1, assertionValue4Argument2, "Size should match the expected value.");
            final var assertionValue3Argument2 = result.messages().getFirst().payload()[0];
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 1L, 1L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = matchesStored(s, "q");
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    void pullPromotesAllDueDelaysEvenWhenTheChunkIsSmallerCheck3() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Duration delay;
            final PullResult result;
            s.declare("q", QueueOptions.defaults());
            delay = Duration.ofSeconds(5L);
            s.publish("q", List.of(message(1, delay), message(2, delay), message(3, LEASE)), InstantSource.fixed(T0));
            final var assertionValue5Argument1 = new QueueCounts(0L, 0L, 3L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue5Argument2);
            result = s.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(delay)));
            final var assertionValue4Argument2 = result.messages().size();
            evaluatePmdArgument(assertionValue4Argument2);
            final var assertionValue3Argument2 = result.messages().getFirst().payload()[0];
            Assertions.assertEquals(1, assertionValue3Argument2, "Payload should match the expected value.");
            final var assertionValue2Argument1 = new QueueCounts(1L, 1L, 1L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = matchesStored(s, "q");
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    void pullPromotesAllDueDelaysEvenWhenTheChunkIsSmallerCheck4() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Duration delay;
            final PullResult result;
            s.declare("q", QueueOptions.defaults());
            delay = Duration.ofSeconds(5L);
            s.publish("q", List.of(message(1, delay), message(2, delay), message(3, LEASE)), InstantSource.fixed(T0));
            final var assertionValue5Argument1 = new QueueCounts(0L, 0L, 3L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue5Argument2);
            result = s.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(delay)));
            final var assertionValue4Argument2 = result.messages().size();
            evaluatePmdArgument(assertionValue4Argument2);
            final var assertionValue3Argument2 = result.messages().getFirst().payload()[0];
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 1L, 1L);
            final var assertionValue2Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
            final var assertionValue1Argument1 = matchesStored(s, "q");
            evaluatePmdArgument(assertionValue1Argument1);
        }
    }

    @Test
    void pullPromotesAllDueDelaysEvenWhenTheChunkIsSmallerCheck5() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Duration delay;
            final PullResult result;
            s.declare("q", QueueOptions.defaults());
            delay = Duration.ofSeconds(5L);
            s.publish("q", List.of(message(1, delay), message(2, delay), message(3, LEASE)), InstantSource.fixed(T0));
            final var assertionValue5Argument1 = new QueueCounts(0L, 0L, 3L);
            evaluatePmdArgument(assertionValue5Argument1);
            final var assertionValue5Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue5Argument2);
            result = s.pull("q", 1, LEASE, InstantSource.fixed(T0.plus(delay)));
            final var assertionValue4Argument2 = result.messages().size();
            evaluatePmdArgument(assertionValue4Argument2);
            final var assertionValue3Argument2 = result.messages().getFirst().payload()[0];
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = new QueueCounts(1L, 1L, 1L);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = s.counts("q");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = matchesStored(s, "q");
            Assertions.assertTrue(assertionValue1Argument1, "Stored counts should match for q.");
        }
    }

    @Test
    @DisplayName("무작위 연산 뒤에도 메모리 개수가 저장된 상태 집계와 같고 다시 열어도 같다")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.CognitiveComplexity",
                reason =
                        "The seeded operation sequence checks stored counts after every operation and after reopening.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.CyclomaticComplexity",
                reason = "The seeded operation sequence covers distinct queue operations and verifies each step.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
    void inMemoryCountsMatchStoredCountsAfterRandomOperationsAndReopening() {
        final Path db;
        final Random random;
        final List<QueueCounts> beforeClose;
        final List<String> failures;
        db = this.dir.resolve("smq.db");
        random = new Random(20_261_106L);
        beforeClose = new ArrayList<>();
        failures = new ArrayList<>();
        try (SqliteStorage s = SqliteStorage.open(db, Durability.PROCESS_CRASH)) {
            Instant now;
            final List<PullResult> held;
            s.declare("dead", QueueOptions.defaults());
            s.declare("q", QueueOptions.defaults().maxDeliveries(2, "dead"));
            now = T0;
            held = new ArrayList<>();
            for (int step = 0; step < 400; step++) {
                final List<NewMessage> batch;
                final PullResult pulled;
                switch (random.nextInt(6)) {
                    case 0:
                        batch = new ArrayList<>();
                        for (int i = 0; i <= random.nextInt(4); i++) {
                            batch.add(message(i, random.nextInt(3) == 0 ? Duration.ofSeconds(5L) : Duration.ZERO));
                        }
                        s.publish("q", batch, InstantSource.fixed(now));
                        break;
                    case 1:
                        now = now.plusSeconds(random.nextInt(13));
                        pulled = s.pull("q", 1 + random.nextInt(4), LEASE, InstantSource.fixed(now));
                        if (!pulled.messages().isEmpty()) {
                            held.add(pulled);
                        }
                        break;
                    case 2:
                    case 3:
                        if (!held.isEmpty()) {
                            final PullResult settled;
                            final List<Disposition> items;
                            settled = held.remove(random.nextInt(held.size()));
                            items = new ArrayList<>();
                            settled.messages().forEach(m -> {
                                items.add(
                                        switch (random.nextInt(4)) {
                                            case 0 -> new Disposition(m.id(), Kind.ACK);
                                            case 1 -> new Disposition(m.id(), Kind.NACK);
                                            case 2 -> new Disposition(m.id(), Kind.NACK, Duration.ofSeconds(7L));
                                            default -> new Disposition(m.id(), Kind.REJECT);
                                        });
                            });
                            s.settle(settled.token(), items, InstantSource.fixed(now));
                        }
                        break;
                    case 4:
                        if (!held.isEmpty()) {
                            final PullResult current;
                            final Instant at;
                            current = held.get(random.nextInt(held.size()));
                            at = now;
                            s.extend(
                                    current.token(),
                                    current.messages().stream().map(m -> m.id()).toList(),
                                    LEASE,
                                    InstantSource.fixed(at));
                        }
                        break;
                    default:
                        now = now.plusSeconds(1 + random.nextInt(12));
                }
                if (!matchesStored(s, "q")) {
                    failures.add("step " + step + ": stored counts differ for q");
                }
                if (!matchesStored(s, "dead")) {
                    failures.add("step " + step + ": stored counts differ for dead");
                }
            }
            beforeClose.add(s.counts("q"));
            beforeClose.add(s.counts("dead"));
        }
        try (SqliteStorage s = SqliteStorage.open(db, Durability.PROCESS_CRASH)) {
            if (!beforeClose.get(0).equals(s.counts("q"))) {
                failures.add("reopen: in-memory counts differ for q");
            }
            if (!beforeClose.get(1).equals(s.counts("dead"))) {
                failures.add("reopen: in-memory counts differ for dead");
            }
            if (!matchesStored(s, "q")) {
                failures.add("reopen: stored counts differ for q");
            }
            if (!matchesStored(s, "dead")) {
                failures.add("reopen: stored counts differ for dead");
            }
        }
        Assertions.assertTrue(failures.isEmpty(), String.join("; ", failures));
    }

    @Test
    @DisplayName("시각이 흐르지 않으면 시각 기준 집계와 같고 흐르면 저장된 상태를 따른다 — check 1")
    void ifTimeDoesNotElapseItIsTheSameAsTheTimeBasedAggregationAndIfTimeElapsesItFollowsTheSavedStateCheck1() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final QueueCounts assertionValue1Argument1;
            final QueueCounts assertionValue1Argument2;
            final Instant later;
            s.declare("q", QueueOptions.defaults());
            s.publish(
                    "q",
                    List.of(message(1, Duration.ZERO), message(2, Duration.ZERO), message(3, Duration.ofSeconds(5L))),
                    InstantSource.fixed(T0));
            s.pull("q", 1, LEASE, InstantSource.fixed(T0));
            assertionValue1Argument1 = ExactCounts.at(s, "q", T0);
            assertionValue1Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Counts should match the expected value.");
            later = T0.plusSeconds(60L);
            evaluate(new QueueCounts(3L, 0L, 0L));
            evaluate(ExactCounts.at(s, "q", later));
            evaluate(new QueueCounts(1L, 1L, 1L));
        }
    }

    @Test
    @DisplayName("시각이 흐르지 않으면 시각 기준 집계와 같고 흐르면 저장된 상태를 따른다 — check 2")
    void ifTimeDoesNotElapseItIsTheSameAsTheTimeBasedAggregationAndIfTimeElapsesItFollowsTheSavedStateCheck2() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Instant later;
            final QueueCounts assertionValue2Argument1;
            final QueueCounts assertionValue2Argument2;
            s.declare("q", QueueOptions.defaults());
            s.publish(
                    "q",
                    List.of(message(1, Duration.ZERO), message(2, Duration.ZERO), message(3, Duration.ofSeconds(5L))),
                    InstantSource.fixed(T0));
            s.pull("q", 1, LEASE, InstantSource.fixed(T0));
            evaluate(ExactCounts.at(s, "q", T0));
            later = T0.plusSeconds(60L);
            assertionValue2Argument1 = new QueueCounts(3L, 0L, 0L);
            assertionValue2Argument2 = ExactCounts.at(s, "q", later);
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "At should match the expected value.");
            evaluate(new QueueCounts(1L, 1L, 1L));
        }
    }

    @Test
    @DisplayName("시각이 흐르지 않으면 시각 기준 집계와 같고 흐르면 저장된 상태를 따른다 — check 3")
    void ifTimeDoesNotElapseItIsTheSameAsTheTimeBasedAggregationAndIfTimeElapsesItFollowsTheSavedStateCheck3() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Instant later;
            final QueueCounts assertionValue3Argument1;
            final QueueCounts assertionValue3Argument2;
            s.declare("q", QueueOptions.defaults());
            s.publish(
                    "q",
                    List.of(message(1, Duration.ZERO), message(2, Duration.ZERO), message(3, Duration.ofSeconds(5L))),
                    InstantSource.fixed(T0));
            s.pull("q", 1, LEASE, InstantSource.fixed(T0));
            evaluate(ExactCounts.at(s, "q", T0));
            later = T0.plusSeconds(60L);
            evaluate(new QueueCounts(3L, 0L, 0L));
            evaluate(ExactCounts.at(s, "q", later));
            assertionValue3Argument1 = new QueueCounts(1L, 1L, 1L);
            assertionValue3Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Counts should match the expected value.");
        }
    }

    @Test
    @DisplayName("없는 큐는 QueueNotFoundException이고 지운 큐도 그렇다 — check 1")
    void aNonExistentQueueIsAQueueNotFoundExceptionAndSoIsADeletedQueueCheck1() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Throwable assertionValue1;
            assertionValue1 = captureThrowable(() -> s.counts("none"));
            Assertions.assertInstanceOf(
                    QueueNotFoundException.class, assertionValue1, "Counts should throw QueueNotFoundException.");
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
            evaluate(s.delete("q"));
            evaluate(captureThrowable(() -> s.counts("q")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("없는 큐는 QueueNotFoundException이고 지운 큐도 그렇다 — check 2")
    void aNonExistentQueueIsAQueueNotFoundExceptionAndSoIsADeletedQueueCheck2() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final QueueCounts assertionValue2Argument1;
            final QueueCounts assertionValue2Argument2;
            evaluate(captureThrowable(() -> s.counts("none")));
            s.declare("q", QueueOptions.defaults());
            assertionValue2Argument1 = new QueueCounts(0L, 0L, 0L);
            assertionValue2Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue2Argument1, assertionValue2Argument2, "Counts should match the expected value.");
            evaluate(s.delete("q"));
            evaluate(captureThrowable(() -> s.counts("q")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("없는 큐는 QueueNotFoundException이고 지운 큐도 그렇다 — check 3")
    void aNonExistentQueueIsAQueueNotFoundExceptionAndSoIsADeletedQueueCheck3() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final boolean assertionValue3Argument1;
            evaluate(captureThrowable(() -> s.counts("none")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
            assertionValue3Argument1 = s.delete("q");
            Assertions.assertTrue(assertionValue3Argument1, "The existing queue should be deleted.");
            evaluate(captureThrowable(() -> s.counts("q")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("없는 큐는 QueueNotFoundException이고 지운 큐도 그렇다 — check 4")
    void aNonExistentQueueIsAQueueNotFoundExceptionAndSoIsADeletedQueueCheck4() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final Throwable assertionValue4;
            evaluate(captureThrowable(() -> s.counts("none")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
            evaluate(s.delete("q"));
            assertionValue4 = captureThrowable(() -> s.counts("q"));
            Assertions.assertInstanceOf(
                    QueueNotFoundException.class, assertionValue4, "Counts should throw QueueNotFoundException.");
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
        }
    }

    @Test
    @DisplayName("없는 큐는 QueueNotFoundException이고 지운 큐도 그렇다 — check 5")
    void aNonExistentQueueIsAQueueNotFoundExceptionAndSoIsADeletedQueueCheck5() {
        try (SqliteStorage s = SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH)) {
            final QueueCounts assertionValue5Argument1;
            final QueueCounts assertionValue5Argument2;
            evaluate(captureThrowable(() -> s.counts("none")));
            s.declare("q", QueueOptions.defaults());
            evaluate(new QueueCounts(0L, 0L, 0L));
            evaluate(s.delete("q"));
            evaluate(captureThrowable(() -> s.counts("q")));
            s.declare("q", QueueOptions.defaults());
            assertionValue5Argument1 = new QueueCounts(0L, 0L, 0L);
            assertionValue5Argument2 = s.counts("q");
            Assertions.assertEquals(
                    assertionValue5Argument1, assertionValue5Argument2, "Counts should match the expected value.");
        }
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
