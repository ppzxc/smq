package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BuilderSmokeTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(7);

    @TempDir
    Path dir;

    private enum Backend {
        MEMORY,
        SQLITE
    }

    private record RoundTripResult(
            MessageId publishedId,
            MessageId receivedId,
            Instant publishedAt,
            Instant leaseDeadline,
            boolean settled,
            QueueCounts counts,
            boolean emptyBeforeExpiry,
            int redeliveryCount) {}

    private RoundTripResult roundTrip(final Backend backend) throws Exception {
        final MutableClock clock;
        final Smq smq;
        clock = new MutableClock(T0);
        smq = switch (backend) {
            case MEMORY -> Smq.inMemory().clock(clock).leaseDuration(LEASE).open();
            case SQLITE ->
                Smq.sqlite(dir.resolve("smq.db"))
                        .clock(clock)
                        .leaseDuration(LEASE)
                        .open();
        };
        try (smq) {
            final Queue queue;
            final MessageId publishedId;
            final Chunk chunk;
            final Message message;
            final boolean settled;
            final QueueCounts counts;
            final boolean emptyBeforeExpiry;
            final int redeliveryCount;
            queue = smq.declare("q", QueueOptions.defaults());
            publishedId = queue.publish(OutgoingMessage.of("a".getBytes(StandardCharsets.UTF_8)));
            chunk = queue.pull(1, Duration.ZERO);
            message = chunk.messages().getFirst();
            settled = queue.settle(chunk.ackAll()).allOk();
            counts = queue.counts();
            queue.publish(OutgoingMessage.of("b".getBytes(StandardCharsets.UTF_8)));
            queue.pull(1, Duration.ZERO);
            clock.advance(LEASE.minusNanos(1));
            emptyBeforeExpiry = queue.pull(1, Duration.ZERO).isEmpty();
            clock.advance(Duration.ofNanos(1));
            redeliveryCount = queue.pull(1, Duration.ZERO).messages().getFirst().deliveryCount();
            return new RoundTripResult(
                    publishedId,
                    message.id(),
                    message.publishedAt(),
                    chunk.leaseDeadline(),
                    settled,
                    counts,
                    emptyBeforeExpiry,
                    redeliveryCount);
        }
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderPreservesMessageId(final Backend backend) throws Exception {
        final RoundTripResult result;
        result = roundTrip(backend);
        assertEquals(result.publishedId(), result.receivedId(), "The published message ID should be preserved.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderAppliesClock(final Backend backend) throws Exception {
        assertEquals(T0, roundTrip(backend).publishedAt(), "The configured clock should set the publication time.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderAppliesLeaseDuration(final Backend backend) throws Exception {
        assertEquals(
                T0.plus(LEASE), roundTrip(backend).leaseDeadline(), "The configured lease should set the deadline.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderSupportsSettlement(final Backend backend) throws Exception {
        assertTrue(roundTrip(backend).settled(), "Settlement should succeed.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderRemovesSettledMessages(final Backend backend) throws Exception {
        assertEquals(new QueueCounts(0, 0, 0), roundTrip(backend).counts(), "Settlement should clear the queue.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderDoesNotRedeliverBeforeExpiry(final Backend backend) throws Exception {
        assertTrue(roundTrip(backend).emptyBeforeExpiry(), "The message should remain leased until the deadline.");
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    void builderRedeliversAtExpiry(final Backend backend) throws Exception {
        assertEquals(2, roundTrip(backend).redeliveryCount(), "The message should be redelivered at expiry.");
    }

    @Test
    @DisplayName("메모리 builder는 양수가 아닌 리스 기간을 거부한다")
    void memoryBuilderRejectsNonPositiveLeaseDurations() {
        for (final Duration bad : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1), null}) {
            assertThrows(IllegalArgumentException.class, () -> Smq.inMemory().leaseDuration(bad), "inMemory " + bad);
        }
    }

    @Test
    @DisplayName("SQLite builder는 양수가 아닌 리스 기간을 거부한다")
    void sqliteBuilderRejectsNonPositiveLeaseDurations() {
        for (final Duration bad : new Duration[] {Duration.ZERO, Duration.ofSeconds(-1), null}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Smq.sqlite(dir.resolve("smq.db")).leaseDuration(bad),
                    "sqlite " + bad);
        }
    }
}
