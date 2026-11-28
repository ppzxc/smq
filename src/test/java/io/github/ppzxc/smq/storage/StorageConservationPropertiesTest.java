package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.memory.MemoryStorage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

class StorageConservationPropertiesTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private static final Duration LEASE = Duration.ofSeconds(30L);

    @Test
    void emptyQueueHasNoMessages() {
        try (Storage storage = new MemoryStorage()) {
            storage.declare("q", QueueOptions.defaults());
            assertEquals(new QueueCounts(0L, 0L, 0L), storage.counts("q"), "A new queue must be empty.");
        }
    }

    @Property(tries = 20)
    void everySmallBatchAndPullLimitCombinationIsChecked(
            @ForAll @IntRange(min = 1, max = 4) final int batchSize,
            @ForAll @IntRange(min = 1, max = 5) final int pullLimit) {
        final PullResult pulled;
        try (Storage storage = new MemoryStorage()) {
            storage.declare("q", QueueOptions.defaults());
            storage.publish(
                    "q",
                    java.util.Collections.nCopies(batchSize, new NewMessage(new byte[] {1}, Map.of())),
                    InstantSource.fixed(START));
            pulled = storage.pull("q", pullLimit, LEASE, InstantSource.fixed(START));
            assertEquals(Math.min(batchSize, pullLimit), pulled.messages().size(), "Pull limit must bound each chunk.");
            assertEquals(
                    new QueueCounts(
                            batchSize - pulled.messages().size(),
                            pulled.messages().size(),
                            0L),
                    storage.counts("q"),
                    "Every batch and pull limit combination must preserve queue counts.");
        }
    }

    @Property(tries = 200)
    void commandSequencesPreserveMemoryQueueState(@ForAll("commands") final List<Command> commands) throws Exception {
        executeCommands(StorageFactory.MEMORY, Path.of("."), commands);
    }

    @Property(tries = 50)
    void commandSequencesPreserveSqliteQueueState(@ForAll("commands") final List<Command> commands) throws Exception {
        final Path directory;
        directory = Files.createTempDirectory("smq-jqwik-");
        try {
            executeCommands(
                    StorageFactory.sqliteNew(io.github.ppzxc.smq.Durability.PROCESS_CRASH), directory, commands);
        } finally {
            deleteDirectoryContents(directory);
        }
    }

    private static void executeCommands(
            final StorageFactory factory, final Path directory, final List<Command> commands) throws Exception {
        final List<MessageId> published;
        final Map<MessageId, Instant> leased;
        int acknowledged;
        Instant now;
        PullResult lastPull;
        published = new ArrayList<>();
        leased = new HashMap<>();
        acknowledged = 0;
        now = START;
        lastPull = PullResult.empty();
        try (Storage storage = factory.open(directory)) {
            storage.declare("q", QueueOptions.defaults());
            for (final Command command : commands) {
                expireLeases(leased, now);
                switch (command.kind()) {
                    case PUBLISH, PUBLISH_DELAYED -> published.addAll(publish(storage, command, now));
                    case PULL -> lastPull = pull(storage, command, now, published, leased);
                    case ACK, NACK -> acknowledged += settle(storage, command, lastPull, now, leased);
                    case ADVANCE_CLOCK -> now = now.plusSeconds(command.amount());
                }
                assertCountsConserveMessages(storage.counts("q"), published.size() - acknowledged);
            }
        }
    }

    @Provide
    Arbitrary<List<Command>> commands() {
        final Arbitrary<Command> publish;
        final Arbitrary<Command> publishDelayed;
        final Arbitrary<Command> pull;
        final Arbitrary<Command> advanceClock;
        publish = Arbitraries.integers().between(1, 4).map(size -> new Command(Kind.PUBLISH, size));
        publishDelayed = Arbitraries.integers().between(1, 4).map(size -> new Command(Kind.PUBLISH_DELAYED, size));
        pull = Arbitraries.integers().between(1, 5).map(limit -> new Command(Kind.PULL, limit));
        advanceClock = Arbitraries.integers().between(0, 60).map(seconds -> new Command(Kind.ADVANCE_CLOCK, seconds));
        return Arbitraries.oneOf(
                        publish,
                        publishDelayed,
                        pull,
                        advanceClock,
                        Arbitraries.just(new Command(Kind.ACK, 0)),
                        Arbitraries.just(new Command(Kind.NACK, 0)))
                .list()
                .ofMinSize(1)
                .ofMaxSize(100);
    }

    private static List<MessageId> publish(final Storage storage, final Command command, final Instant now) {
        final Duration delay;
        delay = command.kind() == Kind.PUBLISH_DELAYED ? Duration.ofSeconds(1L) : Duration.ZERO;
        return storage.publish(
                "q",
                java.util.Collections.nCopies(command.amount(), new NewMessage(new byte[] {1}, Map.of(), delay)),
                InstantSource.fixed(now));
    }

    private static PullResult pull(
            final Storage storage,
            final Command command,
            final Instant now,
            final List<MessageId> published,
            final Map<MessageId, Instant> leased) {
        final PullResult result;
        result = storage.pull("q", command.amount(), LEASE, InstantSource.fixed(now));
        for (final Message message : result.messages()) {
            assertTrue(published.contains(message.id()), "A pull must return a previously published message.");
            assertNull(
                    leased.putIfAbsent(message.id(), Objects.requireNonNull(result.leaseDeadline())),
                    "A message must not be leased twice before expiry.");
        }
        return result;
    }

    private static int settle(
            final Storage storage,
            final Command command,
            final PullResult lastPull,
            final Instant now,
            final Map<MessageId, Instant> leased) {
        final Disposition.Kind dispositionKind;
        final List<Disposition> dispositions;
        final List<Result> results;
        int acknowledged;
        if (lastPull.token() == null) {
            return 0;
        }
        dispositionKind = command.kind() == Kind.ACK ? Disposition.Kind.ACK : Disposition.Kind.NACK;
        dispositions = lastPull.messages().stream()
                .map(message -> new Disposition(message.id(), dispositionKind))
                .toList();
        results = storage.settle(lastPull.token(), dispositions, InstantSource.fixed(now));
        acknowledged = 0;
        for (final Result result : results) {
            if (result.outcome() != Outcome.OK) {
                continue;
            }
            leased.remove(result.id());
            if (dispositionKind == Disposition.Kind.ACK) {
                acknowledged++;
            }
        }
        return acknowledged;
    }

    private static void expireLeases(final Map<MessageId, Instant> leased, final Instant now) {
        leased.entrySet().removeIf(entry -> !entry.getValue().isAfter(now));
    }

    private static void deleteDirectoryContents(final Path directory) {
        final List<Path> contents;
        try (var paths = Files.list(directory)) {
            contents = paths.toList();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
        try {
            for (final Path path : contents) {
                Files.delete(path);
            }
            Files.delete(directory);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private static void assertCountsConserveMessages(final QueueCounts counts, final int remaining) {
        assertEquals(
                remaining,
                counts.ready() + counts.leased() + counts.delayed(),
                "Every unacknowledged message must remain in exactly one queue state.");
    }

    private enum Kind {
        PUBLISH,
        PUBLISH_DELAYED,
        PULL,
        ACK,
        NACK,
        ADVANCE_CLOCK
    }

    private record Command(Kind kind, int amount) {}
}
