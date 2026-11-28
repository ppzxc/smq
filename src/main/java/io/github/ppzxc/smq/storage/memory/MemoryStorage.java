package io.github.ppzxc.smq.storage.memory;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.NoDeadLetterQueueException;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueDeclarationConflictException;
import io.github.ppzxc.smq.QueueInUseException;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.ChangeHint;
import io.github.ppzxc.smq.storage.ConditionChangeHint;
import io.github.ppzxc.smq.storage.DeadLetters;
import io.github.ppzxc.smq.storage.Disposition;
import io.github.ppzxc.smq.storage.Extended;
import io.github.ppzxc.smq.storage.LeaseToken;
import io.github.ppzxc.smq.storage.NewMessage;
import io.github.ppzxc.smq.storage.PullResult;
import io.github.ppzxc.smq.storage.Result;
import io.github.ppzxc.smq.storage.Storage;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

public final class MemoryStorage implements Storage {

    private static final class Entry {
        final MessageId id;
        final byte[] payload;
        final Map<String, String> headers;
        final Instant publishedAt;
        int deliveryCount;

        @Nullable
        Long leaseToken;

        @Nullable
        Instant leaseDeadline;

        @Nullable
        Instant availableAt;

        Entry(final MessageId id, final NewMessage m, final Instant publishedAt) {
            this.id = id;
            this.payload = m.payload().clone();
            this.headers = Map.copyOf(m.headers());
            this.publishedAt = publishedAt;
            this.availableAt = m.availableAt(publishedAt);
        }

        Entry deadLettered(
                final MessageId newId, final String originalQueue, final DeadLetters.Reason reason, final Instant now) {
            final DeadLetters.Origin origin;
            origin = new DeadLetters.Origin(originalQueue, id, headers, deliveryCount);
            return new Entry(newId, new NewMessage(payload, DeadLetters.headers(origin, reason, now)), now);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final ConditionChangeHint hint = new ConditionChangeHint();
    private final Map<String, TreeMap<Long, Entry>> queues = new HashMap<>();
    private final Map<String, QueueOptions> optionsByQueue = new LinkedHashMap<>();
    private long nextMessageId = 1;
    private long nextLeaseToken = 1;

    @Override
    public void declare(final String queue, final QueueOptions options) {
        lock.lock();
        try {
            final QueueOptions existing;
            existing = optionsByQueue.get(queue);
            if (existing != null) {
                if (!existing.equals(options)) {
                    throw new QueueDeclarationConflictException(queue, existing, options);
                }
                return;
            }
            options.deadLetterQueue().ifPresent(dlq -> {
                if (!queues.containsKey(dlq)) {
                    throw new QueueNotFoundException(dlq);
                }
            });
            queues.put(queue, new TreeMap<>());
            optionsByQueue.put(queue, options);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean delete(final String queue) {
        lock.lock();
        try {
            if (!queues.containsKey(queue)) {
                return false;
            }
            for (final Map.Entry<String, QueueOptions> e : optionsByQueue.entrySet()) {
                if (e.getValue().deadLetterQueue().filter(queue::equals).isPresent()) {
                    throw new QueueInUseException(queue, e.getKey());
                }
            }
            optionsByQueue.remove(queue);
            queues.remove(queue);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean exists(final String queue) {
        lock.lock();
        try {
            return queues.containsKey(queue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<QueueOptions> options(final String queue) {
        lock.lock();
        try {
            return Optional.ofNullable(optionsByQueue.get(queue));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public QueueCounts counts(final String queue) {
        lock.lock();
        try {

            long ready;
            long leased;
            long delayed;
            ready = 0;
            leased = 0;
            delayed = 0;
            for (final Entry e : require(queue).values()) {
                if (e.leaseToken != null) {
                    leased++;
                } else if (e.availableAt != null) {
                    delayed++;
                } else {
                    ready++;
                }
            }
            return new QueueCounts(ready, leased, delayed);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<MessageId> publish(final String queue, final List<NewMessage> batch, final InstantSource clock) {
        lock.lock();
        try {
            final Instant now;
            final TreeMap<Long, Entry> messages;
            final List<Entry> entries;
            long next;
            final List<MessageId> ids;
            now = clock.instant();
            messages = require(queue);

            entries = new ArrayList<>(batch.size());
            next = nextMessageId;
            for (final NewMessage m : batch) {
                entries.add(new Entry(new MessageId(next), m, now));
                next++;
            }
            nextMessageId = next;
            ids = new ArrayList<>(entries.size());
            for (final Entry e : entries) {
                messages.put(e.id.value(), e);
                ids.add(e.id);
            }
            return ids;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PullResult pull(final String queue, final int max, final Duration lease, final InstantSource clock) {
        lock.lock();
        try {
            final Instant now;
            final TreeMap<Long, Entry> entries;
            final QueueOptions options;
            final List<Entry> picked;
            final List<Entry> exhausted;
            final boolean deadLettersRemain;
            final long token;
            final Instant deadline;
            final List<Message> messages;
            now = clock.instant();
            entries = require(queue);
            options = requireOptions(queue);

            if (options.singleActiveConsumer() && entries.values().stream().anyMatch(e -> isLeased(e, now))) {
                return PullResult.empty();
            }
            picked = new ArrayList<>();
            exhausted = new ArrayList<>();
            deadLettersRemain = Selection.selectEntries(entries, options, now, max, picked, exhausted);
            moveExhausted(entries, options, queue, now, exhausted);
            if (picked.isEmpty()) {
                return deadLettersRemain
                        ? PullResult.emptyDeadLettersRemain(!exhausted.isEmpty())
                        : PullResult.empty(!exhausted.isEmpty());
            }
            token = nextLeaseToken;
            nextLeaseToken++;
            deadline = now.plus(lease);
            messages = new ArrayList<>(picked.size());
            for (final Entry e : picked) {
                e.leaseToken = token;
                e.leaseDeadline = deadline;
                e.deliveryCount++;
                messages.add(new Message(e.id, e.payload, e.headers, e.deliveryCount, e.publishedAt));
            }
            return PullResult.of(new LeaseToken(token), deadline, messages, !exhausted.isEmpty());
        } finally {
            lock.unlock();
        }
    }

    private static final class Selection {
        private static boolean selectEntries(
                final Map<Long, Entry> entries,
                final QueueOptions options,
                final Instant now,
                final int max,
                final List<Entry> picked,
                final List<Entry> exhausted) {
            boolean deadLettersRemain;
            boolean blocked;
            final boolean moves;
            deadLettersRemain = false;
            blocked = false;
            moves = options.maxDeliveries().isPresent();
            clearReadyDelays(entries, now);
            for (final Entry e : entries.values()) {
                if (selectionComplete(picked, max, moves)) {
                    break;
                }
                if (options.singleActiveConsumer() && isDelayed(e, now)) {
                    blocked = true;
                }
                if (!isReady(e, now)) {
                    continue;
                }
                if (isExhausted(e, options, moves)) {
                    deadLettersRemain |= !addExhausted(exhausted, e);
                } else if (!blocked && picked.size() < max) {
                    picked.add(e);
                }
            }
            return deadLettersRemain;
        }

        private static boolean selectionComplete(final List<Entry> picked, final int max, final boolean moves) {
            return picked.size() == max && !moves;
        }

        private static boolean isExhausted(final Entry entry, final QueueOptions options, final boolean moves) {
            return moves && entry.deliveryCount >= options.maxDeliveries().getAsInt();
        }

        private static boolean addExhausted(final List<Entry> exhausted, final Entry entry) {
            if (exhausted.size() >= DeadLetters.MAX_MOVES_PER_PULL) {
                return false;
            }
            exhausted.add(entry);
            return true;
        }

        private static void clearReadyDelays(final Map<Long, Entry> entries, final Instant now) {
            for (final Entry e : entries.values()) {
                if (e.availableAt != null && !e.availableAt.isAfter(now)) {
                    e.availableAt = null;
                }
            }
        }
    }

    private void moveExhausted(
            final Map<Long, Entry> entries,
            final QueueOptions options,
            final String queue,
            final Instant now,
            final List<Entry> exhausted) {
        final TreeMap<Long, Entry> dlq;
        if (exhausted.isEmpty()) {
            return;
        }
        dlq = require(options.deadLetterQueue().orElseThrow());
        for (final Entry e : exhausted) {
            final long id;
            final Entry moved;
            entries.remove(e.id.value());
            id = nextMessageId;
            nextMessageId++;
            moved = e.deadLettered(new MessageId(id), queue, DeadLetters.Reason.MAX_DELIVERIES, now);
            dlq.put(moved.id.value(), moved);
        }
    }

    @Override
    public PullResult pull(
            final String queue, final int max, final Duration lease, final InstantSource clock, final Duration wait)
            throws InterruptedException {
        long nanos;
        try {
            nanos = wait.toNanos();
        } catch (ArithmeticException e) {
            nanos = Long.MAX_VALUE;
        }
        if (!lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
            return PullResult.empty();
        }
        try {
            return pull(queue, max, lease, clock);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Result> settle(final LeaseToken token, final List<Disposition> items, final InstantSource clock) {
        lock.lock();
        try {
            final Instant now;
            final List<@Nullable Instant> availableAt;
            final List<Result> results;
            now = clock.instant();
            requireDeadLetterQueues(token, items);
            availableAt = new ArrayList<>(items.size());
            for (final Disposition d : items) {
                availableAt.add(d.kind() == Disposition.Kind.NACK ? d.availableAt(now) : null);
            }
            results = new ArrayList<>(items.size());
            for (int i = 0; i < items.size(); i++) {
                results.add(new Result(items.get(i).id(), settleOne(token, items.get(i), now, availableAt.get(i))));
            }
            return results;
        } finally {
            lock.unlock();
        }
    }

    private void requireDeadLetterQueues(final LeaseToken token, final List<Disposition> items) {
        for (final Disposition d : items) {
            final Located found;
            if (d.kind() != Disposition.Kind.REJECT) {
                continue;
            }
            found = locate(d.id());
            if (found != null
                    && holds(found.entry, token)
                    && deadLetterQueueOf(found.queue).isEmpty()) {
                throw new NoDeadLetterQueueException(found.queue);
            }
        }
    }

    private Outcome settleOne(
            final LeaseToken token, final Disposition d, final Instant now, final @Nullable Instant availableAt) {
        final Located found;
        found = locate(d.id());
        if (found == null || !holds(found.entry, token)) {
            return Outcome.STALE;
        }
        switch (d.kind()) {
            case ACK -> found.messages.remove(d.id().value());
            case REJECT -> {
                final String dlq;
                final long id;
                final Entry moved;
                found.messages.remove(d.id().value());
                dlq = deadLetterQueueOf(found.queue).orElseThrow();
                id = nextMessageId;
                nextMessageId++;
                moved = found.entry.deadLettered(new MessageId(id), found.queue, DeadLetters.Reason.REJECTED, now);
                require(dlq).put(moved.id.value(), moved);
            }
            case NACK -> {
                found.entry.leaseToken = null;
                found.entry.leaseDeadline = null;
                found.entry.availableAt = availableAt;
            }
        }
        return Outcome.OK;
    }

    private record Located(String queue, TreeMap<Long, Entry> messages, Entry entry) {}

    private @Nullable Located locate(final MessageId id) {
        for (final Map.Entry<String, TreeMap<Long, Entry>> q : queues.entrySet()) {
            final Entry e;
            e = q.getValue().get(id.value());
            if (e != null) {
                return new Located(q.getKey(), q.getValue(), e);
            }
        }
        return null;
    }

    private Optional<String> deadLetterQueueOf(final String queue) {
        return requireOptions(queue).deadLetterQueue();
    }

    private static boolean holds(final Entry e, final LeaseToken token) {
        return e.leaseToken != null && e.leaseToken == token.value();
    }

    @Override
    public Extended extend(
            final LeaseToken token, final List<MessageId> ids, final Duration d, final InstantSource clock) {
        lock.lock();
        try {

            final Instant now;
            final Instant deadline;
            final List<Result> results;
            now = clock.instant();
            deadline = now.plus(d);
            results = new ArrayList<>(ids.size());
            for (final MessageId id : ids) {
                Outcome outcome;
                outcome = Outcome.STALE;
                for (final Map.Entry<String, TreeMap<Long, Entry>> queue : queues.entrySet()) {
                    final Entry e;
                    e = queue.getValue().get(id.value());
                    if (e != null) {

                        final boolean expiredInSingleActiveQueue;
                        expiredInSingleActiveQueue = !isLeased(e, now)
                                && requireOptions(queue.getKey()).singleActiveConsumer();
                        if (holds(e, token) && !expiredInSingleActiveQueue) {
                            e.leaseDeadline = deadline;
                            outcome = Outcome.OK;
                        }
                        break;
                    }
                }
                results.add(new Result(id, outcome));
            }
            return new Extended(deadline, results);
        } finally {
            lock.unlock();
        }
    }

    private static boolean isReady(final Entry e, final Instant now) {
        return !isDelayed(e, now) && (e.leaseToken == null || e.leaseDeadline == null || !e.leaseDeadline.isAfter(now));
    }

    private static boolean isLeased(final Entry e, final Instant now) {
        return e.leaseToken != null && e.leaseDeadline != null && e.leaseDeadline.isAfter(now);
    }

    private static boolean isDelayed(final Entry e, final Instant now) {
        return e.availableAt != null && e.availableAt.isAfter(now);
    }

    @SuppressWarnings("NonApiType") // TreeMap keeps messages in FIFO order for pulls.
    private TreeMap<Long, Entry> require(final String queue) {
        final TreeMap<Long, Entry> messages;
        messages = queues.get(queue);
        if (messages == null) {
            throw new QueueNotFoundException(queue);
        }
        return messages;
    }

    private QueueOptions requireOptions(final String queue) {
        final QueueOptions options;
        options = optionsByQueue.get(queue);
        if (options == null) {
            throw new QueueNotFoundException(queue);
        }
        return options;
    }

    @Override
    public void snapshot(Path target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ChangeHint> changeHint() {
        return Optional.of(hint);
    }

    @Override
    public void close() {}
}
