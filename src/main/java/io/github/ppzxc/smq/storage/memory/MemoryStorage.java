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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/** 같은 프로세스 안에서만 쓰는 인메모리 저장소. 다중 프로세스는 지원하지 않는다. */
public final class MemoryStorage implements Storage {

    private static final class Entry {
        final MessageId id;
        final byte[] payload;
        final Map<String, String> headers;
        final Instant publishedAt;
        int deliveryCount;
        Long leaseToken;
        Instant leaseDeadline;
        Instant availableAt;

        Entry(MessageId id, NewMessage m, Instant publishedAt) {
            this.id = id;
            this.payload = m.payload().clone();
            this.headers = Map.copyOf(m.headers());
            this.publishedAt = publishedAt;
            this.availableAt = m.availableAt(publishedAt);
        }

        /** dead letter 큐에 새 메시지로 들어갈 항목. id와 전달 횟수는 새로 시작한다. */
        Entry deadLettered(MessageId newId, String originalQueue, DeadLetters.Reason reason, Instant now) {
            DeadLetters.Origin origin = new DeadLetters.Origin(originalQueue, id, headers, deliveryCount);
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
    public void declare(String queue, QueueOptions options) {
        lock.lock();
        try {
            QueueOptions existing = optionsByQueue.get(queue);
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
    public boolean delete(String queue) {
        lock.lock();
        try {
            if (!queues.containsKey(queue)) {
                return false;
            }
            for (Map.Entry<String, QueueOptions> e : optionsByQueue.entrySet()) {
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
    public boolean exists(String queue) {
        lock.lock();
        try {
            return queues.containsKey(queue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<QueueOptions> options(String queue) {
        lock.lock();
        try {
            return Optional.ofNullable(optionsByQueue.get(queue));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public QueueCounts counts(String queue, Instant now) {
        lock.lock();
        try {
            long ready = 0;
            long leased = 0;
            long delayed = 0;
            for (Entry e : require(queue).values()) {
                if (isDelayed(e, now)) {
                    delayed++;
                } else if (isReady(e, now)) {
                    ready++;
                } else {
                    leased++;
                }
            }
            return new QueueCounts(ready, leased, delayed);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<MessageId> publish(String queue, List<NewMessage> batch, Instant now) {
        lock.lock();
        try {
            TreeMap<Long, Entry> messages = require(queue);
            // 항목을 모두 만든 뒤에 반영해서, 중간에 실패해도 일부만 들어가지 않게 한다.
            List<Entry> entries = new ArrayList<>(batch.size());
            long next = nextMessageId;
            for (NewMessage m : batch) {
                entries.add(new Entry(new MessageId(next++), m, now));
            }
            nextMessageId = next;
            List<MessageId> ids = new ArrayList<>(entries.size());
            for (Entry e : entries) {
                messages.put(e.id.value(), e);
                ids.add(e.id);
            }
            return ids;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PullResult pull(String queue, int max, Duration lease, Instant now) {
        lock.lock();
        try {
            TreeMap<Long, Entry> queued = require(queue);
            if (optionsByQueue.get(queue).singleActiveConsumer()
                    && queued.values().stream().anyMatch(e -> isLeased(e, now))) {
                return PullResult.empty();
            }
            List<Entry> picked = new ArrayList<>();
            for (Entry e : queued.values()) {
                if (isReady(e, now)) {
                    picked.add(e);
                    if (picked.size() == max) {
                        break;
                    }
                }
            }
            if (picked.isEmpty()) {
                return PullResult.empty();
            }
            long token = nextLeaseToken++;
            Instant deadline = now.plus(lease);
            List<Message> messages = new ArrayList<>(picked.size());
            for (Entry e : picked) {
                e.leaseToken = token;
                e.leaseDeadline = deadline;
                e.deliveryCount++;
                messages.add(new Message(e.id, e.payload, e.headers, e.deliveryCount, e.publishedAt));
            }
            return new PullResult(new LeaseToken(token), deadline, messages);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Result> settle(LeaseToken token, List<Disposition> items, Instant now) {
        lock.lock();
        try {
            requireDeadLetterQueues(token, items);
            List<Result> results = new ArrayList<>(items.size());
            for (Disposition d : items) {
                results.add(new Result(d.id(), settleOne(token, d, now)));
            }
            return results;
        } finally {
            lock.unlock();
        }
    }

    /** 아무것도 바꾸기 전에 확인해서, 실패하면 정산 전체가 반영되지 않게 한다. */
    private void requireDeadLetterQueues(LeaseToken token, List<Disposition> items) {
        for (Disposition d : items) {
            if (d.kind() != Disposition.Kind.REJECT) {
                continue;
            }
            Located found = locate(d.id());
            if (found != null && holds(found.entry, token) && deadLetterQueueOf(found.queue).isEmpty()) {
                throw new NoDeadLetterQueueException(found.queue);
            }
        }
    }

    private Outcome settleOne(LeaseToken token, Disposition d, Instant now) {
        Located found = locate(d.id());
        if (found == null || !holds(found.entry, token)) {
            return Outcome.STALE;
        }
        switch (d.kind()) {
            case ACK -> found.messages.remove(d.id().value());
            case REJECT -> {
                found.messages.remove(d.id().value());
                String dlq = deadLetterQueueOf(found.queue).orElseThrow();
                Entry moved = found.entry.deadLettered(new MessageId(nextMessageId++), found.queue,
                        DeadLetters.Reason.REJECTED, now);
                queues.get(dlq).put(moved.id.value(), moved);
            }
            case NACK -> {
                found.entry.leaseToken = null;
                found.entry.leaseDeadline = null;
                found.entry.availableAt = d.availableAt(now);
            }
        }
        return Outcome.OK;
    }

    private record Located(String queue, TreeMap<Long, Entry> messages, Entry entry) {
    }

    /** id로 메시지가 있는 큐를 찾는다. 없으면 null이다. */
    private Located locate(MessageId id) {
        for (Map.Entry<String, TreeMap<Long, Entry>> q : queues.entrySet()) {
            Entry e = q.getValue().get(id.value());
            if (e != null) {
                return new Located(q.getKey(), q.getValue(), e);
            }
        }
        return null;
    }

    private Optional<String> deadLetterQueueOf(String queue) {
        return optionsByQueue.get(queue).deadLetterQueue();
    }

    /** 판정 기준은 리스 토큰 일치 하나뿐이다. 기한이 지났는지는 보지 않는다. */
    private static boolean holds(Entry e, LeaseToken token) {
        return e.leaseToken != null && e.leaseToken == token.value();
    }

    @Override
    public Extended extend(LeaseToken token, List<MessageId> ids, Duration d, Instant now) {
        lock.lock();
        try {
            Instant deadline = now.plus(d);
            List<Result> results = new ArrayList<>(ids.size());
            for (MessageId id : ids) {
                Outcome outcome = Outcome.STALE;
                for (TreeMap<Long, Entry> messages : queues.values()) {
                    Entry e = messages.get(id.value());
                    if (e != null) {
                        if (holds(e, token)) {
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

    /** 리스가 없거나 기한이 지났으면 준비 상태다. 기한이 지나도 다시 꺼내기 전까지는 리스 토큰이 남아 있다. */
    private static boolean isReady(Entry e, Instant now) {
        return !isDelayed(e, now) && (e.leaseToken == null || !e.leaseDeadline.isAfter(now));
    }

    private static boolean isLeased(Entry e, Instant now) {
        return e.leaseToken != null && e.leaseDeadline.isAfter(now);
    }

    private static boolean isDelayed(Entry e, Instant now) {
        return e.availableAt != null && e.availableAt.isAfter(now);
    }

    private TreeMap<Long, Entry> require(String queue) {
        TreeMap<Long, Entry> messages = queues.get(queue);
        if (messages == null) {
            throw new QueueNotFoundException(queue);
        }
        return messages;
    }

    @Override
    public Optional<ChangeHint> changeHint() {
        return Optional.of(hint);
    }

    @Override
    public void close() {
        // 해제할 자원이 없다. 기다리는 꺼내기는 코어가 닫으면서 깨운다.
    }
}
