package io.github.ppzxc.smq.storage.memory;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcome;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueDeclarationConflictException;
import io.github.ppzxc.smq.QueueInUseException;
import io.github.ppzxc.smq.QueueNotFoundException;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.storage.Disposition;
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

        Entry(MessageId id, NewMessage m, Instant publishedAt) {
            this.id = id;
            this.payload = m.payload().clone();
            this.headers = Map.copyOf(m.headers());
            this.publishedAt = publishedAt;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
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
            for (Entry e : require(queue).values()) {
                if (e.leaseToken == null) {
                    ready++;
                } else {
                    leased++;
                }
            }
            return new QueueCounts(ready, leased, 0);
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
            List<Entry> picked = new ArrayList<>();
            for (Entry e : require(queue).values()) {
                if (e.leaseToken == null) {
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
            List<Result> results = new ArrayList<>(items.size());
            for (Disposition d : items) {
                results.add(new Result(d.id(), settleOne(token, d)));
            }
            return results;
        } finally {
            lock.unlock();
        }
    }

    private Outcome settleOne(LeaseToken token, Disposition d) {
        for (TreeMap<Long, Entry> messages : queues.values()) {
            Entry e = messages.get(d.id().value());
            if (e != null) {
                if (e.leaseToken == null || e.leaseToken != token.value()) {
                    return Outcome.STALE;
                }
                messages.remove(d.id().value());
                return Outcome.OK;
            }
        }
        return Outcome.STALE;
    }

    private TreeMap<Long, Entry> require(String queue) {
        TreeMap<Long, Entry> messages = queues.get(queue);
        if (messages == null) {
            throw new QueueNotFoundException(queue);
        }
        return messages;
    }

    @Override
    public void close() {
    }
}
