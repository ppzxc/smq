package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.QueueCounts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

final class QueueCounters {

    private static final class Slot {
        volatile QueueCounts value;

        Slot(QueueCounts value) {
            this.value = value;
        }
    }

    private final ConcurrentHashMap<String, Slot> byName = new ConcurrentHashMap<>();

    private final Map<Long, Slot> byId = new HashMap<>();

    void register(String name, long queueId, QueueCounts initial) {
        Slot slot = new Slot(initial);
        byName.put(name, slot);
        byId.put(queueId, slot);
    }

    private void unregister(String name, long queueId) {
        byName.remove(name);
        byId.remove(queueId);
    }

    @Nullable
    QueueCounts get(String name) {
        Slot slot = byName.get(name);
        return slot == null ? null : slot.value;
    }

    Tx begin() {
        return new Tx();
    }

    final class Tx {
        private final Map<Long, long[]> deltas = new LinkedHashMap<>();
        private final List<Runnable> registrations = new ArrayList<>();

        void ready(long queueId, long n) {
            delta(queueId)[0] += n;
        }

        void leased(long queueId, long n) {
            delta(queueId)[1] += n;
        }

        void delayed(long queueId, long n) {
            delta(queueId)[2] += n;
        }

        void declared(String name, long queueId) {
            registrations.add(() -> register(name, queueId, new QueueCounts(0, 0, 0)));
        }

        void deleted(String name, long queueId) {
            registrations.add(() -> unregister(name, queueId));
        }

        private long[] delta(long queueId) {
            return deltas.computeIfAbsent(queueId, k -> new long[3]);
        }

        void apply() {
            for (Runnable r : registrations) {
                r.run();
            }
            for (Map.Entry<Long, long[]> e : deltas.entrySet()) {
                Slot slot = byId.get(e.getKey());
                if (slot == null) {

                    System.getLogger(QueueCounters.class.getName())
                            .log(System.Logger.Level.ERROR, "no counter registered for queue id " + e.getKey());
                    continue;
                }
                long[] d = e.getValue();
                QueueCounts v = slot.value;
                slot.value = new QueueCounts(v.ready() + d[0], v.leased() + d[1], v.delayed() + d[2]);
            }
        }
    }
}
