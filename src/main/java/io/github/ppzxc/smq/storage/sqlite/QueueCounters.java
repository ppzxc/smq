package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.QueueCounts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * 큐마다 준비·리스 중·지연됨 개수를 메모리에 둔다(ADR-0018). {@code counts()}는 DB도 쓰기 락도 쓰지 않고 여기서 읽는다.
 *
 * <p>개수는 기록된 상태 전환을 따라 바뀐다. 쓰기 트랜잭션은 {@link Tx}에 변화량을 모아 두었다가 커밋 직후, 이미 쥔 쓰기 락 안에서
 * 한꺼번에 반영한다. 큐마다 {@link QueueCounts}(바꿀 수 없는 값) 하나를 {@code volatile} 참조째 바꾸므로 읽는 쪽은 갱신 도중의
 * 섞인 값을 볼 수 없다. 두 큐가 함께 바뀌는 연산(dead letter 이동)은 큐별로 따로 바꾸므로 두 큐 사이에 짧은 어긋남이 보일 수 있다.
 * 메모리는 큐 수에 비례한다.
 */
final class QueueCounters {

    private static final class Slot {
        volatile QueueCounts value;

        Slot(QueueCounts value) {
            this.value = value;
        }
    }

    private final ConcurrentHashMap<String, Slot> byName = new ConcurrentHashMap<>();
    /** 쓰기 락 아래에서만 읽고 쓴다. */
    private final Map<Long, Slot> byId = new HashMap<>();

    /** 쓰기 락 아래(또는 아직 공개 전)에서 큐를 등록한다. */
    void register(String name, long queueId, QueueCounts initial) {
        Slot slot = new Slot(initial);
        byName.put(name, slot);
        byId.put(queueId, slot);
    }

    private void unregister(String name, long queueId) {
        byName.remove(name);
        byId.remove(queueId);
    }

    /** 큐가 없으면 null. 락 없이 부를 수 있다. */
    @Nullable
    QueueCounts get(String name) {
        Slot slot = byName.get(name);
        return slot == null ? null : slot.value;
    }

    Tx begin() {
        return new Tx();
    }

    /** 한 트랜잭션의 변화량. 쓰기 락을 쥔 스레드 하나만 쓴다. */
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

        /** 새로 만든 큐. 커밋되면 0으로 등록한다. */
        void declared(String name, long queueId) {
            registrations.add(() -> register(name, queueId, new QueueCounts(0, 0, 0)));
        }

        /** 지운 큐. 커밋되면 뺀다. */
        void deleted(String name, long queueId) {
            registrations.add(() -> unregister(name, queueId));
        }

        private long[] delta(long queueId) {
            return deltas.computeIfAbsent(queueId, k -> new long[3]);
        }

        /** 커밋 직후 쓰기 락 아래에서 한 번 부른다. 던지지 않는다. */
        void apply() {
            for (Runnable r : registrations) {
                r.run();
            }
            for (Map.Entry<Long, long[]> e : deltas.entrySet()) {
                Slot slot = byId.get(e.getKey());
                if (slot == null) {
                    // 커밋된 DB에 있는 큐인데 등록되지 않았다면 불변식 위반이다. 조용히 넘기지 않는다.
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
