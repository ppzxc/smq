package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Checks chunk ordering, overlapping leases, and sampled counts. */
final class SampleChecks {
    private SampleChecks() {}

    static void checkChunkOrder(List<Delivery> deliveries, List<Violation> out) {
        Map<Long, List<Delivery>> chunks = new LinkedHashMap<>();
        for (Delivery d : deliveries) {
            chunks.computeIfAbsent(d.pull().chunk(), k -> new ArrayList<>()).add(d);
        }
        for (List<Delivery> chunk : chunks.values()) {
            List<Delivery> sorted = chunk.stream()
                    .sorted(Comparator.comparingInt(d -> d.pull().position()))
                    .toList();
            for (int i = 1; i < sorted.size(); i++) {
                Delivery prev = sorted.get(i - 1);
                Delivery cur = sorted.get(i);
                if (cur.messageId() <= prev.messageId()) {
                    out.add(new Violation(
                            Invariant.CHUNK_ORDER,
                            List.of(prev.tag(), cur.tag()),
                            List.of(prev.messageId(), cur.messageId()),
                            "청크 " + prev.pull().chunk() + " 안에서 메시지 id가 " + prev.messageId() + " 다음에 " + cur.messageId()
                                    + "다"));
                }
            }
        }
    }

    private record Lease(Delivery first, long start, long end) {}

    /** 청크의 리스는 꺼낸 시각부터 기한까지다. 청크 전체를 정산했다면 가장 늦은 정산 시각에 풀린다. */
    static void checkSingleActive(List<Delivery> deliveries, List<Violation> out) {
        Map<Long, List<Delivery>> chunks = new LinkedHashMap<>();
        for (Delivery d : deliveries) {
            if (d.queue() == QueueKind.TARGET) {
                chunks.computeIfAbsent(d.pull().chunk(), k -> new ArrayList<>()).add(d);
            }
        }
        List<Lease> leases = new ArrayList<>();
        for (List<Delivery> chunk : chunks.values()) {
            long end = chunk.stream()
                    .mapToLong(d -> d.pull().leaseDeadlineMillis())
                    .max()
                    .orElseThrow();
            if (chunk.stream().allMatch(d -> d.settle().action() != Action.NONE)) {
                end = Math.min(
                        end,
                        chunk.stream()
                                .mapToLong(d -> d.settle().atMillis())
                                .max()
                                .orElseThrow());
            }
            Delivery first = chunk.stream()
                    .min(Comparator.comparingInt(d -> d.pull().position()))
                    .orElseThrow();
            leases.add(new Lease(first, first.pull().atMillis(), end));
        }
        leases.sort(Comparator.comparingLong(Lease::start));
        Lease widest = null;
        for (Lease cur : leases) {
            if (widest != null && cur.start() < widest.end() - InvariantChecker.TOLERANCE_MILLIS) {
                out.add(new Violation(
                        Invariant.SINGLE_ACTIVE,
                        List.of(widest.first().tag(), cur.first().tag()),
                        List.of(widest.first().messageId(), cur.first().messageId()),
                        "청크 " + widest.first().pull().chunk() + "의 리스가 " + (widest.end() - cur.start())
                                + "ms 더 살아 있는데 청크 " + cur.first().pull().chunk() + "가 꺼내졌다"));
            }
            if (widest == null || cur.end() > widest.end()) {
                widest = cur;
            }
        }
    }

    // ---- counts

    static void checkCounts(List<CountsSample> samples, List<Violation> out) {
        Set<QueueKind> drained = EnumSet.noneOf(QueueKind.class);
        for (CountsSample s : samples) {
            String where = s.queue() + (s.afterDrain() ? " 소진 뒤" : " 부하 중") + " counts=(" + s.ready() + ", "
                    + s.leased() + ", " + s.delayed() + ")";
            if (s.ready() < 0 || s.leased() < 0 || s.delayed() < 0) {
                out.add(new Violation(Invariant.COUNTS, List.of(), List.of(), where + "에 음수가 있다"));
            } else if (s.afterDrain() && (s.ready() != 0 || s.leased() != 0 || s.delayed() != 0)) {
                out.add(new Violation(Invariant.COUNTS, List.of(), List.of(), where + "가 (0, 0, 0)이 아니다"));
            }
            if (s.afterDrain()) {
                drained.add(s.queue());
            }
        }
        for (QueueKind q : QueueKind.values()) {
            if (!drained.contains(q)) {
                out.add(new Violation(Invariant.COUNTS, List.of(), List.of(), q + " 소진 뒤 counts 표본이 없다"));
            }
        }
    }
}
