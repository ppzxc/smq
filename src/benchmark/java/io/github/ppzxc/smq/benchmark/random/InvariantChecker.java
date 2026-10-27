package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 발행 기록, 전달 기록, counts 표본을 받아 불변식 위반 목록을 돌려주는 순수 검사기.
 *
 * <p>시각은 모두 같은 벽시계(epoch ms)다. 호출 직전 시각을 기록한 쪽은 거짓 위반을 낼 수 없는 방향으로 기울어 있어서,
 * 검사기는 허용 오차 {@link #TOLERANCE_MILLIS}만 더 봐 준다.
 */
final class InvariantChecker {

    /** 지연과 단일 활성 컨슈머 판정의 허용 오차. 이만큼 이르게 꺼낸 것까지는 위반이 아니다. */
    static final long TOLERANCE_MILLIS = 50;

    private static final String SYSTEM_PREFIX = "smq-";
    // 공개 API 밖의 storage 패키지에 기대지 않으려고 헤더 이름과 값을 여기서 따로 적는다(GLOSSARY의 dead letter 이동).
    private static final String REASON = "smq-reason";
    private static final String ORIGINAL_MESSAGE_ID = "smq-original-message-id";
    private static final String LAST_DELIVERY_COUNT = "smq-last-delivery-count";
    private static final String REASON_REJECTED = "rejected";
    private static final String REASON_MAX_DELIVERIES = "max-deliveries";

    private InvariantChecker() {}

    /**
     * @param singleActiveConsumer 대상 큐가 단일 활성 컨슈머로 선언됐는가
     * @param maxDeliveries 대상 큐의 최대 전달 횟수. 없으면 0
     */
    record Config(boolean singleActiveConsumer, int maxDeliveries) {
    }

    /** 한 메시지(큐와 메시지 id)의 전달을 deliveryCount 순으로 모은 것. */
    private record Message(QueueKind queue, long id, List<Delivery> deliveries) {
        Tag tag() {
            return deliveries.getFirst().tag();
        }

        Delivery last() {
            return deliveries.getLast();
        }

        boolean acked() {
            return deliveries.stream().anyMatch(d -> d.settle().applied(Action.ACK));
        }
    }

    static List<Violation> check(Config config, List<Published> published, List<Delivery> deliveries,
            List<CountsSample> samples) {
        List<Violation> out = new ArrayList<>();
        Map<Tag, Published> byTag = new LinkedHashMap<>();
        for (Published p : published) {
            if (byTag.put(p.tag(), p) != null) {
                throw new IllegalArgumentException("같은 태그를 두 번 발행했다: " + p.tag());
            }
        }

        List<Delivery> known = new ArrayList<>();
        for (Delivery d : deliveries) {
            Published p = byTag.get(d.tag());
            if (p == null) {
                out.add(new Violation(Invariant.GHOST, List.of(d.tag()), List.of(d.messageId()),
                        "발행하지 않은 태그가 " + d.queue() + "에서 전달됐다"));
            } else {
                String diff = contentDiff(p.content(), d.content());
                if (diff != null) {
                    out.add(new Violation(Invariant.GHOST, List.of(d.tag()), List.of(d.messageId()),
                            d.queue() + " 전달이 발행 내용과 다르다: " + diff));
                }
                known.add(d);
            }
        }

        Map<String, Message> messages = groupMessages(known);
        Map<Tag, List<Message>> targets = new LinkedHashMap<>();
        Map<Tag, List<Message>> deadLetters = new LinkedHashMap<>();
        for (Message m : messages.values()) {
            checkMessage(config, byTag.get(m.tag()), m, out);
            (m.queue() == QueueKind.TARGET ? targets : deadLetters).computeIfAbsent(m.tag(), t -> new ArrayList<>())
                    .add(m);
        }
        for (Published p : published) {
            checkOutcome(config, p.tag(), targets.getOrDefault(p.tag(), List.of()),
                    deadLetters.getOrDefault(p.tag(), List.of()), out);
        }

        checkChunkOrder(deliveries, out);
        if (config.singleActiveConsumer()) {
            checkSingleActive(deliveries, out);
        }
        checkCounts(samples, out);
        return out;
    }

    // ---- 메시지 단위: 유령(같은 id에 다른 태그), 중복, 지연

    private static Map<String, Message> groupMessages(List<Delivery> known) {
        Map<String, List<Delivery>> grouped = new LinkedHashMap<>();
        for (Delivery d : known) {
            grouped.computeIfAbsent(d.queue() + "#" + d.messageId(), k -> new ArrayList<>()).add(d);
        }
        Map<String, Message> out = new LinkedHashMap<>();
        grouped.forEach((k, list) -> {
            List<Delivery> sorted = list.stream().sorted(Comparator.comparingInt(Delivery::deliveryCount)).toList();
            out.put(k, new Message(sorted.getFirst().queue(), sorted.getFirst().messageId(), sorted));
        });
        return out;
    }

    private static void checkMessage(Config config, Published pub, Message m, List<Violation> out) {
        List<Delivery> ds = m.deliveries();
        List<Tag> tags = List.of(m.tag());
        List<Long> ids = List.of(m.id());

        if (ds.stream().anyMatch(d -> !d.tag().equals(m.tag()))) {
            out.add(new Violation(Invariant.GHOST, ds.stream().map(Delivery::tag).distinct().toList(), ids,
                    m.queue() + " 메시지 id 하나가 서로 다른 태그로 전달됐다"));
        }

        for (int i = 0; i < ds.size(); i++) {
            if (ds.get(i).deliveryCount() != i + 1) {
                out.add(new Violation(Invariant.DUPLICATE, tags, ids, m.queue() + " deliveryCount가 1부터 빈틈없이 늘지 않는다: "
                        + ds.stream().map(d -> String.valueOf(d.deliveryCount())).collect(Collectors.joining(","))));
                break;
            }
        }
        if (m.queue() == QueueKind.TARGET && config.maxDeliveries() > 0) {
            ds.stream().filter(d -> d.deliveryCount() > config.maxDeliveries()).findFirst()
                    .ifPresent(d -> out.add(new Violation(Invariant.DUPLICATE, tags, ids,
                            "최대 전달 횟수 " + config.maxDeliveries() + "를 넘어 " + d.deliveryCount() + "번째로 전달됐다")));
        }

        if (m.queue() == QueueKind.TARGET && pub.delayMillis() > 0) {
            long due = pub.atMillis() + pub.delayMillis();
            long at = ds.getFirst().pull().atMillis();
            if (at < due - TOLERANCE_MILLIS) {
                out.add(new Violation(Invariant.DELAY, tags, ids,
                        "발행 지연 " + pub.delayMillis() + "ms 보다 " + (due - at) + "ms 일찍 전달됐다"));
            }
        }

        for (int i = 1; i < ds.size(); i++) {
            Delivery prev = ds.get(i - 1);
            Delivery cur = ds.get(i);
            long at = cur.pull().atMillis();
            if (prev.settle().applied(Action.ACK) || prev.settle().applied(Action.REJECT)) {
                out.add(new Violation(Invariant.DUPLICATE, tags, ids, m.queue() + " " + prev.settle().action()
                        + "이 반영된 뒤 " + cur.deliveryCount() + "번째로 다시 전달됐다"));
            } else if (prev.settle().applied(Action.NACK)) {
                long due = prev.settle().atMillis() + prev.settle().nackDelayMillis();
                if (at < due - TOLERANCE_MILLIS) {
                    out.add(new Violation(Invariant.DELAY, tags, ids, m.queue() + " NACK 지연 "
                            + prev.settle().nackDelayMillis() + "ms 보다 " + (due - at) + "ms 일찍 재전달됐다"));
                }
            } else if (at < prev.pull().leaseDeadlineMillis() - TOLERANCE_MILLIS) {
                out.add(new Violation(Invariant.DUPLICATE, tags, ids, m.queue() + " 리스 만료 "
                        + (prev.pull().leaseDeadlineMillis() - at) + "ms 전에 NACK 없이 재전달됐다"));
            }
        }
    }

    // ---- 태그 단위: 유실, dead letter 이동

    private static void checkOutcome(Config config, Tag tag, List<Message> targets, List<Message> deadLetters,
            List<Violation> out) {
        List<Tag> tags = List.of(tag);
        List<Long> targetIds = targets.stream().map(Message::id).toList();
        List<Long> dlIds = deadLetters.stream().map(Message::id).toList();

        if (targets.size() > 1) {
            out.add(new Violation(Invariant.DUPLICATE, tags, targetIds,
                    "한 번 발행한 태그가 대상 큐에 " + targets.size() + "개 메시지로 들어갔다"));
            return;
        }
        long acks = targets.stream().flatMap(m -> m.deliveries().stream())
                .filter(d -> d.settle().applied(Action.ACK)).count();
        Delivery last = targets.isEmpty() ? null : targets.getFirst().last();

        // 한 메시지가 두 번 OK로 ACK되는 경우는 ACK 뒤 재전달로 이미 보고된다.
        if (acks >= 1 && !deadLetters.isEmpty()) {
            out.add(new Violation(Invariant.DEAD_LETTER, tags, concat(targetIds, dlIds),
                    "OK로 ACK된 메시지가 dead letter 큐로도 옮겨졌다"));
        } else if (deadLetters.size() > 1) {
            out.add(new Violation(Invariant.DEAD_LETTER, tags, dlIds,
                    "한 태그가 dead letter 큐로 " + deadLetters.size() + "번 옮겨졌다"));
        } else if (deadLetters.size() == 1) {
            Message dl = deadLetters.getFirst();
            String problem = moveProblem(config, last, dl.deliveries().getFirst().content().headers());
            if (problem != null) {
                out.add(new Violation(Invariant.DEAD_LETTER, tags, concat(targetIds, dlIds), problem));
            }
            if (!dl.acked()) {
                out.add(new Violation(Invariant.LOSS, tags, dlIds, "dead letter 큐로 옮겨진 뒤 OK로 ACK되지 않았다"));
            }
        } else if (acks == 0) {
            if (last != null && mustHaveMoved(config, last)) {
                out.add(new Violation(Invariant.DEAD_LETTER, tags, targetIds,
                        "reject나 최대 전달 횟수 소진인데 dead letter 큐로 옮겨지지 않았다"));
            } else {
                out.add(new Violation(Invariant.LOSS, tags, targetIds,
                        last == null ? "발행했지만 한 번도 전달되지 않았다" : "전달됐지만 OK로 ACK되지 않았다"));
            }
        }
    }

    private static boolean mustHaveMoved(Config config, Delivery last) {
        return last.settle().applied(Action.REJECT)
                || (config.maxDeliveries() > 0 && last.deliveryCount() >= config.maxDeliveries());
    }

    /** 이동이 기록과 맞지 않으면 이유를, 맞으면 null을 돌려준다. */
    private static String moveProblem(Config config, Delivery last, Map<String, String> headers) {
        if (last == null) {
            return "대상 큐에서 한 번도 전달되지 않은 메시지가 dead letter 큐로 옮겨졌다";
        }
        List<String> problems = new ArrayList<>();
        String reason = headers.get(REASON);
        if (REASON_REJECTED.equals(reason)) {
            if (!last.settle().applied(Action.REJECT)) {
                problems.add("reason=rejected인데 마지막 전달이 reject되지 않았다");
            }
        } else if (REASON_MAX_DELIVERIES.equals(reason)) {
            if (config.maxDeliveries() <= 0 || last.deliveryCount() != config.maxDeliveries()
                    || last.settle().applied(Action.REJECT)) {
                problems.add("reason=max-deliveries인데 최대 전달 횟수를 소진한 기록이 아니다");
            }
        } else {
            problems.add(REASON + " 헤더가 없거나 알 수 없다: " + reason);
        }
        if (!String.valueOf(last.messageId()).equals(headers.get(ORIGINAL_MESSAGE_ID))) {
            problems.add(ORIGINAL_MESSAGE_ID + "=" + headers.get(ORIGINAL_MESSAGE_ID)
                    + " 기록은 " + last.messageId());
        }
        if (!String.valueOf(last.deliveryCount()).equals(headers.get(LAST_DELIVERY_COUNT))) {
            problems.add(LAST_DELIVERY_COUNT + "=" + headers.get(LAST_DELIVERY_COUNT)
                    + " 기록은 " + last.deliveryCount());
        }
        return problems.isEmpty() ? null : String.join("; ", problems);
    }

    // ---- 청크 단위

    private static void checkChunkOrder(List<Delivery> deliveries, List<Violation> out) {
        Map<Long, List<Delivery>> chunks = new LinkedHashMap<>();
        for (Delivery d : deliveries) {
            chunks.computeIfAbsent(d.pull().chunk(), k -> new ArrayList<>()).add(d);
        }
        for (List<Delivery> chunk : chunks.values()) {
            List<Delivery> sorted = chunk.stream().sorted(Comparator.comparingInt(d -> d.pull().position())).toList();
            for (int i = 1; i < sorted.size(); i++) {
                Delivery prev = sorted.get(i - 1);
                Delivery cur = sorted.get(i);
                if (cur.messageId() <= prev.messageId()) {
                    out.add(new Violation(Invariant.CHUNK_ORDER, List.of(prev.tag(), cur.tag()),
                            List.of(prev.messageId(), cur.messageId()), "청크 " + prev.pull().chunk()
                                    + " 안에서 메시지 id가 " + prev.messageId() + " 다음에 " + cur.messageId() + "다"));
                }
            }
        }
    }

    private record Lease(Delivery first, long start, long end) {
    }

    /** 청크의 리스는 꺼낸 시각부터 기한까지다. 청크 전체를 정산했다면 가장 늦은 정산 시각에 풀린다. */
    private static void checkSingleActive(List<Delivery> deliveries, List<Violation> out) {
        Map<Long, List<Delivery>> chunks = new LinkedHashMap<>();
        for (Delivery d : deliveries) {
            if (d.queue() == QueueKind.TARGET) {
                chunks.computeIfAbsent(d.pull().chunk(), k -> new ArrayList<>()).add(d);
            }
        }
        List<Lease> leases = new ArrayList<>();
        for (List<Delivery> chunk : chunks.values()) {
            long end = chunk.stream().mapToLong(d -> d.pull().leaseDeadlineMillis()).max().orElseThrow();
            if (chunk.stream().allMatch(d -> d.settle().action() != Action.NONE)) {
                end = Math.min(end, chunk.stream().mapToLong(d -> d.settle().atMillis()).max().orElseThrow());
            }
            Delivery first = chunk.stream().min(Comparator.comparingInt(d -> d.pull().position())).orElseThrow();
            leases.add(new Lease(first, first.pull().atMillis(), end));
        }
        leases.sort(Comparator.comparingLong(Lease::start));
        Lease widest = null;
        for (Lease cur : leases) {
            if (widest != null && cur.start() < widest.end() - TOLERANCE_MILLIS) {
                out.add(new Violation(Invariant.SINGLE_ACTIVE, List.of(widest.first().tag(), cur.first().tag()),
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

    private static void checkCounts(List<CountsSample> samples, List<Violation> out) {
        Set<QueueKind> drained = EnumSet.noneOf(QueueKind.class);
        for (CountsSample s : samples) {
            String where = s.queue() + (s.afterDrain() ? " 소진 뒤" : " 부하 중") + " counts=("
                    + s.ready() + ", " + s.leased() + ", " + s.delayed() + ")";
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

    // ---- 유틸

    /** 헤더의 {@code smq-} 접두사는 시스템 예약이라 dead letter 이동 때 붙은 것은 비교에서 뺀다. */
    private static String contentDiff(Content published, Content delivered) {
        if (published.payloadChecksum() != delivered.payloadChecksum()) {
            return "payload 체크섬 " + published.payloadChecksum() + " -> " + delivered.payloadChecksum();
        }
        Map<String, String> user = delivered.headers().entrySet().stream()
                .filter(e -> !e.getKey().startsWith(SYSTEM_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!published.headers().equals(user)) {
            return "헤더 " + published.headers() + " -> " + user;
        }
        return null;
    }

    private static List<Long> concat(List<Long> a, List<Long> b) {
        List<Long> out = new ArrayList<>(a);
        out.addAll(b);
        return out;
    }
}
