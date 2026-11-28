package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Chunk;
import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import io.github.ppzxc.smq.Outcomes;
import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.Queue;
import io.github.ppzxc.smq.QueueCounts;
import io.github.ppzxc.smq.QueueOptions;
import io.github.ppzxc.smq.Smq;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class SoakRunner {

    private record Accounting(int published, int delivered) {}

    private static final Duration NO_WAIT = Duration.ZERO;
    private static final Duration LEASE = Duration.ofSeconds(10);
    private static final Duration PULL_WAIT = Duration.ofSeconds(2);

    private final SoakOptions options;
    private final Durability durability;
    private final Queue accounting;
    private final Queue limited;
    private final Queue deadLetters;
    private final Queue delayed;
    private final Queue singleActive;

    SoakRunner(final SoakOptions options, final Durability durability, final Smq smq) {
        this.options = options;
        this.durability = durability;
        this.accounting = smq.declare("soak-accounting", QueueOptions.defaults());
        this.deadLetters = smq.declare("soak-dead", QueueOptions.defaults());
        this.limited = smq.declare("soak-limited", QueueOptions.defaults().maxDeliveries(2, "soak-dead"));
        this.delayed = smq.declare("soak-delayed", QueueOptions.defaults());
        this.singleActive =
                smq.declare("soak-single-active", QueueOptions.defaults().singleActiveConsumer(true));
    }

    List<SoakResult> run() throws InterruptedException {
        final List<SoakResult> results = new ArrayList<>();
        for (int round = 1; round <= this.options.budget(); round++) {
            final long start = System.nanoTime();
            final SoakWorkload.Round plan = SoakWorkload.round(this.options.seed(), round);
            final List<SoakViolation> violations = new ArrayList<>();
            final Accounting counts = this.checkMessageAccounting(round, plan, violations);
            this.checkDeliveryLimit(round, plan, violations);
            this.checkDelayAndCounts(round, plan, violations);
            this.checkSingleActiveConsumer(round, plan, violations);
            results.add(new SoakResult(
                    this.durability,
                    round,
                    counts.published(),
                    counts.delivered(),
                    System.nanoTime() - start,
                    violations));
            if (!violations.isEmpty()) {
                break;
            }
        }
        return List.copyOf(results);
    }

    private Accounting checkMessageAccounting(
            final int round, final SoakWorkload.Round plan, final List<SoakViolation> violations)
            throws InterruptedException {
        final List<OutgoingMessage> batch = new ArrayList<>();
        for (int index = 0; index < plan.messageCount(); index++) {
            batch.add(OutgoingMessage.of(plan.payloads().get(index)));
        }
        final List<MessageId> published = this.accounting.publish(batch);
        final Set<MessageId> expected = new HashSet<>(published);
        final List<MessageId> observed = new ArrayList<>();
        boolean dropFirst = "drop-first-delivery".equals(this.options.negativeControl()) && round == 1;
        int received = 0;
        while (received < published.size()) {
            final Chunk chunk = this.accounting.pull(plan.chunkSize(), PULL_WAIT, LEASE);
            if (chunk.isEmpty()) {
                break;
            }
            for (final Message message : chunk.messages()) {
                received++;
                final MessageId id = message.id();
                if (!expected.contains(id)) {
                    this.addViolation(
                            violations,
                            "GHOST_DELIVERY",
                            "message-accounting",
                            round,
                            List.of(id),
                            Map.of("publishedIds", ids(published).toString(), "deliveredId", id(id)));
                }
                if (dropFirst) {
                    dropFirst = false;
                } else {
                    observed.add(id);
                }
            }
            final Outcomes settled = this.accounting.settle(chunk.ackAll());
            if (!settled.allOk()) {
                this.addViolation(
                        violations,
                        "LOSS",
                        "message-accounting",
                        round,
                        chunk.messages().stream().map(Message::id).toList(),
                        Map.of("settled", settled.byMessage().toString()));
            }
        }

        if (published.size() != plan.messageCount()) {
            this.addViolation(
                    violations,
                    "LOSS",
                    "message-accounting",
                    round,
                    published,
                    Map.of(
                            "expectedPublished",
                            Integer.toString(plan.messageCount()),
                            "actualPublished",
                            Integer.toString(published.size())));
        }
        if (!new HashSet<>(observed).equals(expected) || observed.size() != expected.size()) {
            this.addViolation(
                    violations,
                    "LOSS",
                    "message-accounting",
                    round,
                    missingIds(published, observed),
                    Map.of(
                            "publishedIds",
                            ids(published).toString(),
                            "observedDeliveryIds",
                            ids(observed).toString()));
        }
        if (!published.equals(observed) && !isDropFirst(this.options, round, observed, expected)) {
            this.addViolation(
                    violations,
                    "CHUNK_ORDER",
                    "message-accounting",
                    round,
                    unionIds(published, observed),
                    Map.of(
                            "publishedIds",
                            ids(published).toString(),
                            "observedDeliveryIds",
                            ids(observed).toString()));
        }
        return new Accounting(published.size(), received);
    }

    private void checkDeliveryLimit(
            final int round, final SoakWorkload.Round plan, final List<SoakViolation> violations)
            throws InterruptedException {
        final MessageId originalId =
                this.limited.publish(OutgoingMessage.of(plan.payloads().getFirst()));
        final List<Integer> deliveryCounts = new ArrayList<>();
        for (int expectedCount = 1; expectedCount <= 2; expectedCount++) {
            final Chunk chunk = this.limited.pull(1, NO_WAIT, LEASE);
            if (chunk.isEmpty()) {
                break;
            }
            final Message message = chunk.messages().getFirst();
            deliveryCounts.add(message.deliveryCount());
            if (!originalId.equals(message.id()) || message.deliveryCount() != expectedCount) {
                this.addViolation(
                        violations,
                        "DUPLICATE_LIMIT",
                        "delivery-limit",
                        round,
                        List.of(originalId, message.id()),
                        Map.of(
                                "expectedDeliveryCount",
                                Integer.toString(expectedCount),
                                "observedDeliveryCount",
                                Integer.toString(message.deliveryCount())));
            }
            this.limited.settle(chunk.nackAll(NO_WAIT));
        }
        final Chunk tooMany = this.limited.pull(1, NO_WAIT, LEASE);
        if (!tooMany.isEmpty()) {
            final Message message = tooMany.messages().getFirst();
            deliveryCounts.add(message.deliveryCount());
            this.addViolation(
                    violations,
                    "DUPLICATE_LIMIT",
                    "delivery-limit",
                    round,
                    List.of(originalId, message.id()),
                    Map.of(
                            "allowedDeliveries",
                            "2",
                            "observedDeliveryCount",
                            Integer.toString(message.deliveryCount())));
            this.limited.settle(tooMany.ackAll());
        }
        if (!deliveryCounts.equals(List.of(1, 2))) {
            this.addViolation(
                    violations,
                    "DUPLICATE_LIMIT",
                    "delivery-limit",
                    round,
                    List.of(originalId),
                    Map.of("expectedDeliveryCounts", "[1, 2]", "observedDeliveryCounts", deliveryCounts.toString()));
        }

        final Chunk movedChunk = this.deadLetters.pull(1, NO_WAIT, LEASE);
        if (movedChunk.isEmpty()) {
            this.addViolation(
                    violations,
                    "DEAD_LETTER_MOVEMENT",
                    "delivery-limit",
                    round,
                    List.of(originalId),
                    Map.of("expectedOriginalIdHeader", id(originalId), "observedDeadLetter", "empty"));
            return;
        }
        final Message moved = movedChunk.messages().getFirst();
        final String originalHeader = moved.headers().get("smq-original-message-id");
        final String deliveryHeader = moved.headers().get("smq-last-delivery-count");
        final String reasonHeader = moved.headers().get("smq-reason");
        if (originalId.equals(moved.id())
                || !id(originalId).equals(originalHeader)
                || !"2".equals(deliveryHeader)
                || !"max-deliveries".equals(reasonHeader)) {
            this.addViolation(
                    violations,
                    "DEAD_LETTER_MOVEMENT",
                    "delivery-limit",
                    round,
                    List.of(originalId, moved.id()),
                    Map.of(
                            "expected",
                                    "new id, original=" + id(originalId) + ", deliveryCount=2, reason=max-deliveries",
                            "observed",
                                    "id=" + id(moved.id()) + ", original=" + originalHeader + ", deliveryCount="
                                            + deliveryHeader + ", reason=" + reasonHeader));
        }
        this.deadLetters.settle(movedChunk.ackAll());
    }

    private void checkDelayAndCounts(
            final int round, final SoakWorkload.Round plan, final List<SoakViolation> violations)
            throws InterruptedException {
        final Duration delay = Duration.ofMillis(this.options.delayMillis());
        final MessageId id = this.delayed.publish(
                OutgoingMessage.of(plan.payloads().getFirst()).withDelay(delay));
        final QueueCounts before = this.delayed.counts();
        final Chunk early = this.delayed.pull(1, NO_WAIT, LEASE);
        final QueueCounts afterEarlyPull = this.delayed.counts();
        if (!early.isEmpty()) {
            this.addViolation(
                    violations,
                    "DELAY",
                    "delayed-publication",
                    round,
                    List.of(id, early.messages().getFirst().id()),
                    Map.of(
                            "expected",
                            "no delivery before " + delay,
                            "observedIds",
                            idsFromChunk(early).toString()));
            this.delayed.settle(early.ackAll());
            return;
        }
        if (!new QueueCounts(0, 0, 1).equals(before) || !new QueueCounts(0, 0, 1).equals(afterEarlyPull)) {
            this.addViolation(
                    violations,
                    "APPROXIMATE_COUNTS",
                    "delayed-publication",
                    round,
                    List.of(id),
                    Map.of(
                            "expectedBeforeAndEarlyPull",
                            "QueueCounts[ready=0, leased=0, delayed=1]",
                            "observedBefore",
                            before.toString(),
                            "observedAfterEarlyPull",
                            afterEarlyPull.toString()));
        }

        Thread.sleep((long) this.options.delayMillis() + 5L);
        final QueueCounts afterDeadline = this.delayed.counts();
        if (!new QueueCounts(0, 0, 1).equals(afterDeadline)) {
            this.addViolation(
                    violations,
                    "APPROXIMATE_COUNTS",
                    "delayed-publication",
                    round,
                    List.of(id),
                    Map.of(
                            "expectedBeforePromotion",
                            "QueueCounts[ready=0, leased=0, delayed=1]",
                            "observed",
                            afterDeadline.toString()));
        }
        final Chunk ready = this.delayed.pull(1, PULL_WAIT, LEASE);
        if (ready.isEmpty() || !id.equals(ready.messages().getFirst().id())) {
            this.addViolation(
                    violations,
                    "DELAY",
                    "delayed-publication",
                    round,
                    List.of(id),
                    Map.of(
                            "expectedAvailableAfterDelay",
                            id(id),
                            "observedIds",
                            idsFromChunk(ready).toString()));
            return;
        }
        final QueueCounts leased = this.delayed.counts();
        if (!new QueueCounts(0, 1, 0).equals(leased)) {
            this.addViolation(
                    violations,
                    "APPROXIMATE_COUNTS",
                    "delayed-publication",
                    round,
                    List.of(id),
                    Map.of(
                            "expectedAfterPromotion",
                            "QueueCounts[ready=0, leased=1, delayed=0]",
                            "observed",
                            leased.toString()));
        }
        this.delayed.settle(ready.ackAll());
    }

    private void checkSingleActiveConsumer(
            final int round, final SoakWorkload.Round plan, final List<SoakViolation> violations)
            throws InterruptedException {
        final List<MessageId> published = this.singleActive.publish(List.of(
                OutgoingMessage.of(plan.payloads().getFirst()),
                OutgoingMessage.of(plan.payloads().get(1))));
        final Chunk first = this.singleActive.pull(1, NO_WAIT, LEASE);
        final Chunk blocked = this.singleActive.pull(1, NO_WAIT, LEASE);
        if (first.isEmpty() || !blocked.isEmpty()) {
            this.addViolation(
                    violations,
                    "SINGLE_ACTIVE_CONSUMER",
                    "single-active-lease",
                    round,
                    published,
                    Map.of(
                            "firstChunkIds",
                            idsFromChunk(first).toString(),
                            "secondPullIds",
                            idsFromChunk(blocked).toString()));
            if (!first.isEmpty()) {
                this.singleActive.settle(first.ackAll());
            }
            if (!blocked.isEmpty()) {
                this.singleActive.settle(blocked.ackAll());
            }
            return;
        }
        this.singleActive.settle(first.ackAll());
        final Chunk afterRelease = this.singleActive.pull(1, NO_WAIT, LEASE);
        if (afterRelease.isEmpty()
                || !published.get(1).equals(afterRelease.messages().getFirst().id())) {
            this.addViolation(
                    violations,
                    "SINGLE_ACTIVE_CONSUMER",
                    "single-active-lease",
                    round,
                    published,
                    Map.of(
                            "expectedAfterAck",
                            id(published.get(1)),
                            "observedIds",
                            idsFromChunk(afterRelease).toString()));
        }
        if (!afterRelease.isEmpty()) {
            this.singleActive.settle(afterRelease.ackAll());
        }
    }

    private void addViolation(
            final List<SoakViolation> violations,
            final String invariant,
            final String scenario,
            final int round,
            final List<MessageId> messageIds,
            final Map<String, String> observations) {
        violations.add(
                new SoakViolation(invariant, scenario, this.options.seed(), round, ids(messageIds), observations));
    }

    private static List<MessageId> missingIds(final List<MessageId> expected, final List<MessageId> observed) {
        final Set<MessageId> found = new HashSet<>(observed);
        return expected.stream().filter(id -> !found.contains(id)).toList();
    }

    private static List<MessageId> unionIds(final List<MessageId> left, final List<MessageId> right) {
        final List<MessageId> result = new ArrayList<>(left);
        for (final MessageId id : right) {
            if (!result.contains(id)) {
                result.add(id);
            }
        }
        return result;
    }

    private static List<Long> ids(final List<MessageId> ids) {
        return ids.stream().map(MessageId::value).toList();
    }

    private static List<Long> idsFromChunk(final Chunk chunk) {
        return ids(chunk.messages().stream().map(Message::id).toList());
    }

    private static String id(final MessageId id) {
        return Long.toString(id.value());
    }

    private static boolean isDropFirst(
            final SoakOptions options, final int round, final List<MessageId> observed, final Set<MessageId> expected) {
        return "drop-first-delivery".equals(options.negativeControl())
                && round == 1
                && observed.size() + 1 == expected.size();
    }
}
