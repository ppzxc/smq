package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LeaseCounterexampleTest {

    private enum Step {
        PUBLISH,
        PULL,
        ADVANCE_TO_DEADLINE
    }

    @Test
    @DisplayName("만료 정각 변이는 원본과 축소 명령열에서 같은 차이를 낸다 — minimized Sequence Exposes Expiry Mutation")
    void minimizedSequenceExposesExpiryMutation() {
        final List<Step> original;
        final List<Step> minimized;
        original = List.of(Step.PUBLISH, Step.PULL, Step.ADVANCE_TO_DEADLINE, Step.PULL);
        minimized = shrink(original);
        assertNotEquals(
                replay(minimized, false), replay(minimized, true), "Replay should differ from the unexpected value.");
    }

    @Test
    @DisplayName("만료 정각 변이는 원본과 축소 명령열에서 같은 차이를 낸다 — original Sequence Exposes Expiry Mutation")
    void originalSequenceExposesExpiryMutation() {
        final List<Step> original;
        original = List.of(Step.PUBLISH, Step.PULL, Step.ADVANCE_TO_DEADLINE, Step.PULL);
        assertNotEquals(
                replay(original, false), replay(original, true), "Replay should differ from the unexpected value.");
    }

    @Test
    @DisplayName("만료 정각 변이는 원본과 축소 명령열에서 같은 차이를 낸다 — minimized Sequence Retains Required Steps")
    void minimizedSequenceRetainsRequiredSteps() {
        final List<Step> original;
        final List<Step> minimized;
        original = List.of(Step.PUBLISH, Step.PULL, Step.ADVANCE_TO_DEADLINE, Step.PULL);
        minimized = shrink(original);
        assertEquals(original, minimized, "Minimized should match the expected value.");
    }

    private static List<Step> shrink(final List<Step> steps) {
        var result = new ArrayList<>(steps);
        for (int i = 0; i < result.size(); ) {
            final var candidate = new ArrayList<>(result);
            candidate.remove(i);
            if (replay(candidate, false) != replay(candidate, true)) {
                result = candidate;
            } else {
                i++;
            }
        }
        return List.copyOf(result);
    }

    private static int replay(final List<Step> steps, final boolean strictExpiryMutation) {
        boolean ready;
        boolean leased;
        long now;
        long deadline;
        int deliveries;
        ready = false;
        leased = false;
        now = 0;
        deadline = 0;
        deliveries = 0;
        for (final Step step : steps) {
            switch (step) {
                case PUBLISH -> ready = true;
                case ADVANCE_TO_DEADLINE -> now = deadline;
                case PULL -> {
                    if (leased && (strictExpiryMutation ? deadline < now : deadline <= now)) {
                        leased = false;
                        ready = true;
                    }
                    if (ready) {
                        ready = false;
                        leased = true;
                        deadline = now + 10;
                        deliveries++;
                    }
                }
            }
        }
        return deliveries;
    }
}
