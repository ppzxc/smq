package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LeaseCounterexampleTest {

    private enum Step {
        PUBLISH,
        PULL,
        ADVANCE_TO_DEADLINE
    }

    @Test
    void 만료_정각_변이는_원본과_축소_명령열에서_같은_차이를_낸다() {
        List<Step> original = List.of(Step.PUBLISH, Step.PULL, Step.ADVANCE_TO_DEADLINE, Step.PULL);
        List<Step> minimized = shrink(original);

        assertNotEquals(replay(minimized, false), replay(minimized, true));
        assertNotEquals(replay(original, false), replay(original, true));
        assertEquals(original, minimized);
    }

    private static List<Step> shrink(List<Step> steps) {
        var result = new ArrayList<>(steps);
        for (int i = 0; i < result.size(); ) {
            var candidate = new ArrayList<>(result);
            candidate.remove(i);
            if (replay(candidate, false) != replay(candidate, true)) {
                result = candidate;
            } else {
                i++;
            }
        }
        return List.copyOf(result);
    }

    private static int replay(List<Step> steps, boolean strictExpiryMutation) {
        boolean ready = false;
        boolean leased = false;
        long now = 0;
        long deadline = 0;
        int deliveries = 0;
        for (Step step : steps) {
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
