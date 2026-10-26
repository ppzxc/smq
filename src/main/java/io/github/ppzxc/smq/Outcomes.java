package io.github.ppzxc.smq;

import java.util.List;
import java.util.Map;

public record Outcomes(Map<MessageId, Outcome> byMessage) {

    public Outcomes {
        byMessage = Map.copyOf(byMessage);
    }

    public boolean allOk() {
        return byMessage.values().stream().allMatch(o -> o == Outcome.OK);
    }

    public List<MessageId> stale() {
        return byMessage.entrySet().stream()
                .filter(e -> e.getValue() == Outcome.STALE)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }
}
