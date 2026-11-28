package io.github.ppzxc.smq.harness;

import io.github.ppzxc.smq.Durability;
import java.util.List;

record SoakResult(
        Durability durability,
        int round,
        int published,
        int delivered,
        long elapsedNanos,
        List<SoakViolation> violations) {

    SoakResult {
        violations = List.copyOf(violations);
    }

    boolean passed() {
        return violations.isEmpty();
    }
}
