package io.github.ppzxc.smq.harness;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

record SoakViolation(
        String invariant,
        String scenario,
        long seed,
        int round,
        List<Long> messageIds,
        Map<String, String> observations) {

    SoakViolation {
        messageIds = List.copyOf(messageIds);
        observations = Collections.unmodifiableMap(new TreeMap<>(observations));
    }
}
