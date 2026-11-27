package io.github.ppzxc.smq.storage;

import java.time.Instant;
import java.util.List;

public record Extended(Instant leaseDeadline, List<Result> results) {
    public Extended {
        results = List.copyOf(results);
    }
}
