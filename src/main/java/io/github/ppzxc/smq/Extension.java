package io.github.ppzxc.smq;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

public record Extension(@Nullable Instant leaseDeadline, Outcomes outcomes) {}
