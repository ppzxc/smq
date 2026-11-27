package io.github.ppzxc.smq.storage;

import io.github.ppzxc.smq.Message;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public record PullResult(
        @Nullable LeaseToken token,
        @Nullable Instant leaseDeadline,
        List<Message> messages,
        boolean deadLettersRemain,
        boolean deadLettersMoved) {

    public PullResult {
        messages = List.copyOf(Objects.requireNonNull(messages, "messages"));
        if (messages.isEmpty() ? token != null || leaseDeadline != null : token == null || leaseDeadline == null) {
            throw new IllegalArgumentException(
                    "token and leaseDeadline must be present exactly when messages are present");
        }
    }

    public static PullResult of(final LeaseToken token, final Instant leaseDeadline, final List<Message> messages) {
        return of(token, leaseDeadline, messages, false);
    }

    public static PullResult of(
            final LeaseToken token, final Instant leaseDeadline, final List<Message> messages, final boolean moved) {
        return new PullResult(token, leaseDeadline, messages, false, moved);
    }

    public static PullResult empty() {
        return empty(false);
    }

    public static PullResult empty(final boolean moved) {
        return new PullResult(null, null, List.of(), false, moved);
    }

    public static PullResult emptyDeadLettersRemain() {
        return emptyDeadLettersRemain(false);
    }

    public static PullResult emptyDeadLettersRemain(final boolean moved) {
        return new PullResult(null, null, List.of(), true, moved);
    }
}
