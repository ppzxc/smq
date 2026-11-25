package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PullResultTest {
    private static final Instant DEADLINE = Instant.parse("2026-01-01T00:00:10Z");
    private static final Message MESSAGE = new Message(new MessageId(1), new byte[] {1}, Map.of(), 1, Instant.EPOCH);

    @Test
    void requires_token_and_deadline_exactly_when_messages_are_present() {
        assertThrows(
                IllegalArgumentException.class, () -> new PullResult(null, DEADLINE, List.of(MESSAGE), false, false));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(new LeaseToken(1), null, List.of(MESSAGE), false, false));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(new LeaseToken(1), DEADLINE, List.of(), false, false));
        assertThrows(IllegalArgumentException.class, () -> new PullResult(null, null, List.of(MESSAGE), false, false));
    }

    @Test
    void copies_messages_and_keeps_empty_dead_letter_flags() {
        List<Message> messages = new ArrayList<>(List.of(MESSAGE));
        PullResult result = PullResult.of(new LeaseToken(1), DEADLINE, messages);
        messages.clear();
        assertEquals(List.of(MESSAGE), result.messages());
        assertThrows(
                UnsupportedOperationException.class, () -> result.messages().clear());

        PullResult empty = PullResult.emptyDeadLettersRemain(true);
        assertTrue(empty.deadLettersRemain());
        assertTrue(empty.deadLettersMoved());
        assertTrue(empty.messages().isEmpty());
        assertNull(empty.token());
        assertNull(empty.leaseDeadline());
    }
}
