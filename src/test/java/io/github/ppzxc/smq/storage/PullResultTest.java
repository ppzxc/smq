package io.github.ppzxc.smq.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Message;
import io.github.ppzxc.smq.MessageId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PullResultTest {
    private static final Instant DEADLINE = Instant.parse("2026-01-01T00:00:10Z");
    private static final Message MESSAGE = new Message(new MessageId(1), new byte[] {1}, Map.of(), 1, Instant.EPOCH);

    @Test
    @DisplayName("requires token and deadline exactly when messages are present — messagesRequireLeaseToken")
    void messagesRequireLeaseToken() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(null, DEADLINE, List.of(MESSAGE), false, false),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("requires token and deadline exactly when messages are present — messagesRequireLeaseDeadline")
    void messagesRequireLeaseDeadline() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(new LeaseToken(1), null, List.of(MESSAGE), false, false),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("requires token and deadline exactly when messages are present — emptyMessagesRejectLease")
    void emptyMessagesRejectLease() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(new LeaseToken(1), DEADLINE, List.of(), false, false),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("requires token and deadline exactly when messages are present — messagesRejectMissingLease")
    void messagesRejectMissingLease() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PullResult(null, null, List.of(MESSAGE), false, false),
                "Construction should throw IllegalArgumentException.");
    }

    @Test
    @DisplayName("copies messages")
    void copiesMessages() {
        final List<Message> messages;
        final PullResult result;
        messages = new ArrayList<>(List.of(MESSAGE));
        result = PullResult.of(new LeaseToken(1), DEADLINE, messages);
        messages.clear();
        assertEquals(List.of(MESSAGE), result.messages(), "Messages should match the expected value.");
    }

    @Test
    @DisplayName("copied messages are immutable")
    void copiedMessagesAreImmutable() {
        final PullResult result;
        result = PullResult.of(new LeaseToken(1), DEADLINE, List.of(MESSAGE));
        assertThrows(
                UnsupportedOperationException.class,
                () -> result.messages().clear(),
                "Clear should throw UnsupportedOperationException.");
    }

    @Test
    void emptyResultReportsRemainingDeadLetters() {
        assertTrue(PullResult.emptyDeadLettersRemain(true).deadLettersRemain(), "Dead letters should remain.");
    }

    @Test
    void emptyResultReportsMovedDeadLetters() {
        assertTrue(PullResult.emptyDeadLettersRemain(true).deadLettersMoved(), "Dead letters should be moved.");
    }

    @Test
    void emptyResultHasNoMessages() {
        assertTrue(PullResult.emptyDeadLettersRemain(true).messages().isEmpty(), "Messages should be empty.");
    }

    @Test
    void emptyResultHasNoToken() {
        assertNull(PullResult.emptyDeadLettersRemain(true).token(), "Token should be null.");
    }

    @Test
    void emptyResultHasNoLeaseDeadline() {
        assertNull(PullResult.emptyDeadLettersRemain(true).leaseDeadline(), "Lease deadline should be null.");
    }
}
