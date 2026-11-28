package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

class OutgoingMessagePropertiesTest {

    @Property
    void payloadIsCopiedOnInputAndOutput(@ForAll("payloads") final byte[] payload) {
        final byte[] input;
        final byte[] original;
        final OutgoingMessage message;
        final byte[] returned;
        input = Arrays.copyOf(payload, payload.length + 1);
        original = input.clone();
        message = OutgoingMessage.of(input);
        assertNotSame(input, message.payload(), "The payload accessor must return a copy.");
        input[0] = (byte) (input[0] + 1);
        assertArrayEquals(original, message.payload(), "Mutating the input must not change the message.");
        returned = message.payload();
        returned[0] = (byte) (returned[0] + 1);
        assertArrayEquals(original, message.payload(), "Mutating the returned payload must not change the message.");
    }

    @Test
    void emptyPayloadIsValid() {
        assertArrayEquals(new byte[0], OutgoingMessage.of(new byte[0]).payload(), "An empty payload is valid.");
    }

    @Test
    void nullPayloadIsRejected() {
        assertThrows(NullPointerException.class, () -> OutgoingMessage.of(null), "A payload is required.");
    }

    @Provide
    Arbitrary<byte[]> payloads() {
        return Arbitraries.bytes().array(byte[].class).ofMaxSize(4096);
    }
}
