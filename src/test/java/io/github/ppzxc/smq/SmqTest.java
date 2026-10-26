package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SmqTest {

    @Test
    void name() {
        assertEquals("smq", Smq.name());
    }
}
