package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class OptionsTest {

    @Test
    void defaultsMatchTheTicket() {
        Options o = Options.parse(new String[0], 1234);
        assertTrue(o.seed().isEmpty());
        assertEquals(Duration.ofMinutes(30), o.budget());
        assertEquals(1234, o.masterSeed());
        assertEquals(Duration.ofSeconds(10), o.load());
        assertFalse(o.smoke());
    }

    @Test
    void seedAndBudgetAreParsed() {
        Options o = Options.parse(new String[] {"--seed", "-42", "--budget", "90s", "--load", "3s"}, 0);
        assertEquals(-42, o.seed().getAsLong());
        assertEquals(Duration.ofSeconds(90), o.budget());
        assertEquals(Duration.ofSeconds(3), o.load());
    }

    @Test
    void durationsAcceptUnitsAndIso() {
        assertEquals(Duration.ofMinutes(30), Options.parseDuration("30m"));
        assertEquals(Duration.ofHours(2), Options.parseDuration("2h"));
        assertEquals(Duration.ofSeconds(45), Options.parseDuration("45"));
        assertEquals(Duration.ofMillis(1500), Options.parseDuration("1500ms"));
        assertEquals(Duration.ofMinutes(5), Options.parseDuration("PT5M"));
        assertThrows(IllegalArgumentException.class, () -> Options.parseDuration("soon"));
    }

    @Test
    void smokeUsesTwoSecondLoadsAndRejectsSeed() {
        Options o = Options.parse(new String[] {"--smoke"}, 0);
        assertTrue(o.smoke());
        assertEquals(Duration.ofSeconds(2), o.load());
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--smoke", "--seed", "1"}, 0));
    }

    @Test
    void badArgumentsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--seed"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--seed", "x"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--nope", "1"}, 0));
    }

    @Test
    void heapDumpIsOffUnlessAsked() {
        assertFalse(Options.parse(new String[] {}, 0).heapDump());
        assertTrue(Options.parse(new String[] {"--heap-dump"}, 0).heapDump());
    }

    @Test
    void 속도_옵션은_기본이_0이고_양의_정수를_받는다() {
        assertEquals(0, Options.parse(new String[0], 0).rate());
        assertEquals(500, Options.parse(new String[] {"--rate", "500"}, 0).rate());
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--rate", "0"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--rate", "-5"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--rate", "fast"}, 0));
    }

    @Test
    void 속도_옵션과_스모크는_함께_쓸_수_없다() {
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--smoke", "--rate", "100"}, 0));
    }

    @Test
    void 속도_옵션이_있으면_JFR이_기본으로_켜진다() {
        assertTrue(Options.parse(new String[] {"--rate", "500"}, 0).jfr());
        assertFalse(Options.parse(new String[0], 0).jfr());
    }
}
