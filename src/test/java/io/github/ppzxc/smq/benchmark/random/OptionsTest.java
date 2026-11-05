package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.Durability;
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

    @Test
    void 메시지_크기_깊이_깊은_큐_옵션은_속도_옵션과_함께_받는다() {
        Options o = Options.parse(
                new String[] {"--rate", "500", "--message-bytes", "2048", "--depth", "300000", "--standing-queue"}, 0);
        assertEquals(2048, o.messageBytes());
        assertEquals(300_000, o.depth());
        assertTrue(o.standingQueue());
        Options d = Options.parse(new String[] {"--rate", "500"}, 0);
        assertEquals(0, d.messageBytes(), "0이면 기본값(1,024)");
        assertEquals(0, d.depth(), "0이면 한도의 기본 깊이");
        assertFalse(d.standingQueue());
    }

    @Test
    void 속도_없이_쓰면_거절하고_값은_양수여야_한다() {
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--message-bytes", "1024"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--depth", "1000"}, 0));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[] {"--standing-queue"}, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "500", "--message-bytes", "0"}, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> Options.parse(new String[] {"--rate", "500", "--depth", "-1"}, 0));
    }

    @Test
    void 내구성_옵션은_기본이_없고_수준_이름을_받는다() {
        assertTrue(Options.parse(new String[0], 0).durability().isEmpty());
        assertEquals(
                Durability.PROCESS_CRASH,
                Options.parse(new String[] {"--durability", "PROCESS_CRASH"}, 0)
                        .durability()
                        .orElseThrow());
        assertEquals(
                Durability.POWER_LOSS,
                Options.parse(new String[] {"--durability", "power_loss"}, 0)
                        .durability()
                        .orElseThrow());
        assertThrows(
                IllegalArgumentException.class, () -> Options.parse(new String[] {"--durability", "sometimes"}, 0));
    }
}
