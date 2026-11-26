package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class ScriptGoldenTest {

    private static final Limits FULL = Limits.full(10_000);

    private static long digest(long seed) {
        RunParams params = RunParams.fromSeed(seed, FULL);
        Script script = new Script(params);
        CRC32C crc = new CRC32C();
        crc.update(params.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (long seq = 0; seq < 50; seq++) {
            Tag tag = new Tag(seed, seq);
            Script.Planned p = script.message(tag);
            crc.update(p.message().payload());
            var hs = new java.util.TreeMap<>(p.content().headers());
            hs.remove(Script.TAG_HEADER);
            crc.update(hs.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            crc.update(Long.toString(p.delayMillis()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            for (int dc = 1; dc <= 3; dc++) {
                crc.update(script.settle(tag, dc).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                crc.update(script.processing(tag, dc).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return crc.getValue();
    }

    @Test
    void generatedWorkloadIsPinnedForFixedSeeds() {
        assertEquals(2987651438L, digest(1));
        assertEquals(113552775L, digest(7));
        assertEquals(1626929857L, digest(42));
        assertEquals(2819112002L, digest(-5));
        assertEquals(1054620683L, digest(123456789L));
    }
}
