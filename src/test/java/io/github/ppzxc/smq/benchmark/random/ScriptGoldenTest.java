package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.zip.CRC32C;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ScriptGoldenTest {

    private static final Limits FULL = Limits.full(10_000);

    private static long digest(final long seed) {
        final RunParams params;
        final Script script;
        final CRC32C crc;
        params = RunParams.fromSeed(seed, FULL);
        script = new Script(params);
        crc = new CRC32C();
        crc.update(params.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (long seq = 0; seq < 50; seq++) {
            final Tag tag;
            final Script.Planned p;
            tag = new Tag(seed, seq);
            p = script.message(tag);
            crc.update(p.message().payload());
            final var hs = new java.util.TreeMap<>(p.content().headers());
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

    @ParameterizedTest
    @CsvSource({"1, 2987651438", "7, 113552775", "42, 1626929857", "-5, 2819112002", "123456789, 1054620683"})
    void generatedWorkloadIsPinnedForFixedSeed(final long seed, final long expectedDigest) {
        assertEquals(expectedDigest, digest(seed), "The generated workload digest should match the pinned value.");
    }
}
