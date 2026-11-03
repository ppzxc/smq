package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/** 시드 재현성 고정. 기대값은 다중 프로세스 흔적을 걷어내기 전 커밋(5dd43a9)에서 뽑은 것이다. */
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
            hs.remove(Script.TAG_HEADER); // 태그 문자열 형식은 바뀐다
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
        assertEquals(1650023215L, digest(1));
        assertEquals(3614411652L, digest(7));
        assertEquals(4015063363L, digest(42));
        assertEquals(4006591804L, digest(-5));
        assertEquals(1018423965L, digest(123456789L));
    }
}
