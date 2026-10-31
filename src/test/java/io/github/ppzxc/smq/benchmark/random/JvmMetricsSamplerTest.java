package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 메트릭 파일에 CPU가 남고, 파일 하나에서 요약을 다시 만들 수 있는지. */
class JvmMetricsSamplerTest {

    @TempDir
    Path dir;

    @Test
    void 표본_줄에_프로세스_CPU가_들어_있다() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                        instanceof com.sun.management.OperatingSystemMXBean,
                "이 플랫폼은 프로세스 CPU를 주지 않는다");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            long end = System.nanoTime() + 400_000_000L;
            while (System.nanoTime() < end) {
                Thread.onSpinWait();
            }
        }
        List<String> lines = Files.readAllLines(dir.resolve("jvm-metrics.jsonl"));
        assertFalse(lines.isEmpty());
        assertTrue(
                lines.stream().anyMatch(l -> l.contains("\"processCpuLoad\"")), "CPU 필드가 있어야 한다: " + lines.getFirst());
        assertTrue(lines.getFirst().contains("\"processCpuTimeMs\""));
    }

    @Test
    void 메트릭_파일에서_요약을_다시_만든다() throws IOException {
        Path f = dir.resolve("p0-jvm-metrics.jsonl");
        Files.writeString(f, """
                {"heapUsedMB":10,"nonHeapUsedMB":5,"gcCount":1,"gcTimeMs":10,"liveThreads":4,"openFds":8,"maxFds":100,"processCpuLoad":0.2}
                {"heapUsedMB":30,"nonHeapUsedMB":7,"gcCount":3,"gcTimeMs":40,"liveThreads":6,"openFds":12,"maxFds":100,"processCpuLoad":0.6}
                """);

        JvmMetricsSampler.Summary s = JvmMetricsSampler.summarize(f);

        assertEquals(30L * 1024 * 1024, s.peakHeapUsedBytes());
        assertEquals(7L * 1024 * 1024, s.peakNonHeapUsedBytes());
        assertEquals(6, s.peakLiveThreads());
        assertEquals(3, s.totalGcCount());
        assertEquals(40, s.totalGcTimeMs());
        assertEquals(12, s.peakOpenFds());
        assertEquals(100, s.maxFds());
        assertEquals(2, s.sampleCount());
        assertEquals(0.6, s.peakCpuLoad(), 1e-9);
        assertEquals(0.4, s.meanCpuLoad(), 1e-9);
    }

    @Test
    void CPU_필드가_없으면_음수로_표시한다() throws IOException {
        Path f = dir.resolve("old.jsonl");
        Files.writeString(f, "{\"heapUsedMB\":1,\"nonHeapUsedMB\":1,\"gcCount\":0,\"gcTimeMs\":0,\"liveThreads\":1}\n");
        JvmMetricsSampler.Summary s = JvmMetricsSampler.summarize(f);
        assertEquals(1, s.sampleCount());
        assertEquals(-1.0, s.peakCpuLoad(), 1e-9);
        assertEquals(-1.0, s.meanCpuLoad(), 1e-9);
    }

    @Test
    void 파일이_없으면_표본_0개의_요약이다() {
        JvmMetricsSampler.Summary s = JvmMetricsSampler.summarize(dir.resolve("none.jsonl"));
        assertEquals(0, s.sampleCount());
    }
}
