package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.PolicyException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JvmMetricsSamplerTest {

    @TempDir
    Path dir;

    @Test
    @PolicyException(
            rule = "PMD.UnusedLocalVariable",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UnusedLocalVariable")
    @DisplayName("표본 줄에 프로세스 CPU가 들어 있다 — process Cpu Sample File Is Not Empty")
    void processCpuSampleFileIsNotEmpty() throws Exception {
        final List<String> lines;
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                        instanceof com.sun.management.OperatingSystemMXBean,
                "이 플랫폼은 프로세스 CPU를 주지 않는다");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            final long end;
            end = System.nanoTime() + 400_000_000L;
            while (System.nanoTime() < end) {
                Thread.onSpinWait();
            }
        }
        lines = Files.readAllLines(dir.resolve("jvm-metrics.jsonl"));
        assertFalse(lines.isEmpty(), "Lines should not be empty.");
    }

    @Test
    @PolicyException(
            rule = "PMD.UnusedLocalVariable",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UnusedLocalVariable")
    @DisplayName("표본 줄에 프로세스 CPU가 들어 있다 — process Cpu Sample Contains Load")
    void processCpuSampleContainsLoad() throws Exception {
        final List<String> lines;
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                        instanceof com.sun.management.OperatingSystemMXBean,
                "이 플랫폼은 프로세스 CPU를 주지 않는다");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            final long end;
            end = System.nanoTime() + 400_000_000L;
            while (System.nanoTime() < end) {
                Thread.onSpinWait();
            }
        }
        lines = Files.readAllLines(dir.resolve("jvm-metrics.jsonl"));
        assertTrue(
                lines.stream().anyMatch(l -> l.contains("\"processCpuLoad\"")), "CPU 필드가 있어야 한다: " + lines.getFirst());
    }

    @Test
    @PolicyException(
            rule = "PMD.UnusedLocalVariable",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UnusedLocalVariable")
    @DisplayName("표본 줄에 프로세스 CPU가 들어 있다 — process Cpu Sample Contains Cpu Time")
    void processCpuSampleContainsCpuTime() throws Exception {
        final List<String> lines;
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                        instanceof com.sun.management.OperatingSystemMXBean,
                "이 플랫폼은 프로세스 CPU를 주지 않는다");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            final long end;
            end = System.nanoTime() + 400_000_000L;
            while (System.nanoTime() < end) {
                Thread.onSpinWait();
            }
        }
        lines = Files.readAllLines(dir.resolve("jvm-metrics.jsonl"));
        assertTrue(lines.getFirst().contains("\"processCpuTimeMs\""), "Lines should contain the expected value.");
    }

    private static Arguments metric(
            final String name, final ToDoubleFunction<JvmMetricsSampler.Summary> value, final double expected) {
        return Arguments.of(name, value, expected);
    }

    private static Stream<Arguments> summaryMetrics() {
        return Stream.of(
                metric("peak heap", JvmMetricsSampler.Summary::peakHeapUsedBytes, 30L * 1024 * 1024),
                metric("peak non-heap", JvmMetricsSampler.Summary::peakNonHeapUsedBytes, 7L * 1024 * 1024),
                metric("peak threads", JvmMetricsSampler.Summary::peakLiveThreads, 6),
                metric("GC count", JvmMetricsSampler.Summary::totalGcCount, 3),
                metric("GC time", JvmMetricsSampler.Summary::totalGcTimeMs, 40),
                metric("peak descriptors", JvmMetricsSampler.Summary::peakOpenFds, 12),
                metric("maximum descriptors", JvmMetricsSampler.Summary::maxFds, 100),
                metric("sample count", JvmMetricsSampler.Summary::sampleCount, 2),
                metric("peak CPU", JvmMetricsSampler.Summary::peakCpuLoad, 0.6),
                metric("mean CPU", JvmMetricsSampler.Summary::meanCpuLoad, 0.4));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("summaryMetrics")
    @DisplayName("메트릭 파일에서 요약을 다시 만든다")
    void summaryMetricIsRecreatedFromMetricsFile(
            final String name, final ToDoubleFunction<JvmMetricsSampler.Summary> value, final double expected)
            throws IOException {
        final Path file;
        final JvmMetricsSampler.Summary summary;
        file = dir.resolve("jvm-metrics.jsonl");
        Files.writeString(file, """
                {"heapUsedMB":10,"nonHeapUsedMB":5,"gcCount":1,"gcTimeMs":10,"liveThreads":4,"openFds":8,"maxFds":100,"processCpuLoad":0.2}
                {"heapUsedMB":30,"nonHeapUsedMB":7,"gcCount":3,"gcTimeMs":40,"liveThreads":6,"openFds":12,"maxFds":100,"processCpuLoad":0.6}
                """);
        summary = JvmMetricsSampler.summarize(file);
        assertEquals(expected, value.applyAsDouble(summary), 1e-9, name + " should match the expected value.");
    }

    @Test
    @DisplayName("CPU 필드가 없으면 음수로 표시한다 — missing Cpu Fields Preserve Sample Count")
    void missingCpuFieldsPreserveSampleCount() throws IOException {
        final Path f;
        final JvmMetricsSampler.Summary s;
        f = dir.resolve("old.jsonl");
        Files.writeString(f, "{\"heapUsedMB\":1,\"nonHeapUsedMB\":1,\"gcCount\":0,\"gcTimeMs\":0,\"liveThreads\":1}\n");
        s = JvmMetricsSampler.summarize(f);
        assertEquals(1, s.sampleCount(), "Sample count should match the expected value.");
    }

    @Test
    @DisplayName("CPU 필드가 없으면 음수로 표시한다 — missing Cpu Fields Report Unavailable Peak")
    void missingCpuFieldsReportUnavailablePeak() throws IOException {
        final Path f;
        final JvmMetricsSampler.Summary s;
        f = dir.resolve("old.jsonl");
        Files.writeString(f, "{\"heapUsedMB\":1,\"nonHeapUsedMB\":1,\"gcCount\":0,\"gcTimeMs\":0,\"liveThreads\":1}\n");
        s = JvmMetricsSampler.summarize(f);
        assertEquals(-1.0, s.peakCpuLoad(), 1e-9, "Peak cpu load should match the expected value.");
    }

    @Test
    @DisplayName("CPU 필드가 없으면 음수로 표시한다 — missing Cpu Fields Report Unavailable Mean")
    void missingCpuFieldsReportUnavailableMean() throws IOException {
        final Path f;
        final JvmMetricsSampler.Summary s;
        f = dir.resolve("old.jsonl");
        Files.writeString(f, "{\"heapUsedMB\":1,\"nonHeapUsedMB\":1,\"gcCount\":0,\"gcTimeMs\":0,\"liveThreads\":1}\n");
        s = JvmMetricsSampler.summarize(f);
        assertEquals(-1.0, s.meanCpuLoad(), 1e-9, "Mean cpu load should match the expected value.");
    }

    @Test
    @DisplayName("파일이 없으면 표본 0개의 요약이다")
    void ifThereIsNoFileItIsASummaryOf0Samples() {
        final JvmMetricsSampler.Summary s;
        s = JvmMetricsSampler.summarize(dir.resolve("none.jsonl"));
        assertEquals(0, s.sampleCount(), "Sample count should match the expected value.");
    }

    @Test
    @DisplayName("proc status의 VmRSS를 바이트로 읽는다 — valid VmRSS")
    void vmRssIsConvertedFromKilobytesToBytes() {
        assertEquals(
                123_456L * 1024,
                JvmMetricsSampler.parseVmRssBytes(
                        List.of("Name:\tjava", "VmPeak:\t  999999 kB", "VmRSS:\t  123456 kB", "Threads:\t12")),
                "Parse vm rss bytes should match the expected value.");
    }

    @Test
    @DisplayName("proc status의 VmRSS를 바이트로 읽는다 — missing VmRSS")
    void missingVmRssReturnsUnavailable() {
        assertEquals(
                -1,
                JvmMetricsSampler.parseVmRssBytes(List.of("Name:\tjava", "Threads:\t12")),
                "Parse vm rss bytes should match the expected value.");
    }

    @Test
    @DisplayName("proc status의 VmRSS를 바이트로 읽는다 — invalid VmRSS")
    void invalidVmRssReturnsUnavailable() {
        assertEquals(
                -1,
                JvmMetricsSampler.parseVmRssBytes(List.of("VmRSS:\t  많이 kB")),
                "Parse vm rss bytes should match the expected value.");
    }

    @Test
    @DisplayName("proc status의 VmRSS를 바이트로 읽는다 — empty status")
    void emptyProcStatusReturnsUnavailable() {
        assertEquals(
                -1,
                JvmMetricsSampler.parseVmRssBytes(List.of()),
                "Parse vm rss bytes should match the expected value.");
    }

    @Test
    @DisplayName("요약에 상주 메모리 최댓값이 들어간다 — summary Reports Peak Resident Memory")
    void summaryReportsPeakResidentMemory() throws IOException {
        final Path f;
        f = dir.resolve("rss.jsonl");
        Files.writeString(f, """
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":100.5}
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":300}
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":200}
            """);
        assertEquals(
                300L * 1024 * 1024,
                JvmMetricsSampler.summarize(f).peakRssBytes(),
                "Peak rss bytes should match the expected value.");
    }

    @Test
    @DisplayName("요약에 상주 메모리 최댓값이 들어간다 — empty Summary Reports Unavailable Resident Memory")
    void emptySummaryReportsUnavailableResidentMemory() throws IOException {
        final Path f;
        f = dir.resolve("rss.jsonl");
        Files.writeString(f, """
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":100.5}
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":300}
            {"heapUsedMB":1,"nonHeapUsedMB":1,"gcCount":0,"gcTimeMs":0,"liveThreads":1,"rssMB":200}
            """);
        assertEquals(-1, JvmMetricsSampler.summarize(dir.resolve("none.jsonl")).peakRssBytes(), "표본이 없으면 모른다");
    }

    @Test
    @PolicyException(
            rule = "PMD.UnusedLocalVariable",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UnusedLocalVariable")
    @DisplayName("이 플랫폼이 리눅스면 표본에 실제 RSS가 있다 — linux Sample Contains Resident Memory")
    void linuxSampleContainsResidentMemory() throws Exception {
        final Path file;
        final String line;
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(Path.of("/proc/self/status")));
        file = dir.resolve("jvm-metrics.jsonl");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            for (int i = 0;
                    i < 200 && (!Files.exists(file) || Files.readAllLines(file).isEmpty());
                    i++) {
                Thread.sleep(25);
            }
        }
        line = Files.readAllLines(file).getFirst();
        assertTrue(line.contains("\"rssMB\""), line);
    }

    @Test
    @PolicyException(
            rule = "PMD.UnusedLocalVariable",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UnusedLocalVariable")
    @DisplayName("이 플랫폼이 리눅스면 표본에 실제 RSS가 있다 — linux Summary Reports Positive Resident Memory")
    void linuxSummaryReportsPositiveResidentMemory() throws Exception {
        final Path file;
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(Path.of("/proc/self/status")));
        file = dir.resolve("jvm-metrics.jsonl");
        try (JvmMetricsSampler s = JvmMetricsSampler.start(dir, 30)) {
            for (int i = 0;
                    i < 200 && (!Files.exists(file) || Files.readAllLines(file).isEmpty());
                    i++) {
                Thread.sleep(25);
            }
        }
        assertTrue(JvmMetricsSampler.summarize(file).peakRssBytes() > 0, "상주 메모리는 0보다 크다");
    }
}
