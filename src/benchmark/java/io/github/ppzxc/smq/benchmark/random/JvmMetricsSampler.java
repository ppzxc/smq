package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.PolicyException;
import io.github.ppzxc.smq.benchmark.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JVM 메트릭(힙·GC·스레드·파일 디스크립터)을 주기적으로 표본하여 jsonl 파일에 남기는 데몬. {@link AutoCloseable}이라 try-with-resources로
 * 쓴다. 닫으면 표본을 멈추고 누적 요약({@link Summary})을 돌려준다.
 *
 * <p>오버헤드가 극히 작아(MXBean 읽기 + 파일 한 줄 append) 장시간 테스트에서 상시 켜도 측정에 영향을 주지 않는다.
 */
final class JvmMetricsSampler implements AutoCloseable {

    /** 닫은 뒤 읽을 수 있는 누적 요약. */
    record Summary(
            long peakHeapUsedBytes,
            long peakNonHeapUsedBytes,
            long peakLiveThreads,
            long totalGcCount,
            long totalGcTimeMs,
            long peakOpenFds,
            long maxFds,
            int sampleCount,
            double peakCpuLoad,
            double meanCpuLoad,
            long peakRssBytes) {

        Map<String, Object> toMap() {
            final Map<String, Object> m;
            m = new LinkedHashMap<>();
            m.put("peakHeapUsedMB", peakHeapUsedBytes / (1024.0 * 1024.0));
            m.put("peakNonHeapUsedMB", peakNonHeapUsedBytes / (1024.0 * 1024.0));
            m.put("peakLiveThreads", peakLiveThreads);
            m.put("totalGcCount", totalGcCount);
            m.put("totalGcTimeMs", totalGcTimeMs);
            m.put("peakOpenFds", peakOpenFds);
            m.put("maxFds", maxFds);
            m.put("sampleCount", sampleCount);
            m.put("peakCpuLoad", peakCpuLoad);
            m.put("meanCpuLoad", meanCpuLoad);
            m.put("peakRssMB", peakRssBytes < 0 ? null : peakRssBytes / (1024.0 * 1024.0));
            return m;
        }
    }

    private static final long DEFAULT_INTERVAL_MS = 5_000;

    private final Path file;
    private final long intervalMs;
    private final Thread thread;
    private volatile boolean running = true;

    // 누적 피크
    private long peakHeapUsed;
    private long peakNonHeapUsed;
    private long peakThreads;
    private long peakOpenFds;
    private long maxFds;
    private int sampleCount;
    private double peakCpuLoad = -1;
    private double cpuLoadSum;
    private int cpuSamples;
    private long peakRssBytes = -1;

    // MXBeans (불변)
    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    private JvmMetricsSampler(final Path file, final long intervalMs) {
        this.file = file;
        this.intervalMs = intervalMs;
        this.thread = new Thread(this::loop, "jvm-metrics-sampler");
        this.thread.setDaemon(true);
    }

    /** 기본 5초 간격으로 표본한다. */
    static JvmMetricsSampler start(final Path dir) {
        return start(dir, DEFAULT_INTERVAL_MS);
    }

    static JvmMetricsSampler start(final Path dir, final long intervalMs) {
        final Path file;
        final JvmMetricsSampler s;
        file = dir.resolve("jvm-metrics.jsonl");
        s = new JvmMetricsSampler(file, intervalMs);
        s.thread.start();
        return s;
    }

    Summary summary() {
        long totalGcCount;
        long totalGcTime;
        totalGcCount = 0;
        totalGcTime = 0;
        for (final GarbageCollectorMXBean gc : gcBeans) {
            totalGcCount += gc.getCollectionCount();
            totalGcTime += gc.getCollectionTime();
        }
        return new Summary(
                peakHeapUsed,
                peakNonHeapUsed,
                peakThreads,
                totalGcCount,
                totalGcTime,
                peakOpenFds,
                maxFds,
                sampleCount,
                cpuSamples == 0 ? -1 : peakCpuLoad,
                cpuSamples == 0 ? -1 : cpuLoadSum / cpuSamples,
                peakRssBytes);
    }

    /**
     * 메트릭 파일 하나에서 요약을 다시 만든다. 파일이 없으면
     * 표본 0개의 요약이다. GC 횟수와 시간은 마지막 표본의 누적값이다. CPU 필드가 없으면 -1이다.
     */
    @PolicyException(
            rule = "PMD.CyclomaticComplexity",
            reason = "The summary aggregates independent optional fields while reading each metrics sample once.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.CyclomaticComplexity")
    static Summary summarize(final Path file) {
        final List<String> lines;
        long peakHeap;
        long peakNonHeap;
        long peakThreads;
        long gcCount;
        long gcTime;
        long peakFds;
        long maxFds;
        int n;
        double peakCpu;
        double cpuSum;
        int cpuN;
        long peakRss;
        peakHeap = 0;
        peakNonHeap = 0;
        peakThreads = 0;
        gcCount = 0;
        gcTime = 0;
        peakFds = 0;
        maxFds = 0;
        n = 0;
        peakCpu = -1;
        cpuSum = 0;
        cpuN = 0;
        peakRss = -1;

        try {
            lines = Files.exists(file) ? Files.readAllLines(file) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (final String line : lines) {
            final Map<?, ?> m;
            if (line.isBlank()) {
                continue;
            }
            m = (Map<?, ?>) Json.parse(line);
            n++;
            peakHeap = Math.max(peakHeap, megabytes(m.get("heapUsedMB")));
            peakNonHeap = Math.max(peakNonHeap, megabytes(m.get("nonHeapUsedMB")));
            peakThreads = Math.max(peakThreads, longOf(m.get("liveThreads")));
            gcCount = longOf(m.get("gcCount"));
            gcTime = longOf(m.get("gcTimeMs"));
            peakFds = Math.max(peakFds, longOf(m.get("openFds")));
            maxFds = Math.max(maxFds, longOf(m.get("maxFds")));
            if (m.get("rssMB") instanceof Number rss) {
                peakRss = Math.max(peakRss, megabytes(rss));
            }
            if (m.get("processCpuLoad") instanceof Number cpu && cpu.doubleValue() >= 0) {
                peakCpu = Math.max(peakCpu, cpu.doubleValue());
                cpuSum += cpu.doubleValue();
                cpuN++;
            }
        }
        return new Summary(
                peakHeap,
                peakNonHeap,
                peakThreads,
                gcCount,
                gcTime,
                peakFds,
                maxFds,
                n,
                peakCpu,
                cpuN == 0 ? -1 : cpuSum / cpuN,
                peakRss);
    }

    private static long megabytes(final Object v) {
        return v instanceof Number x ? Math.round(x.doubleValue() * 1024 * 1024) : 0;
    }

    private static long longOf(final Object v) {
        return v instanceof Number x ? x.longValue() : 0;
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(3_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        while (running) {
            try {
                sample();
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "Platform-specific management beans are optional; unsupported metrics must not fail the workload.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void sample() {
        final long now;
        final MemoryUsage heap;
        final MemoryUsage nonHeap;
        final int liveThreads;
        final GcTotals gc;
        long openFds;
        long mxFds;
        final long rssBytes;
        double cpuLoad;
        long cpuTimeMs;
        final Map<String, Object> m;
        now = System.currentTimeMillis();
        heap = memory.getHeapMemoryUsage();
        nonHeap = memory.getNonHeapMemoryUsage();
        liveThreads = threads.getThreadCount();

        gc = gcTotals();

        openFds = -1;
        mxFds = -1;
        try {
            // com.sun.management.UnixOperatingSystemMXBean (Linux/Mac)
            final var os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
                openFds = unix.getOpenFileDescriptorCount();
                mxFds = unix.getMaxFileDescriptorCount();
            }
        } catch (Exception ignored) {
            // Windows 등에서는 지원하지 않는다
        }

        rssBytes = readRssBytes();
        peakRssBytes = Math.max(peakRssBytes, rssBytes);

        cpuLoad = -1;
        cpuTimeMs = -1;
        try {
            if (ManagementFactory.getOperatingSystemMXBean()
                    instanceof com.sun.management.OperatingSystemMXBean sunOs) {
                cpuLoad = sunOs.getProcessCpuLoad(); // 직전 호출 이후 평균. 첫 표본은 음수일 수 있다
                cpuTimeMs = sunOs.getProcessCpuTime() / 1_000_000L;
            }
        } catch (Exception ignored) {
            // 지원하지 않는 플랫폼
        }
        updateOptionalPeaks(cpuLoad, openFds, mxFds);

        // 피크 갱신
        peakHeapUsed = Math.max(peakHeapUsed, heap.getUsed());
        peakNonHeapUsed = Math.max(peakNonHeapUsed, nonHeap.getUsed());
        peakThreads = Math.max(peakThreads, liveThreads);
        sampleCount++;

        m = new LinkedHashMap<>();
        m.put("timestampMs", now);
        m.put("heapUsedMB", round2(heap.getUsed() / (1024.0 * 1024.0)));
        m.put("heapMaxMB", round2(heap.getMax() / (1024.0 * 1024.0)));
        m.put("nonHeapUsedMB", round2(nonHeap.getUsed() / (1024.0 * 1024.0)));
        m.put("gcCount", gc.count());
        m.put("gcTimeMs", gc.timeMillis());
        m.put("liveThreads", liveThreads);
        addOptionalMetrics(m, openFds, mxFds, rssBytes, cpuLoad, cpuTimeMs);

        try {
            Files.writeString(file, Json.write(m) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record GcTotals(long count, long timeMillis) {}

    private GcTotals gcTotals() {
        long count;
        long time;
        count = 0;
        time = 0;
        for (final GarbageCollectorMXBean gc : gcBeans) {
            final long c;
            final long t;
            c = gc.getCollectionCount();
            t = gc.getCollectionTime();
            if (c >= 0) count += c;
            if (t >= 0) time += t;
        }
        return new GcTotals(count, time);
    }

    private void updateOptionalPeaks(final double cpuLoad, final long openFds, final long mxFds) {
        if (cpuLoad >= 0) {
            peakCpuLoad = Math.max(peakCpuLoad, cpuLoad);
            cpuLoadSum += cpuLoad;
            cpuSamples++;
        }
        if (openFds > 0) peakOpenFds = Math.max(peakOpenFds, openFds);
        if (mxFds > 0) maxFds = mxFds;
    }

    private static void addOptionalMetrics(
            final Map<String, Object> m,
            final long openFds,
            final long mxFds,
            final long rssBytes,
            final double cpuLoad,
            final long cpuTimeMs) {
        if (openFds >= 0) m.put("openFds", openFds);
        if (mxFds >= 0) m.put("maxFds", mxFds);
        if (rssBytes >= 0) m.put("rssMB", round2(rssBytes / (1024.0 * 1024.0)));
        if (cpuLoad >= 0) m.put("processCpuLoad", round4(cpuLoad)); // 코어가 많으면 0.01 단위는 너무 거칠다
        if (cpuTimeMs >= 0) m.put("processCpuTimeMs", cpuTimeMs);
    }

    /** 이 프로세스의 상주 메모리(바이트). Linux의 {@code /proc/self/status}에서만 읽고, 못 읽으면 -1이다. */
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason =
                    "Unavailable or malformed platform metrics are reported as unknown instead of failing the benchmark.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    static long readRssBytes() {
        try {
            return parseVmRssBytes(Files.readAllLines(Path.of("/proc/self/status")));
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    static long parseVmRssBytes(final List<String> statusLines) {
        for (final String line : statusLines) {
            if (line.startsWith("VmRSS:")) {
                try {
                    return Long.parseLong(line.substring("VmRSS:".length())
                                    .replace("kB", "")
                                    .trim())
                            * 1024;
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private static double round4(final double v) {
        return Math.round(v * 10_000.0) / 10_000.0;
    }

    private static double round2(final double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
