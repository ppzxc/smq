package io.github.ppzxc.smq.benchmark.random;

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
            int sampleCount) {

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("peakHeapUsedMB", peakHeapUsedBytes / (1024.0 * 1024.0));
            m.put("peakNonHeapUsedMB", peakNonHeapUsedBytes / (1024.0 * 1024.0));
            m.put("peakLiveThreads", peakLiveThreads);
            m.put("totalGcCount", totalGcCount);
            m.put("totalGcTimeMs", totalGcTimeMs);
            m.put("peakOpenFds", peakOpenFds);
            m.put("maxFds", maxFds);
            m.put("sampleCount", sampleCount);
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

    // MXBeans (불변)
    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    private JvmMetricsSampler(Path file, long intervalMs) {
        this.file = file;
        this.intervalMs = intervalMs;
        this.thread = new Thread(this::loop, "jvm-metrics-sampler");
        this.thread.setDaemon(true);
    }

    /** 기본 5초 간격으로 표본한다. */
    static JvmMetricsSampler start(Path dir) {
        return start(dir, DEFAULT_INTERVAL_MS);
    }

    static JvmMetricsSampler start(Path dir, long intervalMs) {
        Path file = dir.resolve("jvm-metrics.jsonl");
        JvmMetricsSampler s = new JvmMetricsSampler(file, intervalMs);
        s.thread.start();
        return s;
    }

    /** 자식 JVM용. 프로세스 번호를 파일 이름에 반영한다. */
    static JvmMetricsSampler startChild(Path dir, int process) {
        Path file = dir.resolve("p" + process + "-jvm-metrics.jsonl");
        JvmMetricsSampler s = new JvmMetricsSampler(file, DEFAULT_INTERVAL_MS);
        s.thread.start();
        return s;
    }

    Summary summary() {
        long totalGcCount = 0;
        long totalGcTime = 0;
        for (GarbageCollectorMXBean gc : gcBeans) {
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
                sampleCount);
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

    private void sample() {
        long now = System.currentTimeMillis();
        MemoryUsage heap = memory.getHeapMemoryUsage();
        MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();
        int liveThreads = threads.getThreadCount();

        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gc : gcBeans) {
            long c = gc.getCollectionCount();
            long t = gc.getCollectionTime();
            if (c >= 0) gcCount += c;
            if (t >= 0) gcTime += t;
        }

        long openFds = -1;
        long mxFds = -1;
        try {
            // com.sun.management.UnixOperatingSystemMXBean (Linux/Mac)
            var os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
                openFds = unix.getOpenFileDescriptorCount();
                mxFds = unix.getMaxFileDescriptorCount();
            }
        } catch (Exception ignored) {
            // Windows 등에서는 지원하지 않는다
        }

        long rssBytes = -1;
        try {
            var os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
                // JDK 14+: getProcessCpuLoad 대신 committedVirtualMemorySize로 RSS 근사
                rssBytes = sunOs.getCommittedVirtualMemorySize();
            }
        } catch (Exception ignored) {
            // 지원하지 않는 플랫폼
        }

        // 피크 갱신
        peakHeapUsed = Math.max(peakHeapUsed, heap.getUsed());
        peakNonHeapUsed = Math.max(peakNonHeapUsed, nonHeap.getUsed());
        peakThreads = Math.max(peakThreads, liveThreads);
        if (openFds > 0) peakOpenFds = Math.max(peakOpenFds, openFds);
        if (mxFds > 0) maxFds = mxFds;
        sampleCount++;

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("timestampMs", now);
        m.put("heapUsedMB", round2(heap.getUsed() / (1024.0 * 1024.0)));
        m.put("heapMaxMB", round2(heap.getMax() / (1024.0 * 1024.0)));
        m.put("nonHeapUsedMB", round2(nonHeap.getUsed() / (1024.0 * 1024.0)));
        m.put("gcCount", gcCount);
        m.put("gcTimeMs", gcTime);
        m.put("liveThreads", liveThreads);
        if (openFds >= 0) m.put("openFds", openFds);
        if (mxFds >= 0) m.put("maxFds", mxFds);
        if (rssBytes >= 0) m.put("rssMB", round2(rssBytes / (1024.0 * 1024.0)));

        try {
            Files.writeString(file, Json.write(m) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
