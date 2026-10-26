package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 같은 SQLite 파일을 여러 JVM이 공개 API로 함께 쓸 때의 안전성. */
class SqliteMultiProcessTest {

    private static final int WORKERS = 3;
    private static final int PER_WORKER = 60;

    @TempDir
    Path dir;

    private final List<Worker> workers = new ArrayList<>();

    @AfterEach
    void killWorkers() {
        workers.forEach(w -> w.process.destroyForcibly());
    }

    /** 별도 JVM 하나. 표준 출력을 줄 단위로 모은다. */
    private final class Worker {
        final Process process;
        final ConcurrentLinkedQueue<String> lines = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> gotAtNanos = new ConcurrentLinkedQueue<>();
        final CountDownLatch ready = new CountDownLatch(1);
        final Thread reader;

        Worker(String name, int count, String mode) throws IOException {
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED",
                    "-cp", System.getProperty("java.class.path"), MultiProcessWorker.class.getName(),
                    dir.resolve("smq.db").toString(), name, String.valueOf(count), mode)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.equals("READY")) {
                            ready.countDown();
                        } else if (line.startsWith("GOT ")) {
                            gotAtNanos.add(System.nanoTime());
                        }
                        lines.add(line);
                    }
                } catch (IOException ignored) {
                    // 프로세스가 끝났다.
                }
            });
            reader.setDaemon(true);
            reader.start();
            workers.add(this);
        }

        long received() {
            return lines.stream().filter(l -> l.startsWith("GOT ")).count();
        }

        void stop() throws Exception {
            process.getOutputStream().close();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "작업자가 끝나지 않았다");
            reader.join(5000);
        }
    }

    @Test
    void 여러_JVM이_발행하고_꺼내도_메시지가_두_번_묶이거나_사라지지_않는다() throws Exception {
        for (int i = 0; i < WORKERS; i++) {
            new Worker("w" + i, PER_WORKER, "pull");
        }
        int total = WORKERS * PER_WORKER;
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (workers.stream().mapToLong(Worker::received).sum() < total) {
            assertTrue(System.nanoTime() < deadline, "시간 안에 다 꺼내지 못했다");
            Thread.sleep(50);
        }
        // 멈춘 뒤 마지막 출력까지 모아서 중복 여부를 본다.
        for (Worker w : workers) {
            w.stop();
        }
        List<String> got = new ArrayList<>();
        for (Worker w : workers) {
            assertFalse(w.lines.contains("SETTLE_FAILED"));
            w.lines.stream().filter(l -> l.startsWith("GOT ")).map(l -> l.substring(4)).forEach(got::add);
        }
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < WORKERS; i++) {
            for (int j = 0; j < PER_WORKER; j++) {
                expected.add("w" + i + "-" + j);
            }
        }
        assertEquals(total, got.size(), "이중 리스가 있으면 총 개수가 넘친다");
        assertEquals(expected, new HashSet<>(got));
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db")).open()) {
            assertEquals(new QueueCounts(0, 0, 0), smq.queue("q").counts());
        }
    }

    /**
     * 수신 JVM은 긴 꺼내기 하나로 기다려 폴링 간격을 상한(250ms)까지 늘려 둔다. 발행 시각은 수신 JVM이 대기 중임을
     * 확인하고 폴링 간격이 상한에 이를 만큼 기다린 뒤, 발행 직전에 잰다. 그래서 지연에는 JVM 기동이 섞이지 않고,
     * 발행 시간과 폴링 간격 한 번(상한 250ms)과 출력 전달만 남는다. 매 회가 0 이상이며 상한에 같은 크기의 여유를
     * 더한 값 이하여야 한다(최댓값 기준, 중앙값으로 완화하지 않는다). 위상이 달라지도록 회마다 기다림을 조금씩 바꾼다.
     */
    @Test
    void 기다리던_꺼내기가_다른_프로세스의_발행을_폴링_간격_상한_안에_받는다() throws Exception {
        Duration upperBound = Duration.ofMillis(250);
        Duration slack = Duration.ofMillis(250);
        Worker receiver = new Worker("recv", 0, "pull-wait");
        assertTrue(receiver.ready.await(30, TimeUnit.SECONDS), "수신 JVM이 대기에 들어가지 않았다");
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db")).leaseDuration(Duration.ofSeconds(120)).open()) {
            Queue q = smq.declare("q", QueueOptions.defaults());
            for (int round = 0; round < 8; round++) {
                // 폴링 간격이 1ms에서 상한 250ms까지 늘어날 시간(합 약 255ms)을 주고, 위상을 흩뜨린다.
                Thread.sleep(600 + round * 37L);
                assertEquals(round, receiver.gotAtNanos.size());
                long publishStart = System.nanoTime();
                q.publish(OutgoingMessage.of(("r" + round).getBytes(StandardCharsets.UTF_8)));
                long deadline = publishStart + Duration.ofSeconds(10).toNanos();
                while (receiver.gotAtNanos.size() <= round) {
                    assertTrue(System.nanoTime() < deadline, "수신 JVM이 " + round + "번째 메시지를 받지 못했다");
                    Thread.sleep(1);
                }
                long lagMillis = TimeUnit.NANOSECONDS.toMillis(
                        ((Long[]) receiver.gotAtNanos.toArray(new Long[0]))[round] - publishStart);
                assertTrue(lagMillis >= 0, round + "회: 발행보다 먼저 받았다(" + lagMillis + "ms)");
                assertTrue(lagMillis <= upperBound.plus(slack).toMillis(),
                        round + "회: 발행 뒤 " + lagMillis + "ms에 받았다");
            }
        }
    }
}
