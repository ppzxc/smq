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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SqliteMultiProcessTest}보다 훨씬 많은 JVM과 메시지로 WAL 공유 메모리와 파일 잠금을 압박한다(#59).
 * 작업자 여럿이 같은 순간에 DB를 열고(스키마 생성 경합), 발행과 꺼내기가 겹치며, 작업자가 강제 종료돼
 * 잠금을 쥔 채 죽어도 나머지가 계속 진행해야 한다. 기준은 중복 전달 없음과 유실 없음이다.
 */
class SqliteMultiProcessStressTest {

    @TempDir
    Path dir;

    private final List<Worker> workers = new ArrayList<>();

    @AfterEach
    void killWorkers() {
        workers.forEach(w -> w.process.destroyForcibly());
    }

    private final class Worker {
        final String name;
        final Process process;
        final ConcurrentLinkedQueue<String> lines = new ConcurrentLinkedQueue<>();
        final Thread reader;

        Worker(String name, int count, int leaseSeconds) throws IOException {
            this.name = name;
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED",
                    "-cp", System.getProperty("java.class.path"), MultiProcessWorker.class.getName(),
                    dir.resolve("smq.db").toString(), name, String.valueOf(count), "pull",
                    String.valueOf(leaseSeconds))
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
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
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "작업자가 끝나지 않았다");
            reader.join(5000);
        }
    }

    private static Set<String> expected(int workers, int perWorker) {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < workers; i++) {
            for (int j = 0; j < perWorker; j++) {
                expected.add("w" + i + "-" + j);
            }
        }
        return expected;
    }

    private List<String> got(Worker... from) {
        List<String> got = new ArrayList<>();
        for (Worker w : from) {
            assertFalse(w.lines.contains("SETTLE_FAILED"), w.name + ": 정산 실패");
            w.lines.stream().filter(l -> l.startsWith("GOT ")).map(l -> l.substring(4)).forEach(got::add);
        }
        return got;
    }

    @Test
    void 열두_JVM이_동시에_열고_발행하고_꺼내도_중복도_유실도_없다() throws Exception {
        int count = 12;
        int perWorker = 500;
        for (int i = 0; i < count; i++) {
            new Worker("w" + i, perWorker, 120);
        }
        long total = (long) count * perWorker;
        long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
        while (workers.stream().mapToLong(Worker::received).sum() < total) {
            assertTrue(System.nanoTime() < deadline, "시간 안에 다 꺼내지 못했다");
            Thread.sleep(100);
        }
        for (Worker w : workers) {
            w.stop();
        }
        List<String> got = got(workers.toArray(new Worker[0]));
        assertEquals(total, got.size(), "이중 리스가 있으면 총 개수가 넘친다");
        assertEquals(expected(count, perWorker), new HashSet<>(got));
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db")).open()) {
            assertEquals(new QueueCounts(0, 0, 0), smq.queue("q").counts());
        }
    }

    /**
     * 작업자 하나를 한창 꺼내는 중에 강제 종료한다. 쓰기 잠금이나 WAL 공유 메모리 잠금을 쥔 채 죽어도 나머지는
     * 막히지 않아야 하고, 죽은 작업자가 정산하지 못한 메시지는 리스가 끝난 뒤 나머지가 받아야 한다. 죽은 작업자가
     * 정산은 했지만 출력하지 못한 메시지(한 번에 꺼내는 5개 이하)만 유실로 허용한다.
     */
    @Test
    void 작업자가_강제_종료돼도_나머지는_계속_진행하고_중복도_유실도_없다() throws Exception {
        int count = 6;
        int perWorker = 400;
        for (int i = 0; i < count; i++) {
            new Worker("w" + i, perWorker, 3);
        }
        Worker victim = workers.get(0);
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (victim.received() < 20) {
            assertTrue(System.nanoTime() < deadline, "희생 작업자가 꺼내기 시작하지 않았다");
            Thread.sleep(10);
        }
        victim.process.destroyForcibly();
        assertTrue(victim.process.waitFor(30, TimeUnit.SECONDS));
        victim.reader.join(5000);

        Worker[] survivors = workers.subList(1, count).toArray(new Worker[0]);
        Set<String> expected = expected(count, perWorker);
        long target = expected.size() - 5;
        deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
        while (true) {
            Set<String> seen = new HashSet<>(got(workers.toArray(new Worker[0])));
            if (seen.size() >= target) {
                break;
            }
            assertTrue(System.nanoTime() < deadline, "나머지가 시간 안에 진행하지 못했다: " + seen.size() + "/" + expected.size());
            Thread.sleep(100);
        }
        // 정산 못 한 메시지의 리스가 끝나 다시 나올 시간을 준 뒤 멈춘다.
        Thread.sleep(5000);
        for (Worker w : survivors) {
            w.stop();
        }
        List<String> got = got(workers.toArray(new Worker[0]));
        Set<String> unique = new HashSet<>(got);
        assertTrue(expected.containsAll(unique), "발행하지 않은 메시지가 나왔다");
        // 죽은 작업자가 정산한 메시지는 다시 나오지 않으므로 중복은 정산 뒤 출력 전에 죽은 경우와 구분할 수 없다.
        // 정산은 출력보다 먼저이므로 중복(이중 리스)은 없어야 한다.
        assertEquals(unique.size(), got.size(), "이중 전달이 있다");
        assertTrue(expected.size() - unique.size() <= 5, "유실: " + (expected.size() - unique.size()));
        try (Smq smq = Smq.sqlite(dir.resolve("smq.db")).open()) {
            assertEquals(new QueueCounts(0, 0, 0), smq.queue("q").counts());
        }
    }
}
