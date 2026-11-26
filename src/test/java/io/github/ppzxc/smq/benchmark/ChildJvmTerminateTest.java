package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** 자식 정리는 강제로 끝내고 프로세스가 사라질 때까지 기다린다. 셸 스크립트를 자식으로 쓴다. */
class ChildJvmTerminateTest {

    private static String previousModulePath;

    @org.junit.jupiter.api.BeforeAll
    static void setModulePathForChildJvm() {
        previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
    }

    @org.junit.jupiter.api.AfterAll
    static void restoreModulePath() {
        if (previousModulePath == null) System.clearProperty("jdk.module.path");
        else System.setProperty("jdk.module.path", previousModulePath);
    }

    @Test
    void 강제_종료_뒤_프로세스가_사라졌다() throws Exception {
        assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("windows"), "셸이 필요하다");
        Process p = new ProcessBuilder("sh", "-c", "trap '' TERM; echo ready; while true; do sleep 0.05; done").start();
        p.getInputStream().read(); // trap이 설치되기를 기다린다("ready" 한 글자)

        ChildJvm.terminate(p);

        assertFalse(p.isAlive());
    }

    @Test
    void 대기_스레드가_인터럽트될_때_자식이_이미_종료됐다() throws Exception {
        Path dir = Files.createTempDirectory("smq-child-interrupt");
        Path pidFile = dir.resolve("child.pid");
        Path output = dir.resolve("child.out");
        ChildJvm child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "test");
        long pid;
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertTrue(Files.exists(pidFile) && Files.size(pidFile) > 0, "child should publish its pid");
            pid = Long.parseLong(Files.readString(pidFile));

            AtomicReference<Throwable> result = new AtomicReference<>();
            AtomicBoolean aliveWhenInterrupted = new AtomicBoolean(true);
            Thread waiter = new Thread(() -> {
                try {
                    child.await(60_000);
                } catch (Throwable e) {
                    result.set(e);
                    aliveWhenInterrupted.set(
                            ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
                }
            });
            waiter.start();
            Thread.sleep(50);
            waiter.interrupt();
            waiter.join(10_000);

            assertFalse(waiter.isAlive(), "interrupted wait should return");
            assertInstanceOf(InterruptedException.class, result.get());
            assertFalse(aliveWhenInterrupted.get(), "child should be gone when interruption reaches the caller");
        } finally {
            if (Files.exists(pidFile) && Files.size(pidFile) > 0) {
                ProcessHandle.of(Long.parseLong(Files.readString(pidFile))).ifPresent(process -> {
                    process.destroyForcibly();
                    process.onExit().join();
                });
            }
            Files.deleteIfExists(pidFile);
            Files.deleteIfExists(output);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    void 정상_종료와_실패_종료를_구분한다() throws Exception {
        Path dir = Files.createTempDirectory("smq-child-exit");
        Path output = dir.resolve("child.out");
        try {
            ChildJvm successful = ChildJvm.start(ExitChild.class.getName(), List.of("0"), output, "success");
            successful.await(5_000);

            ChildJvm failed = ChildJvm.start(ExitChild.class.getName(), List.of("7"), output, "failure");
            IllegalStateException error = assertThrows(IllegalStateException.class, () -> failed.await(5_000));
            assertTrue(error.getMessage().contains("종료 코드 7"));
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    void 대기_한도를_넘으면_자식이_종료된_뒤_예외를_낸다() throws Exception {
        Path dir = Files.createTempDirectory("smq-child-timeout");
        Path pidFile = dir.resolve("child.pid");
        Path output = dir.resolve("child.out");
        ChildJvm child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "timeout");
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertTrue(Files.exists(pidFile) && Files.size(pidFile) > 0, "child should publish its pid");
            long pid = Long.parseLong(Files.readString(pidFile));

            IllegalStateException error = assertThrows(IllegalStateException.class, () -> child.await(1));

            assertTrue(error.getMessage().contains("제한 시간"));
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
        } finally {
            if (Files.exists(pidFile) && Files.size(pidFile) > 0) {
                ProcessHandle.of(Long.parseLong(Files.readString(pidFile))).ifPresent(process -> {
                    process.destroyForcibly();
                    process.onExit().join();
                });
            }
            Files.deleteIfExists(pidFile);
            Files.deleteIfExists(output);
            Files.deleteIfExists(dir);
        }
    }

    public static final class WaitingChild {

        private WaitingChild() {}

        public static void main(String[] args) throws Exception {
            Path pidFile = Path.of(args[0]);
            Files.createFile(pidFile);
            Thread.sleep(50);
            Files.writeString(pidFile, Long.toString(ProcessHandle.current().pid()));
            while (true) Thread.sleep(1_000);
        }
    }

    public static final class ExitChild {

        private ExitChild() {}

        public static void main(String[] args) {
            System.exit(Integer.parseInt(args[0]));
        }
    }
}
