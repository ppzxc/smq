package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.github.ppzxc.smq.PolicyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("강제 종료 뒤 프로세스가 사라졌다")
    void theProcessDisappearedAfterAForcedShutdown() throws Exception {
        final Process p;
        assumeFalse(System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows"), "셸이 필요하다");
        p = new ProcessBuilder("sh", "-c", "trap '' TERM; echo ready; while true; do sleep 0.05; done").start();
        p.getInputStream().read();
        ChildJvm.terminate(p);
        assertFalse(p.isAlive(), "Process should not be alive.");
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("대기 스레드가 인터럽트될 때 자식이 이미 종료됐다 — check 1")
    void theChildProcessHadAlreadyTerminatedWhenTheWaitingThreadWasInterrupted() throws Exception {
        final long pid;
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-interrupt");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "test");
        try {
            final long deadline;
            final AtomicReference<Throwable> result;
            final AtomicBoolean aliveWhenInterrupted;
            final Thread waiter;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            assertTrue(assertionValue4Argument1, "child should publish its pid");
            pid = Long.parseLong(Files.readString(pidFile));
            result = new AtomicReference<>();
            aliveWhenInterrupted = new AtomicBoolean(true);
            waiter = new Thread(() -> {
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
            final var assertionValue3Argument1 = waiter.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = aliveWhenInterrupted.get();
            evaluatePmdArgument(assertionValue1Argument1);
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
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("대기 스레드가 인터럽트될 때 자식이 이미 종료됐다 — check 2")
    void theChildProcessHadAlreadyTerminatedWhenTheWaitingThreadWasInterruptedCheck2() throws Exception {
        final long pid;
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-interrupt");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "test");
        try {
            final long deadline;
            final AtomicReference<Throwable> result;
            final AtomicBoolean aliveWhenInterrupted;
            final Thread waiter;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            result = new AtomicReference<>();
            aliveWhenInterrupted = new AtomicBoolean(true);
            waiter = new Thread(() -> {
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
            final var assertionValue3Argument1 = waiter.isAlive();
            assertFalse(assertionValue3Argument1, "interrupted wait should return");
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = aliveWhenInterrupted.get();
            evaluatePmdArgument(assertionValue1Argument1);
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
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("대기 스레드가 인터럽트될 때 자식이 이미 종료됐다 — check 3")
    void theChildProcessHadAlreadyTerminatedWhenTheWaitingThreadWasInterruptedCheck3() throws Exception {
        final long pid;
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-interrupt");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "test");
        try {
            final long deadline;
            final AtomicReference<Throwable> result;
            final AtomicBoolean aliveWhenInterrupted;
            final Thread waiter;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            result = new AtomicReference<>();
            aliveWhenInterrupted = new AtomicBoolean(true);
            waiter = new Thread(() -> {
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
            final var assertionValue3Argument1 = waiter.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            assertInstanceOf(
                    InterruptedException.class,
                    assertionValue2Argument2,
                    "Value should be an instance of InterruptedException.");
            final var assertionValue1Argument1 = aliveWhenInterrupted.get();
            evaluatePmdArgument(assertionValue1Argument1);
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
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("대기 스레드가 인터럽트될 때 자식이 이미 종료됐다 — check 4")
    void theChildProcessHadAlreadyTerminatedWhenTheWaitingThreadWasInterruptedCheck4() throws Exception {
        final long pid;
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-interrupt");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "test");
        try {
            final long deadline;
            final AtomicReference<Throwable> result;
            final AtomicBoolean aliveWhenInterrupted;
            final Thread waiter;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            result = new AtomicReference<>();
            aliveWhenInterrupted = new AtomicBoolean(true);
            waiter = new Thread(() -> {
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
            final var assertionValue3Argument1 = waiter.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = aliveWhenInterrupted.get();
            assertFalse(assertionValue1Argument1, "child should be gone when interruption reaches the caller");
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
    @DisplayName("정상 종료와 실패 종료를 구분한다 — check 1")
    void distinguishBetweenNormalTerminationAndFailedTermination() throws Exception {
        final Path dir;
        final Path output;
        dir = Files.createTempDirectory("smq-child-exit");
        output = dir.resolve("child.out");
        try {
            final ChildJvm successful;
            final ChildJvm failed;
            final IllegalStateException error;
            successful = ChildJvm.start(ExitChild.class.getName(), List.of("0"), output, "success");
            successful.await(5_000);
            failed = ChildJvm.start(ExitChild.class.getName(), List.of("7"), output, "failure");
            final var assertionValue2 = captureThrowable(() -> failed.await(5_000));
            assertInstanceOf(IllegalStateException.class, assertionValue2, "Await should throw IllegalStateException.");
            error = (IllegalStateException) assertionValue2;
            final var assertionValue1Argument1 = error.getMessage().contains("종료 코드 7");
            evaluatePmdArgument(assertionValue1Argument1);
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("정상 종료와 실패 종료를 구분한다 — check 2")
    void distinguishBetweenNormalTerminationAndFailedTerminationCheck2() throws Exception {
        final Path dir;
        final Path output;
        dir = Files.createTempDirectory("smq-child-exit");
        output = dir.resolve("child.out");
        try {
            final ChildJvm successful;
            final ChildJvm failed;
            final IllegalStateException error;
            successful = ChildJvm.start(ExitChild.class.getName(), List.of("0"), output, "success");
            successful.await(5_000);
            failed = ChildJvm.start(ExitChild.class.getName(), List.of("7"), output, "failure");
            final var assertionValue2 = captureThrowable(() -> failed.await(5_000));
            error = (IllegalStateException) assertionValue2;
            final var assertionValue1Argument1 = error.getMessage().contains("종료 코드 7");
            assertTrue(assertionValue1Argument1, "Message should contain the expected value.");
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("대기 한도를 넘으면 자식이 종료된 뒤 예외를 낸다 — check 1")
    void ifTheWaitLimitIsExceededTheChildProcessTerminatesAndThrowsAnException() throws Exception {
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-timeout");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "timeout");
        try {
            final long deadline;
            final long pid;
            final IllegalStateException error;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            assertTrue(assertionValue4Argument1, "child should publish its pid");
            pid = Long.parseLong(Files.readString(pidFile));
            final var assertionValue3 = captureThrowable(() -> child.await(1));
            error = (IllegalStateException) assertionValue3;
            final var assertionValue2Argument1 = error.getMessage().contains("제한 시간");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 =
                    ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
            evaluatePmdArgument(assertionValue1Argument1);
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
    @DisplayName("대기 한도를 넘으면 자식이 종료된 뒤 예외를 낸다 — check 2")
    void ifTheWaitLimitIsExceededTheChildProcessTerminatesAndThrowsAnExceptionCheck2() throws Exception {
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-timeout");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "timeout");
        try {
            final long deadline;
            final long pid;
            final IllegalStateException error;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            final var assertionValue3 = captureThrowable(() -> child.await(1));
            assertInstanceOf(IllegalStateException.class, assertionValue3, "Await should throw IllegalStateException.");
            error = (IllegalStateException) assertionValue3;
            final var assertionValue2Argument1 = error.getMessage().contains("제한 시간");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 =
                    ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
            evaluatePmdArgument(assertionValue1Argument1);
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
    @DisplayName("대기 한도를 넘으면 자식이 종료된 뒤 예외를 낸다 — check 3")
    void ifTheWaitLimitIsExceededTheChildProcessTerminatesAndThrowsAnExceptionCheck3() throws Exception {
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-timeout");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "timeout");
        try {
            final long deadline;
            final long pid;
            final IllegalStateException error;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            final var assertionValue3 = captureThrowable(() -> child.await(1));
            error = (IllegalStateException) assertionValue3;
            final var assertionValue2Argument1 = error.getMessage().contains("제한 시간");
            assertTrue(assertionValue2Argument1, "Message should contain the expected value.");
            final var assertionValue1Argument1 =
                    ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
            evaluatePmdArgument(assertionValue1Argument1);
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
    @DisplayName("대기 한도를 넘으면 자식이 종료된 뒤 예외를 낸다 — check 4")
    void ifTheWaitLimitIsExceededTheChildProcessTerminatesAndThrowsAnExceptionCheck4() throws Exception {
        final Path dir;
        final Path pidFile;
        final Path output;
        final ChildJvm child;
        dir = Files.createTempDirectory("smq-child-timeout");
        pidFile = dir.resolve("child.pid");
        output = dir.resolve("child.out");
        child = ChildJvm.start(WaitingChild.class.getName(), List.of(pidFile.toString()), output, "timeout");
        try {
            final long deadline;
            final long pid;
            final IllegalStateException error;
            deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((!Files.exists(pidFile) || Files.size(pidFile) == 0) && System.nanoTime() < deadline)
                Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(pidFile) && Files.size(pidFile) > 0;
            evaluatePmdArgument(assertionValue4Argument1);
            pid = Long.parseLong(Files.readString(pidFile));
            final var assertionValue3 = captureThrowable(() -> child.await(1));
            error = (IllegalStateException) assertionValue3;
            final var assertionValue2Argument1 = error.getMessage().contains("제한 시간");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 =
                    ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
            assertFalse(assertionValue1Argument1, "The child process should no longer be alive.");
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

        public static void main(final String[] args) throws Exception {
            final Path pidFile;
            pidFile = Path.of(args[0]);
            Files.createFile(pidFile);
            Thread.sleep(50);
            Files.writeString(pidFile, Long.toString(ProcessHandle.current().pid()));
            while (true) Thread.sleep(1_000);
        }
    }

    public static final class ExitChild {

        private ExitChild() {}

        public static void main(final String[] args) {
            System.exit(Integer.parseInt(args[0]));
        }
    }

    private static Throwable captureThrowable(final org.junit.jupiter.api.function.Executable action) {
        return org.junit.jupiter.api.Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
