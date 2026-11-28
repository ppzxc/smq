package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ppzxc.smq.PolicyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RunnerCancellationTest {

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("취소가 호출자에게 전달되기 전에 작업 디렉터리를 정리한다 — check 1")
    void cleanUpTheWorkingDirectoryBeforeTheCancellationIsPassedToTheCaller() throws Exception {
        final String previousModulePath;
        final Path workDir;
        final Path runDir;
        final AtomicReference<Throwable> result;
        final AtomicBoolean cleanedWhenReturned;
        final Thread runner;
        previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
        workDir = Files.createTempDirectory("smq-runner-cancel");
        runDir = workDir.resolve("run");
        result = new AtomicReference<>();
        cleanedWhenReturned = new AtomicBoolean();
        runner = new Thread(() -> {
            try {
                new Runner(workDir).run(RunSpec.s2(Storage.MEMORY, Profile.P0, 60_000, 0));
            } catch (Throwable e) {
                result.set(e);
                cleanedWhenReturned.set(!Files.exists(runDir));
            }
        });
        try {
            final long deadline;
            final Path childOutput;
            runner.start();
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            childOutput = runDir.resolve("child.out");
            while (!Files.exists(childOutput) && System.nanoTime() < deadline) Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(childOutput);
            assertTrue(assertionValue4Argument1, "runner should start its child JVM");
            runner.interrupt();
            runner.join(10_000);
            final var assertionValue3Argument1 = runner.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = cleanedWhenReturned.get();
            evaluatePmdArgument(assertionValue1Argument1);
        } finally {
            runner.interrupt();
            runner.join(10_000);
            if (previousModulePath == null) System.clearProperty("jdk.module.path");
            else System.setProperty("jdk.module.path", previousModulePath);
            try (var paths = Files.walk(workDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> path.toFile().delete());
            }
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("취소가 호출자에게 전달되기 전에 작업 디렉터리를 정리한다 — check 2")
    void cleanUpTheWorkingDirectoryBeforeTheCancellationIsPassedToTheCallerCheck2() throws Exception {
        final String previousModulePath;
        final Path workDir;
        final Path runDir;
        final AtomicReference<Throwable> result;
        final AtomicBoolean cleanedWhenReturned;
        final Thread runner;
        previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
        workDir = Files.createTempDirectory("smq-runner-cancel");
        runDir = workDir.resolve("run");
        result = new AtomicReference<>();
        cleanedWhenReturned = new AtomicBoolean();
        runner = new Thread(() -> {
            try {
                new Runner(workDir).run(RunSpec.s2(Storage.MEMORY, Profile.P0, 60_000, 0));
            } catch (Throwable e) {
                result.set(e);
                cleanedWhenReturned.set(!Files.exists(runDir));
            }
        });
        try {
            final long deadline;
            final Path childOutput;
            runner.start();
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            childOutput = runDir.resolve("child.out");
            while (!Files.exists(childOutput) && System.nanoTime() < deadline) Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(childOutput);
            evaluatePmdArgument(assertionValue4Argument1);
            runner.interrupt();
            runner.join(10_000);
            final var assertionValue3Argument1 = runner.isAlive();
            assertFalse(assertionValue3Argument1, "canceled run should return");
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = cleanedWhenReturned.get();
            evaluatePmdArgument(assertionValue1Argument1);
        } finally {
            runner.interrupt();
            runner.join(10_000);
            if (previousModulePath == null) System.clearProperty("jdk.module.path");
            else System.setProperty("jdk.module.path", previousModulePath);
            try (var paths = Files.walk(workDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> path.toFile().delete());
            }
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("취소가 호출자에게 전달되기 전에 작업 디렉터리를 정리한다 — check 3")
    void cleanUpTheWorkingDirectoryBeforeTheCancellationIsPassedToTheCallerCheck3() throws Exception {
        final String previousModulePath;
        final Path workDir;
        final Path runDir;
        final AtomicReference<Throwable> result;
        final AtomicBoolean cleanedWhenReturned;
        final Thread runner;
        previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
        workDir = Files.createTempDirectory("smq-runner-cancel");
        runDir = workDir.resolve("run");
        result = new AtomicReference<>();
        cleanedWhenReturned = new AtomicBoolean();
        runner = new Thread(() -> {
            try {
                new Runner(workDir).run(RunSpec.s2(Storage.MEMORY, Profile.P0, 60_000, 0));
            } catch (Throwable e) {
                result.set(e);
                cleanedWhenReturned.set(!Files.exists(runDir));
            }
        });
        try {
            final long deadline;
            final Path childOutput;
            runner.start();
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            childOutput = runDir.resolve("child.out");
            while (!Files.exists(childOutput) && System.nanoTime() < deadline) Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(childOutput);
            evaluatePmdArgument(assertionValue4Argument1);
            runner.interrupt();
            runner.join(10_000);
            final var assertionValue3Argument1 = runner.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            assertInstanceOf(
                    InterruptedException.class,
                    assertionValue2Argument2,
                    "Value should be an instance of InterruptedException.");
            final var assertionValue1Argument1 = cleanedWhenReturned.get();
            evaluatePmdArgument(assertionValue1Argument1);
        } finally {
            runner.interrupt();
            runner.join(10_000);
            if (previousModulePath == null) System.clearProperty("jdk.module.path");
            else System.setProperty("jdk.module.path", previousModulePath);
            try (var paths = Files.walk(workDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> path.toFile().delete());
            }
        }
    }

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    @DisplayName("취소가 호출자에게 전달되기 전에 작업 디렉터리를 정리한다 — check 4")
    void cleanUpTheWorkingDirectoryBeforeTheCancellationIsPassedToTheCallerCheck4() throws Exception {
        final String previousModulePath;
        final Path workDir;
        final Path runDir;
        final AtomicReference<Throwable> result;
        final AtomicBoolean cleanedWhenReturned;
        final Thread runner;
        previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
        workDir = Files.createTempDirectory("smq-runner-cancel");
        runDir = workDir.resolve("run");
        result = new AtomicReference<>();
        cleanedWhenReturned = new AtomicBoolean();
        runner = new Thread(() -> {
            try {
                new Runner(workDir).run(RunSpec.s2(Storage.MEMORY, Profile.P0, 60_000, 0));
            } catch (Throwable e) {
                result.set(e);
                cleanedWhenReturned.set(!Files.exists(runDir));
            }
        });
        try {
            final long deadline;
            final Path childOutput;
            runner.start();
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            childOutput = runDir.resolve("child.out");
            while (!Files.exists(childOutput) && System.nanoTime() < deadline) Thread.sleep(10);
            final var assertionValue4Argument1 = Files.exists(childOutput);
            evaluatePmdArgument(assertionValue4Argument1);
            runner.interrupt();
            runner.join(10_000);
            final var assertionValue3Argument1 = runner.isAlive();
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue2Argument2 = result.get();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument1 = cleanedWhenReturned.get();
            assertTrue(assertionValue1Argument1, "runner should remove its work directory before returning");
        } finally {
            runner.interrupt();
            runner.join(10_000);
            if (previousModulePath == null) System.clearProperty("jdk.module.path");
            else System.setProperty("jdk.module.path", previousModulePath);
            try (var paths = Files.walk(workDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> path.toFile().delete());
            }
        }
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
