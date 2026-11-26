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
import org.junit.jupiter.api.Test;

class RunnerCancellationTest {

    @Test
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void 취소가_호출자에게_전달되기_전에_작업_디렉터리를_정리한다() throws Exception {
        String previousModulePath = System.getProperty("jdk.module.path");
        System.setProperty("jdk.module.path", System.getProperty("java.class.path"));
        Path workDir = Files.createTempDirectory("smq-runner-cancel");
        Path runDir = workDir.resolve("run");
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean cleanedWhenReturned = new AtomicBoolean();
        Thread runner = new Thread(() -> {
            try {
                new Runner(workDir).run(RunSpec.s2(Storage.MEMORY, Profile.P0, 60_000, 0));
            } catch (Throwable e) {
                result.set(e);
                cleanedWhenReturned.set(!Files.exists(runDir));
            }
        });
        try {
            runner.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            Path childOutput = runDir.resolve("child.out");
            while (!Files.exists(childOutput) && System.nanoTime() < deadline) Thread.sleep(10);
            assertTrue(Files.exists(childOutput), "runner should start its child JVM");

            runner.interrupt();
            runner.join(10_000);

            assertFalse(runner.isAlive(), "canceled run should return");
            assertInstanceOf(InterruptedException.class, result.get());
            assertTrue(cleanedWhenReturned.get(), "runner should remove its work directory before returning");
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
}
