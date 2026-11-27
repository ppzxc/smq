package io.github.ppzxc.smq.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** 자식 JVM 하나. 부모와 같은 모듈 경로로 띄우고(모듈 경로가 없으면 실패한다) 표준 출력을 파일로 받는다. */
public final class ChildJvm {

    private final Process process;
    private final Path out;
    private final String label;

    private ChildJvm(final Process process, final Path out, final String label) {
        this.process = process;
        this.out = out;
        this.label = label;
    }

    /** {@code mainClass}를 새 JVM에서 돌린다. 표준 출력은 {@code out}에, 표준 오류는 부모의 것에 잇는다. */
    public static ChildJvm start(final String mainClass, final List<String> args, final Path out, final String label)
            throws IOException {
        final String java;
        final List<String> cmd;
        final Process p;
        java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        cmd = new ArrayList<>();
        cmd.add(java);
        cmd.addAll(
                ModuleGuard.childJvmArgs(System.getProperty("jdk.module.path"), System.getProperty("java.class.path")));
        cmd.add(mainClass);
        cmd.addAll(args);
        p = new ProcessBuilder(cmd)
                .redirectOutput(out.toFile())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        return new ChildJvm(p, out, label);
    }

    /** 끝나기를 기다린다. 시간을 넘기거나 종료 코드가 0이 아니면 예외를 낸다. */
    public void await(final long timeoutMs) throws InterruptedException {
        try {
            if (!process.waitFor(Math.max(timeoutMs, 1_000), TimeUnit.MILLISECONDS)) {
                terminate(process);
                throw new IllegalStateException("자식 JVM이 제한 시간 안에 끝나지 않았다: " + label);
            }
        } catch (InterruptedException e) {
            terminate(process);
            throw e;
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("자식 JVM 실패(" + label + "), 종료 코드 " + process.exitValue());
        }
    }

    /** 표준 출력의 마지막 JSON 객체 줄. */
    public String lastJsonLine() throws IOException {
        return Files.readAllLines(out).stream()
                .filter(l -> l.startsWith("{"))
                .reduce((a, b) -> b)
                .orElseThrow(() -> new IllegalStateException("자식 JVM이 결과를 내지 않았다: " + label));
    }

    /** 강제로 끝내고 프로세스가 사라질 때까지 기다린다. 인터럽트를 받으면 플래그를 복원한다. */
    static void terminate(final Process process) {
        boolean interrupted;
        process.destroyForcibly();
        interrupted = false;
        try {
            while (process.isAlive()) {
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
