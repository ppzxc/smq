package io.github.ppzxc.smq.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 자식 JVM 하나. 부모와 같은 모듈 경로로 띄우고(모듈 경로가 없으면 실패한다) 표준 출력을 파일로 받는다. 고정 프로파일 러너와 무작위
 * 하네스가 함께 쓴다.
 */
public final class ChildJvm {

    private final Process process;
    private final Path out;
    private final String label;

    private ChildJvm(Process process, Path out, String label) {
        this.process = process;
        this.out = out;
        this.label = label;
    }

    /** {@code mainClass}를 새 JVM에서 돌린다. 표준 출력은 {@code out}에, 표준 오류는 부모의 것에 잇는다. */
    public static ChildJvm start(String mainClass, List<String> args, Path out, String label) throws IOException {
        return start(mainClass, args, out, null, List.of(), label);
    }

    /**
     * {@link #start(String, List, Path, String)}에 더해 표준 오류를 {@code err} 파일로 받고({@code null}이면 부모의 것에 잇는다)
     * 모듈 경로 인자 앞에 JVM 옵션 {@code jvmOptions}를 붙인다.
     */
    public static ChildJvm start(String mainClass, List<String> args, Path out, Path err, List<String> jvmOptions,
            String label) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> cmd = new ArrayList<>();
        cmd.add(java);
        cmd.addAll(jvmOptions);
        cmd.addAll(ModuleGuard.childJvmArgs(System.getProperty("jdk.module.path"), System.getProperty("java.class.path")));
        cmd.add(mainClass);
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd)
                .redirectOutput(out.toFile())
                .redirectError(err == null ? ProcessBuilder.Redirect.INHERIT : ProcessBuilder.Redirect.to(err.toFile()))
                .start();
        return new ChildJvm(p, out, label);
    }

    /** 끝나기를 기다린다. 시간을 넘기거나 종료 코드가 0이 아니면 예외를 낸다. */
    public void await(long timeoutMs) throws Exception {
        if (!process.waitFor(Math.max(timeoutMs, 1_000), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("자식 JVM이 제한 시간 안에 끝나지 않았다: " + label);
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("자식 JVM 실패(" + label + "), 종료 코드 " + process.exitValue());
        }
    }

    /** 종료 코드. 아직 돌고 있으면 null. */
    public Integer exitCode() {
        return process.isAlive() ? null : process.exitValue();
    }

    /** 표준 출력의 마지막 JSON 객체 줄. */
    public String lastJsonLine() throws IOException {
        return Files.readAllLines(out).stream().filter(l -> l.startsWith("{")).reduce((a, b) -> b)
                .orElseThrow(() -> new IllegalStateException("자식 JVM이 결과를 내지 않았다: " + label));
    }

    public void destroy() {
        process.destroyForcibly();
    }
}
