package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.ChildJvm;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * 다중 프로세스 실행에서 부모가 자식을 띄우는 방법. 실제로는 자식 JVM이고({@link #jvm()}), 테스트는 같은 JVM의 스레드로 바꿔 끼운다.
 */
interface ChildSpawner {

    /**
     * @param process 자식의 프로세스 번호(0부터). 이 자식이 발행하는 태그의 프로세스 구성요소다
     * @param outDir 자식이 기록을 쓰는 디렉터리(부모의 실행 디렉터리)
     * @param startAt 부하 구간이 시작하는 시각(epoch ms). 자식은 그때까지 기다렸다가 시작한다
     */
    record Spec(RunParams params, int process, Path db, Path outDir, long startAt) {}

    interface Running {
        /** 끝나기를 기다린다. 실패했거나 시간을 넘기면 예외를 낸다. */
        void await(long timeoutMs) throws Exception;

        void destroy();
    }

    Running spawn(Spec spec) throws IOException;

    /** 기존 벤치와 같은 {@link ChildJvm}으로 자식 JVM을 띄운다. 자식도 모듈 경로에서 돈다. */
    static ChildSpawner jvm() {
        return jvm(false);
    }

    /**
     * @param heapDump 켜면 자식 JVM에 {@code -XX:+HeapDumpOnOutOfMemoryError}를 주고 덤프를 실행 디렉터리에 남긴다. 덤프가 커서 기본은
     *     끈다
     */
    static ChildSpawner jvm(boolean heapDump) {
        return spec -> {
            Path err = spec.outDir().resolve("p" + spec.process() + ".err");
            List<String> options = heapDump
                    ? List.of("-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=" + spec.outDir())
                    : List.of();
            ChildJvm jvm = ChildJvm.start(
                    RandomChildMain.class.getName(),
                    List.of(
                            spec.params().toArg(),
                            Integer.toString(spec.process()),
                            spec.db().toString(),
                            spec.outDir().toString(),
                            Long.toString(spec.startAt())),
                    spec.outDir().resolve("p" + spec.process() + ".out"),
                    err,
                    options,
                    "무작위 하네스 프로세스 " + spec.process());
            return new Running() {
                public void await(long timeoutMs) throws Exception {
                    try {
                        jvm.await(timeoutMs);
                    } catch (Exception e) {
                        throw new ChildFailure(
                                spec.process(),
                                jvm.exitCode(),
                                ChildFailure.readTail(err, ChildFailure.STDERR_TAIL_BYTES),
                                e);
                    }
                }

                public void destroy() {
                    jvm.destroy();
                }
            };
        };
    }
}
