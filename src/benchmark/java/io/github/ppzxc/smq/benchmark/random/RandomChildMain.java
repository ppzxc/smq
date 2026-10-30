package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.ModuleGuard;
import java.nio.file.Path;

/**
 * 무작위 하네스의 자식 JVM 진입점. 인자는 {@code <params> <프로세스 번호> <DB 경로> <출력 디렉터리> <부하 시작 epoch ms>}이다.
 * 부하 구간에서 프로듀서와 컨슈머 스레드를 돌리고 발행·전달 기록을 출력 디렉터리에 쓴 뒤 끝난다.
 */
public final class RandomChildMain {

    private RandomChildMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("사용법: RandomChildMain <params> <process> <dbPath> <outDir> <startAtEpochMs>");
            System.exit(2);
        }
        ModuleGuard.requireSmqModule("무작위 자식 JVM");
        RandomRun.runChild(
                RunParams.fromArg(args[0]),
                Integer.parseInt(args[1]),
                Path.of(args[2]),
                Path.of(args[3]),
                Long.parseLong(args[4]));
    }
}
