package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 자식 정리는 정상 종료 신호를 먼저 보내 종료 훅(JFR 덤프)이 돌게 하고, 안 끝나면 강제로 끝낸다. 셸 스크립트를 자식으로 쓴다. */
class ChildJvmTerminateTest {

    @TempDir
    Path dir;

    private Process sh(String script) throws IOException {
        assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("windows"), "셸이 필요하다");
        return new ProcessBuilder("sh", "-c", script).start();
    }

    @Test
    void 정상_종료_신호를_받아_종료_훅이_돌_시간을_준다() throws Exception {
        Path marker = dir.resolve("hooked");
        Process p = sh("trap 'echo done > " + marker + "; exit 0' TERM; echo ready; while true; do sleep 0.05; done");
        p.getInputStream().read(); // trap이 설치되기를 기다린다("ready" 한 글자)

        ChildJvm.terminate(p, 5_000);

        assertFalse(p.isAlive());
        assertTrue(Files.exists(marker), "종료 훅이 실행됐어야 한다(SIGKILL이면 실행되지 않는다)");
    }

    @Test
    void 신호를_무시하면_유예_뒤에_강제로_끝낸다() throws Exception {
        Process p = sh("trap '' TERM; echo ready; while true; do sleep 0.05; done");
        p.getInputStream().read();

        long start = System.nanoTime();
        ChildJvm.terminate(p, 300);
        long ms = (System.nanoTime() - start) / 1_000_000;

        assertFalse(p.isAlive());
        assertTrue(ms >= 250, "유예 시간은 기다린다: " + ms);
        assertTrue(ms < 5_000, "무한히 기다리지 않는다: " + ms);
    }

    @Test
    void 이미_끝난_프로세스는_바로_돌아온다() throws Exception {
        Process p = sh("exit 0");
        p.waitFor();
        long start = System.nanoTime();
        ChildJvm.terminate(p, 5_000);
        assertTrue((System.nanoTime() - start) / 1_000_000 < 1_000);
    }
}
