package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import org.junit.jupiter.api.Test;

/** 자식 정리는 강제로 끝내고 프로세스가 사라질 때까지 기다린다. 셸 스크립트를 자식으로 쓴다. */
class ChildJvmTerminateTest {

    @Test
    void 강제_종료_뒤_프로세스가_사라졌다() throws Exception {
        assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("windows"), "셸이 필요하다");
        Process p = new ProcessBuilder("sh", "-c", "trap '' TERM; echo ready; while true; do sleep 0.05; done").start();
        p.getInputStream().read(); // trap이 설치되기를 기다린다("ready" 한 글자)

        ChildJvm.terminate(p);

        assertFalse(p.isAlive());
    }
}
