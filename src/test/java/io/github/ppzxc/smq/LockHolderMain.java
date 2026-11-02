package io.github.ppzxc.smq;

import java.nio.file.Path;

/** 락 테스트가 별도 JVM으로 띄우는 프로그램. 저장소를 열고 {@code READY}를 쓴 뒤 표준 입력이 닫힐 때까지 쥔다. */
public final class LockHolderMain {

    private LockHolderMain() {}

    public static void main(String[] args) throws Exception {
        try (Smq ignored = Smq.sqlite(Path.of(args[0])).open()) {
            System.out.println("READY");
            System.out.flush();
            while (System.in.read() != -1) {
                // 입력이 닫힐 때까지 쥔다.
            }
        }
    }
}
