package io.github.ppzxc.smq.benchmark.random;

import java.io.IOException;

/** 실행이 쓰는 두 큐. */
enum QueueKind {
    TARGET(0),
    DEAD_LETTER(1);

    private final int spoolCode;

    QueueKind(int spoolCode) {
        this.spoolCode = spoolCode;
    }

    int spoolCode() {
        return spoolCode;
    }

    static QueueKind fromSpoolCode(int code) throws IOException {
        return switch (code) {
            case 0 -> TARGET;
            case 1 -> DEAD_LETTER;
            default -> throw new IOException("Unknown queue code: " + code);
        };
    }
}
