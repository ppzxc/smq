package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.benchmark.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * 자식 JVM이 실패를 부모에게 알리는 방법. {@code Error}({@code OutOfMemoryError} 포함)까지 잡아 {@code p<n>-error.json}에
 * 예외 클래스, 메시지, 스택 트레이스를 쓰고 그대로 다시 던진다. 이 파일을 쓰지 못하고 죽으면 부모가 종료 코드와 표준 오류로 판정한다.
 */
final class ChildReport {

    private ChildReport() {}

    interface Body {
        void run() throws Exception;
    }

    static Path file(Path outDir, int process) {
        return outDir.resolve("p" + process + "-error.json");
    }

    /** {@code body}를 돌리고, {@link Throwable}이 나면 보고 파일을 쓴 뒤 다시 던진다. */
    static void guard(Path outDir, int process, Body body) throws Exception {
        try {
            body.run();
        } catch (Throwable t) {
            write(outDir, process, t);
            throw t;
        }
    }

    private static void write(Path outDir, int process, Throwable t) {
        try {
            ErrorDetail d = ErrorDetail.of(ErrorDetail.child(process), t, null, null);
            Files.writeString(file(outDir, process), Json.write(d.toMap()) + "\n");
        } catch (Throwable ignored) {
            // OOM 직후에는 보고서를 만드는 것도 실패할 수 있다. 원래 예외를 가리지 않고, 보고하지 못하면 부모가 종료 코드와 표준 오류로 판정한다.
        }
    }

    /** 자식이 남긴 보고. 없거나 읽을 수 없으면 비어 있다. */
    static Optional<ErrorDetail> read(Path outDir, int process) {
        Path f = file(outDir, process);
        if (!Files.isRegularFile(f)) {
            return Optional.empty();
        }
        try {
            Map<?, ?> m = (Map<?, ?>) Json.parse(Files.readString(f));
            return Optional.of(new ErrorDetail(
                    (String) m.get("location"),
                    (String) m.get("exceptionClass"),
                    (String) m.get("message"),
                    (String) m.get("stackTrace"),
                    null,
                    null));
        } catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
    }
}
