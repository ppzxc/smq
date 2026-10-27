package io.github.ppzxc.smq.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** 결과 파일에 남기는 환경 기록. 고정 프로파일 벤치와 무작위 하네스가 같은 항목을 쓴다. */
public final class Environment {

    private Environment() {}

    /** @param workDir SQLite 파일을 만들 디렉터리. 이 파일 시스템 종류가 {@code workDirFs}로 남는다 */
    public static Map<String, String> capture(String mode, String storage, Path workDir) throws IOException {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("date", Instant.now().toString());
        env.put("mode", mode);
        env.put("storage", storage);
        env.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("arch", System.getProperty("os.arch"));
        env.put("cpus", String.valueOf(Runtime.getRuntime().availableProcessors()));
        env.put("java", System.getProperty("java.version"));
        env.put("workDirFs", Files.getFileStore(workDir).type());
        Path version = Path.of("/proc/version");
        if (Files.exists(version) && Files.readString(version).toLowerCase().contains("microsoft")) {
            env.put("caveat", "WSL2에서 잰 값이다. fsync 비용을 과소평가할 수 있다.");
        }
        return env;
    }
}
