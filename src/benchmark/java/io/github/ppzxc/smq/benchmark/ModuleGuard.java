package io.github.ppzxc.smq.benchmark;

import io.github.ppzxc.smq.Smq;
import java.util.List;

/**
 * 벤치 JVM이 smq를 모듈 경로의 이름 있는 모듈로 읽는지 확인한다. 클래스패스에서 읽으면 {@code module-info}의 exports가
 * 하네스의 경계를 강제하지 못하므로, 부모와 자식 JVM은 시작할 때 이 검사를 통과해야 한다.
 */
public final class ModuleGuard {

    static final String SMQ_MODULE = "io.github.ppzxc.smq";

    private ModuleGuard() {}

    /** 클래스가 이름 있는 모듈에서 로드됐으면 모듈 이름을, 아니면 예외를 낸다. */
    static String requireNamed(Class<?> type) {
        Module module = type.getModule();
        if (!module.isNamed()) {
            throw new IllegalStateException(type.getName() + "이(가) 모듈 경로가 아닌 클래스패스에서 로드됐다");
        }
        return module.getName();
    }

    /** 부모·자식 JVM이 시작할 때 부른다. smq가 모듈 경로에 없으면 실패하고, 있으면 그 사실을 한 줄로 알린다. */
    public static void requireSmqModule(String role) {
        String name = requireNamed(Smq.class);
        if (!SMQ_MODULE.equals(name)) throw new IllegalStateException("예상하지 못한 모듈: " + name);
        System.err.println("[module] " + role + ": smq는 모듈 경로의 " + name + " 모듈에서 읽는다");
    }

    /** 자식 JVM 명령줄의 앞부분(java 실행 파일 다음, 메인 클래스 앞). 부모의 모듈 경로를 그대로 물려준다. */
    static List<String> childJvmArgs(String modulePath, String classPath) {
        if (modulePath == null || modulePath.isBlank()) {
            throw new IllegalStateException("부모 JVM에 모듈 경로가 없다. 벤치는 모듈 경로로만 돈다");
        }
        return List.of(
                "--enable-native-access=ALL-UNNAMED",
                "--enable-native-access=org.xerial.sqlitejdbc",
                "--module-path",
                modulePath,
                "--add-modules",
                SMQ_MODULE,
                "-cp",
                classPath);
    }
}
