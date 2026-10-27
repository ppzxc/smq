package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/** 벤치 JVM이 smq를 모듈 경로에서 읽는지 확인하는 가드의 동작. */
class ModuleGuardTest {

    @Test
    void acceptsClassLoadedFromNamedModule() {
        assertEquals("java.base", ModuleGuard.requireNamed(String.class));
    }

    @Test
    void rejectsClassLoadedFromClasspath() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> ModuleGuard.requireNamed(ModuleGuardTest.class));
        assertTrue(e.getMessage().contains("모듈 경로"));
    }

    @Test
    void childCommandCarriesModulePathAndKeepsBenchmarkOnClasspath() {
        List<String> cmd = ModuleGuard.childJvmArgs("/mods/smq.jar:/mods/sqlite.jar", "/cp/benchmark");
        assertEquals(List.of("--enable-native-access=ALL-UNNAMED", "--enable-native-access=org.xerial.sqlitejdbc",
                "--module-path", "/mods/smq.jar:/mods/sqlite.jar", "--add-modules", "io.github.ppzxc.smq",
                "-cp", "/cp/benchmark"), cmd);
    }

    @Test
    void childCommandRefusesMissingModulePath() {
        assertThrows(IllegalStateException.class, () -> ModuleGuard.childJvmArgs(null, "/cp"));
    }
}
