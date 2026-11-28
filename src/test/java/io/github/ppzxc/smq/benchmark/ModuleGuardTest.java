package io.github.ppzxc.smq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class ModuleGuardTest {

    @Test
    void acceptsClassLoadedFromNamedModule() {
        assertEquals(
                "java.base", ModuleGuard.requireNamed(String.class), "Require named should match the expected value.");
    }

    @Test
    void rejectsClassLoadedFromClasspath() {
        final IllegalStateException e;
        final var assertionValue2 = captureThrowable(() -> ModuleGuard.requireNamed(ModuleGuardTest.class));
        assertInstanceOf(
                IllegalStateException.class, assertionValue2, "Require named should throw IllegalStateException.");
        e = (IllegalStateException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains("모듈 경로");
        evaluatePmdArgument(assertionValue1Argument1);
    }

    @Test
    void rejectsClassLoadedFromClasspathCheck2() {
        final IllegalStateException e;
        final var assertionValue2 = captureThrowable(() -> ModuleGuard.requireNamed(ModuleGuardTest.class));
        e = (IllegalStateException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains("모듈 경로");
        assertTrue(assertionValue1Argument1, "Message should contain the expected value.");
    }

    @Test
    void childCommandCarriesModulePathAndKeepsBenchmarkOnClasspath() {
        final List<String> cmd;
        cmd = ModuleGuard.childJvmArgs("/mods/smq.jar:/mods/sqlite.jar", "/cp/benchmark");
        assertEquals(
                List.of(
                        "--enable-native-access=ALL-UNNAMED",
                        "--enable-native-access=org.xerial.sqlitejdbc",
                        "--module-path",
                        "/mods/smq.jar:/mods/sqlite.jar",
                        "--add-modules",
                        "io.github.ppzxc.smq",
                        "-cp",
                        "/cp/benchmark"),
                cmd,
                "Cmd should match the expected value.");
    }

    @Test
    void childCommandRefusesMissingModulePath() {
        assertThrows(
                IllegalStateException.class,
                () -> ModuleGuard.childJvmArgs(null, "/cp"),
                "Child jvm args should throw IllegalStateException.");
    }

    private static Throwable captureThrowable(final org.junit.jupiter.api.function.Executable action) {
        return assertThrows(Throwable.class, action);
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
