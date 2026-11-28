package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ModuleDescriptorTest {

    private static ModuleDescriptor descriptor() throws IOException, URISyntaxException {

        final Path root;
        root = Path.of(
                Smq.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (Files.isDirectory(root)) {
            try (InputStream in = Files.newInputStream(root.resolve("module-info.class"))) {
                return ModuleDescriptor.read(in);
            }
        }
        try (FileSystem jar = FileSystems.newFileSystem(root);
                InputStream in = Files.newInputStream(jar.getPath("module-info.class"))) {
            return ModuleDescriptor.read(in);
        }
    }

    @Test
    @DisplayName("모듈 이름은 io.github.ppzxc.smq다")
    void moduleNameIsSmq() throws IOException, URISyntaxException {
        assertEquals("io.github.ppzxc.smq", descriptor().name(), "The module name should identify smq.");
    }

    @Test
    @DisplayName("공개 패키지 하나만 export한다")
    void onlyOnePublicPackageIsExported() throws IOException, URISyntaxException {
        final Set<String> exported;
        exported = descriptor().exports().stream()
                .map(ModuleDescriptor.Exports::source)
                .collect(Collectors.toSet());
        assertEquals(Set.of("io.github.ppzxc.smq"), exported, "Only the public package should be exported.");
    }

    @Test
    @DisplayName("한정 export는 두지 않는다")
    void exportsAreNotQualified() throws IOException, URISyntaxException {
        assertTrue(
                descriptor().exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified),
                "Exports should be available to all modules.");
    }

    @Test
    @DisplayName("필요한 module은 sql과 sqlite jdbc와 정적 전이 jspecify다")
    void requiredModulesIncludeSqlSqliteJdbcAndStaticTransitiveJspecify() throws IOException, URISyntaxException {
        final Set<ModuleDescriptor.Requires> declared;
        declared = descriptor().requires().stream()
                .filter(r -> !r.modifiers().contains(ModuleDescriptor.Requires.Modifier.MANDATED))
                .collect(Collectors.toSet());

        assertEquals(
                Set.of("java.sql", "org.xerial.sqlitejdbc", "org.jspecify"),
                declared.stream().map(ModuleDescriptor.Requires::name).collect(Collectors.toSet()),
                "Declared should match the expected value.");
    }

    @Test
    @DisplayName("jspecify는 정적 전이 의존성이다")
    void jspecifyIsStaticAndTransitive() throws IOException, URISyntaxException {
        final ModuleDescriptor.Requires jspecify;
        jspecify = descriptor().requires().stream()
                .filter(r -> r.name().equals("org.jspecify"))
                .findFirst()
                .orElseThrow();
        assertEquals(
                Set.of(ModuleDescriptor.Requires.Modifier.STATIC, ModuleDescriptor.Requires.Modifier.TRANSITIVE),
                jspecify.modifiers(),
                "Modifiers should match the expected value.");
    }

    @Test
    @DisplayName("런타임 의존성은 전이되지 않는다")
    void runtimeRequirementsAreNotTransitive() throws IOException, URISyntaxException {
        final Set<ModuleDescriptor.Requires> runtimeRequires;
        runtimeRequires = descriptor().requires().stream()
                .filter(r -> !r.modifiers().contains(ModuleDescriptor.Requires.Modifier.MANDATED))
                .filter(r -> !r.name().equals("org.jspecify"))
                .collect(Collectors.toSet());
        assertTrue(
                runtimeRequires.stream()
                        .noneMatch(r -> r.modifiers().contains(ModuleDescriptor.Requires.Modifier.TRANSITIVE)),
                "런타임 requires는 transitive가 아니어야 한다");
    }
}
