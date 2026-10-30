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
import org.junit.jupiter.api.Test;

/** ADR-0002: 저장소 SPI는 비공개 내부 계약이다. module-info가 공개 패키지만 export하는지 고정한다. */
class ModuleDescriptorTest {

    private static ModuleDescriptor descriptor() throws IOException, URISyntaxException {
        // 루트의 /module-info.class는 JDK 모듈 것이 먼저 잡히므로 Smq의 코드 위치에서 읽는다.
        Path root = Path.of(Smq.class.getProtectionDomain().getCodeSource().getLocation().toURI());
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
    void 공개_패키지_하나만_export한다() throws IOException, URISyntaxException {
        ModuleDescriptor d = descriptor();

        assertEquals("io.github.ppzxc.smq", d.name());
        Set<String> exported = d.exports().stream()
                .map(ModuleDescriptor.Exports::source)
                .collect(Collectors.toSet());
        assertEquals(Set.of("io.github.ppzxc.smq"), exported);
        assertTrue(d.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified), "한정 export는 두지 않는다");
    }

    @Test
    void 필요한_module은_sql과_sqlite_jdbc와_정적_전이_jspecify다() throws IOException, URISyntaxException {
        Set<ModuleDescriptor.Requires> declared = descriptor().requires().stream()
                .filter(r -> !r.modifiers().contains(ModuleDescriptor.Requires.Modifier.MANDATED))
                .collect(Collectors.toSet());

        assertEquals(
                Set.of("java.sql", "org.xerial.sqlitejdbc", "org.jspecify"),
                declared.stream().map(ModuleDescriptor.Requires::name).collect(Collectors.toSet()));

        ModuleDescriptor.Requires jspecify = declared.stream()
                .filter(r -> r.name().equals("org.jspecify"))
                .findFirst()
                .orElseThrow();
        assertEquals(
                Set.of(ModuleDescriptor.Requires.Modifier.STATIC, ModuleDescriptor.Requires.Modifier.TRANSITIVE),
                jspecify.modifiers());

        Set<ModuleDescriptor.Requires> runtimeRequires = declared.stream()
                .filter(r -> !r.name().equals("org.jspecify"))
                .collect(Collectors.toSet());
        assertTrue(
                runtimeRequires.stream()
                        .noneMatch(r -> r.modifiers().contains(ModuleDescriptor.Requires.Modifier.TRANSITIVE)),
                "런타임 requires는 transitive가 아니어야 한다");
    }
}
