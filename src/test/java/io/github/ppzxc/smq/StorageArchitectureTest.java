package io.github.ppzxc.smq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StorageArchitectureTest {
    private static final Pattern IMPORT = Pattern.compile("import\\s+(?:static\\s+)?([\\w.]+)");
    private static final Pattern QUALIFIED =
            Pattern.compile("\\b(?:io\\.github\\.ppzxc\\.smq|java\\.sql|org\\.sqlite)\\.[\\w.]+");
    private static final String CORE = "io.github.ppzxc.smq.";

    @Test
    @DisplayName("코어는 저장소 구현과 jdbc를 참조하지 않고 조립은 허용한다")
    void theCoreAllowsAssemblyWithoutReferencingTheRepositoryImplementationAndJDBC() throws IOException {
        final Path root;
        final List<String> violations;
        root = Path.of("src/main/java/io/github/ppzxc/smq");
        violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(
                            path -> violations.addAll(violations(path.toString().replace('\\', '/'), read(path))));
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    @DisplayName("코어 구현에서 SQLite 저장소 참조를 금지한다")
    void coreImplementationCannotReferenceSqliteStorage() {
        assertTrue(
                violations(
                                "src/main/java/io/github/ppzxc/smq/DefaultQueue.java",
                                "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                        .getFirst()
                        .contains("core"),
                "Violations should contain the expected value.");
    }

    @Test
    @DisplayName("조립 코드는 SQLite 저장소 참조를 허용한다")
    void assemblyCanReferenceSqliteStorage() {
        assertTrue(
                violations(
                                "src/main/java/io/github/ppzxc/smq/Smq.java",
                                "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                        .isEmpty(),
                "Violations should be empty.");
    }

    @Test
    @DisplayName("저장소 구현은 서로 참조하지 않는다 — memory isolation")
    void memoryStorageCannotReferenceSqlite() {
        assertTrue(
                violations(
                                "src/main/java/io/github/ppzxc/smq/storage/memory/M.java",
                                "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                        .getFirst()
                        .contains("storage"),
                "Violations should contain the expected value.");
    }

    @Test
    @DisplayName("저장소 구현은 서로 참조하지 않는다 — storage SPI")
    void sqliteStorageCanReferenceStorageSpi() {
        assertEquals(
                List.of(),
                violations(
                        "src/main/java/io/github/ppzxc/smq/storage/sqlite/S.java",
                        "import io.github.ppzxc.smq.storage.Storage;\n"),
                "Violations should match the expected value.");
    }

    @Test
    @DisplayName("import 없이 쓴 전체 클래스명도 금지한다")
    void fullClassNamesWrittenWithoutAnImportAreAlsoProhibited() {
        assertTrue(
                violations(
                                "src/main/java/io/github/ppzxc/smq/DefaultQueue.java",
                                "io.github.ppzxc.smq.storage.sqlite.SqliteStorage storage;\n")
                        .getFirst()
                        .contains("storage.sqlite.SqliteStorage"),
                "Violations should contain the expected value.");
    }

    private static String read(final Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> violations(final String path, final String source) {
        final boolean core;
        final boolean assembly;
        final String implementation;
        final Matcher imports;
        final Set<String> names;
        final Matcher qualified;
        final List<String> violations;
        core = path.contains("/smq/") && !path.contains("/smq/storage/");
        assembly = path.endsWith("/Smq.java");
        implementation =
                path.contains("/storage/memory/") ? "memory" : path.contains("/storage/sqlite/") ? "sqlite" : "";
        imports = IMPORT.matcher(source);
        names = new LinkedHashSet<>();
        while (imports.find()) {
            names.add(imports.group(1));
        }
        qualified = QUALIFIED.matcher(source);
        while (qualified.find()) {
            names.add(qualified.group());
        }
        violations = new ArrayList<>();
        for (final String name : names) {
            boolean forbidden;
            forbidden = core
                    && !assembly
                    && (name.startsWith(CORE + "storage.memory.")
                            || name.startsWith(CORE + "storage.sqlite.")
                            || name.startsWith("java.sql.")
                            || name.startsWith("org.sqlite."));
            forbidden |= !implementation.isEmpty()
                    && ((implementation.equals("memory") && name.startsWith(CORE + "storage.sqlite."))
                            || (implementation.equals("sqlite") && name.startsWith(CORE + "storage.memory.")));
            if (forbidden) {
                violations.add(path + ": forbidden dependency " + name
                        + "; core must depend on the storage SPI and implementations must remain independent");
            }
        }
        return violations;
    }
}
