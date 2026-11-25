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
import org.junit.jupiter.api.Test;

class StorageArchitectureTest {
    private static final Pattern IMPORT = Pattern.compile("import\\s+(?:static\\s+)?([\\w.]+)");
    private static final Pattern QUALIFIED =
            Pattern.compile("\\b(?:io\\.github\\.ppzxc\\.smq|java\\.sql|org\\.sqlite)\\.[\\w.]+");
    private static final String CORE = "io.github.ppzxc.smq.";

    @Test
    void 코어는_저장소_구현과_jdbc를_참조하지_않고_조립은_허용한다() throws IOException {
        Path root = Path.of("src/main/java/io/github/ppzxc/smq");
        List<String> violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(
                            path -> violations.addAll(violations(path.toString().replace('\\', '/'), read(path))));
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
        assertTrue(violations(
                        "src/main/java/io/github/ppzxc/smq/DefaultQueue.java",
                        "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                .getFirst()
                .contains("core"));
        assertTrue(violations(
                        "src/main/java/io/github/ppzxc/smq/Smq.java",
                        "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                .isEmpty());
    }

    @Test
    void 저장소_구현은_서로_참조하지_않는다() {
        assertTrue(violations(
                        "src/main/java/io/github/ppzxc/smq/storage/memory/M.java",
                        "import io.github.ppzxc.smq.storage.sqlite.SqliteStorage;\n")
                .getFirst()
                .contains("storage"));
        assertEquals(
                List.of(),
                violations(
                        "src/main/java/io/github/ppzxc/smq/storage/sqlite/S.java",
                        "import io.github.ppzxc.smq.storage.Storage;\n"));
    }

    @Test
    void import_없이_쓴_전체_클래스명도_금지한다() {
        assertTrue(violations(
                        "src/main/java/io/github/ppzxc/smq/DefaultQueue.java",
                        "io.github.ppzxc.smq.storage.sqlite.SqliteStorage storage;\n")
                .getFirst()
                .contains("storage.sqlite.SqliteStorage"));
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> violations(String path, String source) {
        boolean core = path.contains("/smq/") && !path.contains("/smq/storage/");
        boolean assembly = path.endsWith("/Smq.java");
        String implementation =
                path.contains("/storage/memory/") ? "memory" : path.contains("/storage/sqlite/") ? "sqlite" : "";
        Matcher imports = IMPORT.matcher(source);
        Set<String> names = new LinkedHashSet<>();
        while (imports.find()) {
            names.add(imports.group(1));
        }
        Matcher qualified = QUALIFIED.matcher(source);
        while (qualified.find()) {
            names.add(qualified.group());
        }
        List<String> violations = new ArrayList<>();
        for (String name : names) {
            boolean forbidden = core
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
