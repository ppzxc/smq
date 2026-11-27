package io.github.ppzxc.smq;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class SqliteSingleProcessLockTest {

    @TempDir
    Path dir;

    @Test
    @DisplayName("같은 JVM에서 같은 파일을 두 번 열면 즉시 StorageLockedException이고 경로가 메시지에 있다 — check 1")
    void
            ifYouOpenTheSameFileTwiceInTheSameJVMAStorageLockedExceptionOccursImmediatelyAndThePathIsIncludedInTheMessage() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            final StorageLockedException e;
            evaluatePmdArgument(first);
            final var assertionValue4 = captureThrowable(() -> Smq.sqlite(db).open());
            Assertions.assertInstanceOf(
                    StorageLockedException.class, assertionValue4, "Open should throw StorageLockedException.");
            e = (StorageLockedException) (StorageLockedException) assertionValue4;
            final var assertionValue3Argument1 = e.getMessage().contains(db.toString());
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = e.getMessage().contains(db + ".lock");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue2Argument2);
        }
    }

    @Test
    @DisplayName("같은 JVM에서 같은 파일을 두 번 열면 즉시 StorageLockedException이고 경로가 메시지에 있다 — check 2")
    void
            ifYouOpenTheSameFileTwiceInTheSameJVMAStorageLockedExceptionOccursImmediatelyAndThePathIsIncludedInTheMessageCheck2() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            final StorageLockedException e;
            evaluatePmdArgument(first);
            final var assertionValue4 = captureThrowable(() -> Smq.sqlite(db).open());
            e = (StorageLockedException) (StorageLockedException) assertionValue4;
            final var assertionValue3Argument1 = e.getMessage().contains(db.toString());
            final var assertionValue3Argument2 = e.getMessage();
            Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
            final var assertionValue2Argument1 = e.getMessage().contains(db + ".lock");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue2Argument2);
        }
    }

    @Test
    @DisplayName("같은 JVM에서 같은 파일을 두 번 열면 즉시 StorageLockedException이고 경로가 메시지에 있다 — check 3")
    void
            ifYouOpenTheSameFileTwiceInTheSameJVMAStorageLockedExceptionOccursImmediatelyAndThePathIsIncludedInTheMessageCheck3() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            final StorageLockedException e;
            evaluatePmdArgument(first);
            final var assertionValue4 = captureThrowable(() -> Smq.sqlite(db).open());
            e = (StorageLockedException) (StorageLockedException) assertionValue4;
            final var assertionValue3Argument1 = e.getMessage().contains(db.toString());
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = e.getMessage().contains(db + ".lock");
            final var assertionValue2Argument2 = e.getMessage();
            Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        }
    }

    @Test
    @DisplayName("같은 JVM에서 같은 파일을 두 번 열면 즉시 StorageLockedException이고 경로가 메시지에 있다 — check 4")
    void
            ifYouOpenTheSameFileTwiceInTheSameJVMAStorageLockedExceptionOccursImmediatelyAndThePathIsIncludedInTheMessageCheck4() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            final StorageLockedException e;
            final var assertionValue4 = captureThrowable(() -> Smq.sqlite(db).open());
            e = (StorageLockedException) (StorageLockedException) assertionValue4;
            final var assertionValue3Argument1 = e.getMessage().contains(db.toString());
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument1 = e.getMessage().contains(db + ".lock");
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue2Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue2Argument2);
            Assertions.assertNotNull(first, "First should be present.");
        }
    }

    @Test
    @DisplayName("닫고 나면 락 파일이 남아 있어도 다시 열 수 있다 — check 1")
    void afterClosingEvenIfTheLockFileRemainsItCanBeOpenedAgainCheck1() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq first = Smq.sqlite(db).open()) {
            Assertions.assertNotNull(first, "First should be present.");
        }
        evaluate(Files.exists(Path.of(db + ".lock")));
        try (Smq secondResource = Smq.sqlite(db).open()) {
            evaluate(secondResource != null);
        }
    }

    @Test
    @DisplayName("닫고 나면 락 파일이 남아 있어도 다시 열 수 있다 — check 2")
    void afterClosingEvenIfTheLockFileRemainsItCanBeOpenedAgainCheck2() {
        final Path db;
        final boolean assertionValue2Argument1;
        db = this.dir.resolve("smq.db");
        try (Smq firstResource = Smq.sqlite(db).open()) {
            evaluate(firstResource != null);
        }
        assertionValue2Argument1 = Files.exists(Path.of(db + ".lock"));
        Assertions.assertTrue(assertionValue2Argument1, "Of should exist.");
        try (Smq secondResource = Smq.sqlite(db).open()) {
            evaluate(secondResource != null);
        }
    }

    @Test
    @DisplayName("닫고 나면 락 파일이 남아 있어도 다시 열 수 있다 — check 3")
    void afterClosingEvenIfTheLockFileRemainsItCanBeOpenedAgainCheck3() {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq firstResource = Smq.sqlite(db).open()) {
            evaluate(firstResource != null);
        }
        evaluate(Files.exists(Path.of(db + ".lock")));
        try (Smq second = Smq.sqlite(db).open()) {
            Assertions.assertNotNull(second, "Second should be present.");
        }
    }

    @Test
    @Timeout(60L)
    @DisplayName("다른 JVM이 쥔 파일은 열 수 없고 그 JVM이 죽으면 다시 열 수 있다 — check 1")
    void filesHeldByAnotherJVMCannotBeOpenedButTheyCanBeOpenedAgainOnceThatJVMTerminatesCheck1() throws Exception {
        final Path db;
        final Process child;
        db = this.dir.resolve("smq.db");
        child = this.startHolder(db);
        try {
            final Throwable assertionValue1;
            assertionValue1 = captureThrowable(() -> Smq.sqlite(db).open());
            Assertions.assertInstanceOf(
                    StorageLockedException.class, assertionValue1, "Open should throw StorageLockedException.");
        } finally {
            child.destroyForcibly();
            evaluate(child.waitFor(30L, TimeUnit.SECONDS));
        }
        try (Smq reopenedResource = Smq.sqlite(db).open()) {
            evaluate(reopenedResource != null);
        }
    }

    @Test
    @Timeout(60L)
    @DisplayName("다른 JVM이 쥔 파일은 열 수 없고 그 JVM이 죽으면 다시 열 수 있다 — check 2")
    void filesHeldByAnotherJVMCannotBeOpenedButTheyCanBeOpenedAgainOnceThatJVMTerminatesCheck2() throws Exception {
        final Path db;
        final Process child;
        db = this.dir.resolve("smq.db");
        child = this.startHolder(db);
        try {
            evaluate(captureThrowable(() -> Smq.sqlite(db).open()));
        } finally {
            final boolean assertionValue2Argument1;
            child.destroyForcibly();
            assertionValue2Argument1 = child.waitFor(30L, TimeUnit.SECONDS);
            Assertions.assertTrue(assertionValue2Argument1, "The child process should exit before the timeout.");
        }
        try (Smq reopenedResource = Smq.sqlite(db).open()) {
            evaluate(reopenedResource != null);
        }
    }

    @Test
    @Timeout(60L)
    @DisplayName("다른 JVM이 쥔 파일은 열 수 없고 그 JVM이 죽으면 다시 열 수 있다 — check 3")
    void filesHeldByAnotherJVMCannotBeOpenedButTheyCanBeOpenedAgainOnceThatJVMTerminatesCheck3() throws Exception {
        final Path db;
        final Process child;
        db = this.dir.resolve("smq.db");
        child = this.startHolder(db);
        try {
            evaluate(captureThrowable(() -> Smq.sqlite(db).open()));
        } finally {
            child.destroyForcibly();
            evaluate(child.waitFor(30L, TimeUnit.SECONDS));
        }
        try (Smq reopened = Smq.sqlite(db).open()) {
            Assertions.assertNotNull(reopened, "Reopened should be present.");
        }
    }

    @Test
    @Timeout(20L)
    @DisplayName("외부 도구가 DB를 쥐고 있으면 기다리지 않고 즉시 StorageLockedException이다 — check 1")
    void ifAnExternalToolHoldsTheDBAStorageLockedExceptionIsThrownImmediatelyWithoutWaitingCheck1() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            Assertions.assertNotNull(smq, "Queue manager should be present.");
        }
        try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = external.createStatement()) {
            final long start;
            s.execute("PRAGMA locking_mode = EXCLUSIVE");
            s.execute("BEGIN EXCLUSIVE");
            start = System.nanoTime();
            evaluate(captureThrowable(() -> Smq.sqlite(db).open()));
            evaluate(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10L)) < 0);
        }
    }

    @Test
    @Timeout(20L)
    @DisplayName("외부 도구가 DB를 쥐고 있으면 기다리지 않고 즉시 StorageLockedException이다 — check 2")
    void ifAnExternalToolHoldsTheDBAStorageLockedExceptionIsThrownImmediatelyWithoutWaitingCheck2() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq smqResource = Smq.sqlite(db).open()) {
            evaluate(smqResource != null);
        }
        try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = external.createStatement()) {
            final long start;
            final Throwable assertionValue2;
            s.execute("PRAGMA locking_mode = EXCLUSIVE");
            s.execute("BEGIN EXCLUSIVE");
            start = System.nanoTime();
            assertionValue2 = captureThrowable(() -> Smq.sqlite(db).open());
            Assertions.assertInstanceOf(
                    StorageLockedException.class, assertionValue2, "Open should throw StorageLockedException.");
            evaluate(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10L)) < 0);
        }
    }

    @Test
    @Timeout(20L)
    @DisplayName("외부 도구가 DB를 쥐고 있으면 기다리지 않고 즉시 StorageLockedException이다 — check 3")
    void ifAnExternalToolHoldsTheDBAStorageLockedExceptionIsThrownImmediatelyWithoutWaitingCheck3() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq smqResource = Smq.sqlite(db).open()) {
            evaluate(smqResource != null);
        }
        try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = external.createStatement()) {
            final long start;
            final boolean assertionValue3Argument1;
            s.execute("PRAGMA locking_mode = EXCLUSIVE");
            s.execute("BEGIN EXCLUSIVE");
            start = System.nanoTime();
            evaluate(captureThrowable(() -> Smq.sqlite(db).open()));
            assertionValue3Argument1 =
                    Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10L)) < 0;
            Assertions.assertTrue(assertionValue3Argument1, "Compare to should be less than 0.");
        }
    }

    @Test
    @DisplayName("smq가 열려 있는 동안 외부 연결은 읽을 수 없다")
    void externalConnectionsCannotBeReadWhileTheSmqIsOpen() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            smq.declare("q", QueueOptions.defaults());
            try (Connection external = DriverManager.getConnection("jdbc:sqlite:" + db);
                    Statement s = external.createStatement()) {
                Assertions.assertThrows(
                        SQLException.class,
                        () -> s.executeQuery("SELECT count(*) FROM message"),
                        "Execute query should throw SQLException.");
            }
        }
    }

    @Test
    @DisplayName("이전 다중 프로세스 사용이 남긴 shm 파일이 있어도 열린다")
    void itOpensEvenIfThereIsAnShmFileLeftByPreviousMultiProcessUsage() throws Exception {
        final Path db;
        db = this.dir.resolve("smq.db");
        try (Smq smq = Smq.sqlite(db).open()) {
            Assertions.assertNotNull(smq, "Queue manager should be present.");
        }
        Files.write(Path.of(db + "-shm"), new byte[32_768]);
        try (Smq reopened = Smq.sqlite(db).open()) {
            reopened.declare("q", QueueOptions.defaults());
        }
    }

    @Test
    @DisplayName("락 파일을 만들 수 없으면 StorageLockedException이 아니라 SmqStorageException이다 — check 1")
    void ifALockFileCannotBeCreatedItIsSmqStorageExceptionNotStorageLockedException() {
        final Path db;
        final SmqStorageException e;
        db = this.dir.resolve("없는_디렉터리").resolve("smq.db");
        final var assertionValue2 = captureThrowable(() -> Smq.sqlite(db).open());
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue2, "Open should throw SmqStorageException.");
        e = (SmqStorageException) (SmqStorageException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains(db + ".lock");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("락 파일을 만들 수 없으면 StorageLockedException이 아니라 SmqStorageException이다 — check 2")
    void ifALockFileCannotBeCreatedItIsSmqStorageExceptionNotStorageLockedExceptionCheck2() {
        final Path db;
        final SmqStorageException e;
        db = this.dir.resolve("없는_디렉터리").resolve("smq.db");
        final var assertionValue2 = captureThrowable(() -> Smq.sqlite(db).open());
        e = (SmqStorageException) (SmqStorageException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains(db + ".lock");
        final var assertionValue1Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
    }

    @Test
    @DisplayName("스키마 판정이 거부해도 BUSY로 오인하지 않고 락을 풀어 다시 열어도 같은 거부다 — check 1")
    void
            evenIfTheSchemaDeterminationIsRejectedItIsNotMistakenlyIdentifiedAsBUSYAndEvenIfTheLockIsReleasedAndReopenedTheSameRejectionOccursCheck1()
                    throws Exception {
        final Path newer;
        final Path foreign;
        newer = this.dir.resolve("newer.db");
        try (Smq smq = Smq.sqlite(newer).open()) {
            Assertions.assertNotNull(smq, "Queue manager should be present.");
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + newer);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA user_version = 999");
        }
        foreign = this.dir.resolve("foreign.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + foreign);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t(x)");
        }
        for (final Path db : new Path[] {newer, foreign}) {
            for (int i = 0; i < 2; i++) {
                evaluate(captureThrowable(() -> Smq.sqlite(db).open()));
            }
        }
    }

    @Test
    @DisplayName("스키마 판정이 거부해도 BUSY로 오인하지 않고 락을 풀어 다시 열어도 같은 거부다 — check 2")
    void
            evenIfTheSchemaDeterminationIsRejectedItIsNotMistakenlyIdentifiedAsBUSYAndEvenIfTheLockIsReleasedAndReopenedTheSameRejectionOccursCheck2()
                    throws Exception {
        final Path newer;
        final Path foreign;
        newer = this.dir.resolve("newer.db");
        try (Smq smqResource = Smq.sqlite(newer).open()) {
            evaluate(smqResource != null);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + newer);
                Statement s = c.createStatement()) {
            s.execute("PRAGMA user_version = 999");
        }
        foreign = this.dir.resolve("foreign.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + foreign);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t(x)");
        }
        for (final Path db : new Path[] {newer, foreign}) {
            for (int i = 0; i < 2; i++) {
                final Throwable assertionValue2;
                assertionValue2 = captureThrowable(() -> Smq.sqlite(db).open());
                Assertions.assertInstanceOf(IncompatibleStorageException.class, assertionValue2, db.toString());
            }
        }
    }

    private Process startHolder(final Path db) throws Exception {
        final String java;
        final Process p;
        java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        p = new ProcessBuilder(
                        java,
                        "--enable-native-access=ALL-UNNAMED",
                        "-cp",
                        System.getProperty("java.class.path"),
                        LockHolderMain.class.getName(),
                        db.toString())
                .redirectError(Redirect.INHERIT)
                .start();
        try (BufferedReader in =
                new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            Assertions.assertEquals("READY", in.readLine(), "Read line should match the expected value.");
        }
        return p;
    }

    private static Throwable captureThrowable(final Executable action) {
        return Assertions.assertThrows(Throwable.class, action);
    }

    private static Object evaluate(final Object value) {
        return value;
    }

    private static Object evaluatePmdArgument(final Object value) {
        return value;
    }
}
