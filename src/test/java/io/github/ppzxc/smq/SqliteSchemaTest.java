package io.github.ppzxc.smq;

import io.github.ppzxc.smq.storage.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class SqliteSchemaTest {

    @TempDir
    Path dir;

    private Path db() {
        return this.dir.resolve("smq.db");
    }

    private static void jdbc(final Path db, final String... statements) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement()) {
            for (final String sql : statements) {
                s.execute(sql);
            }
        }
    }

    private static int userVersion(final Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("PRAGMA user_version")) {
            return rs.getInt(1);
        }
    }

    private int supportedVersion() throws Exception {
        final Path probe;
        probe = this.dir.resolve("probe.db");
        Smq.sqlite(probe).open().close();
        return userVersion(probe);
    }

    private static OutgoingMessage msg(final String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("더 높은 버전의 smq 파일은 발견한 버전과 지원하는 버전을 적어 거부하고 파일을 건드리지 않는다 — check 1")
    void
            ifAHigherVersionOfTheSmqFileIsFoundItIsRejectedByNotingTheDetectedVersionAndTheSupportedVersionAndTheFileIsLeftUntouched()
                    throws Exception {
        final int supported;
        final int newer;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        newer = supported + 5;
        jdbc(this.db(), "PRAGMA user_version = " + newer);
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue4, "Open should throw IncompatibleStorageException.");
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found " + newer);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("더 높은 버전의 smq 파일은 발견한 버전과 지원하는 버전을 적어 거부하고 파일을 건드리지 않는다 — check 2")
    void
            ifAHigherVersionOfTheSmqFileIsFoundItIsRejectedByNotingTheDetectedVersionAndTheSupportedVersionAndTheFileIsLeftUntouchedCheck2()
                    throws Exception {
        final int supported;
        final int newer;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        newer = supported + 5;
        jdbc(this.db(), "PRAGMA user_version = " + newer);
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found " + newer);
        final var assertionValue3Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("더 높은 버전의 smq 파일은 발견한 버전과 지원하는 버전을 적어 거부하고 파일을 건드리지 않는다 — check 3")
    void
            ifAHigherVersionOfTheSmqFileIsFoundItIsRejectedByNotingTheDetectedVersionAndTheSupportedVersionAndTheFileIsLeftUntouchedCheck3()
                    throws Exception {
        final int supported;
        final int newer;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        newer = supported + 5;
        jdbc(this.db(), "PRAGMA user_version = " + newer);
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found " + newer);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        final var assertionValue2Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("더 높은 버전의 smq 파일은 발견한 버전과 지원하는 버전을 적어 거부하고 파일을 건드리지 않는다 — check 4")
    void
            ifAHigherVersionOfTheSmqFileIsFoundItIsRejectedByNotingTheDetectedVersionAndTheSupportedVersionAndTheFileIsLeftUntouchedCheck4()
                    throws Exception {
        final int supported;
        final int newer;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        newer = supported + 5;
        jdbc(this.db(), "PRAGMA user_version = " + newer);
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found " + newer);
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        Assertions.assertEquals(newer, assertionValue1Argument2, "User version should match the expected value.");
    }

    @Test
    @DisplayName("같은 버전의 smq 파일은 데이터를 그대로 둔 채 열린다")
    void smqFilesOfTheSameVersionAreOpenedWithTheDataLeftIntact() throws Exception {
        try (Smq first = Smq.sqlite(this.db()).open()) {
            first.declare("q", QueueOptions.defaults()).publish(msg("a"));
        }
        try (Smq second = Smq.sqlite(this.db()).open()) {
            Assertions.assertEquals(1L, second.queue("q").counts().ready(), "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("같은 버전의 smq 파일을 다시 열어도 버전이 유지된다")
    void reopeningSameVersionFilePreservesVersion() throws Exception {
        final int supported;
        supported = this.supportedVersion();
        try (Smq first = Smq.sqlite(this.db()).open()) {
            first.declare("q", QueueOptions.defaults()).publish(msg("a"));
        }
        Smq.sqlite(this.db()).open().close();
        Assertions.assertEquals(supported, userVersion(this.db()), "The schema version should remain unchanged.");
    }

    @Test
    @DisplayName("smq가 아닌 비어 있지 않은 파일은 거부하고 망가뜨리지 않는다 — check 1")
    void nonEmptyFilesThatAreNotSmqFilesAreRejectedAndNotCorruptedCheck1() throws Exception {
        final Throwable assertionValue1;
        jdbc(this.db(), "CREATE TABLE foreign_table(x)", "INSERT INTO foreign_table VALUES (1)");
        assertionValue1 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue1, "Open should throw IncompatibleStorageException.");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + this.db());
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM foreign_table")) {
            evaluate(rs.getInt(1));
        }
    }

    @Test
    @DisplayName("smq가 아닌 비어 있지 않은 파일은 거부하고 망가뜨리지 않는다 — check 2")
    void nonEmptyFilesThatAreNotSmqFilesAreRejectedAndNotCorruptedCheck2() throws Exception {
        jdbc(this.db(), "CREATE TABLE foreign_table(x)", "INSERT INTO foreign_table VALUES (1)");
        evaluate(captureThrowable(() -> Smq.sqlite(this.db()).open()));
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + this.db());
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM foreign_table")) {
            final int assertionValue2Argument2;
            assertionValue2Argument2 = rs.getInt(1);
            Assertions.assertEquals(1, assertionValue2Argument2, "Int should match the expected value.");
        }
    }

    @Test
    @DisplayName("sqlite 파일 형식이 아닌 파일도 거부한다 — check 1")
    void filesThatAreNotInTheSQLiteFileFormatAreAlsoRejectedCheck1() throws Exception {
        final Throwable assertionValue1;
        Files.writeString(this.db(), "this is not a database file at all, just some text\n".repeat(20));
        assertionValue1 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue1, "Open should throw IncompatibleStorageException.");
        evaluate(Files.readString(this.db()).startsWith("this is not a database"));
    }

    @Test
    @DisplayName("sqlite 파일 형식이 아닌 파일도 거부한다 — check 2")
    void filesThatAreNotInTheSQLiteFileFormatAreAlsoRejectedCheck2() throws Exception {
        final boolean assertionValue2Argument1;
        Files.writeString(this.db(), "this is not a database file at all, just some text\n".repeat(20));
        evaluate(captureThrowable(() -> Smq.sqlite(this.db()).open()));
        assertionValue2Argument1 = Files.readString(this.db()).startsWith("this is not a database");
        Assertions.assertTrue(assertionValue2Argument1, "The original file contents should remain intact.");
    }

    @Test
    @DisplayName("다른 표식이 있는 파일은 테이블이 없어도 거부한다 — check 1")
    void filesWithDifferentMarkersAreRejectedEvenIfThereIsNoTable() throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        jdbc(this.db(), "PRAGMA application_id = 12345");
        final var assertionValue3 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue3, "Open should throw IncompatibleStorageException.");
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
        final var assertionValue2Argument1 = e.getMessage().contains("application_id=12345");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("다른 표식이 있는 파일은 테이블이 없어도 거부한다 — check 2")
    void filesWithDifferentMarkersAreRejectedEvenIfThereIsNoTableCheck2() throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        jdbc(this.db(), "PRAGMA application_id = 12345");
        final var assertionValue3 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
        final var assertionValue2Argument1 = e.getMessage().contains("application_id=12345");
        final var assertionValue2Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        final var assertionValue1Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("다른 표식이 있는 파일은 테이블이 없어도 거부한다 — check 3")
    void filesWithDifferentMarkersAreRejectedEvenIfThereIsNoTableCheck3() throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        jdbc(this.db(), "PRAGMA application_id = 12345");
        final var assertionValue3 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
        final var assertionValue2Argument1 = e.getMessage().contains("application_id=12345");
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument1 = e.getMessage().contains("supported " + supported);
        final var assertionValue1Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
    }

    @Test
    @DisplayName("다른 표식이 있으면 user version이 지원하는 버전과 같아도 거부한다 — check 1")
    void ifThereIsADifferentMarkerTheFileIsRejectedEvenIfTheUserVersionIsTheSameAsTheSupportedVersion()
            throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        jdbc(this.db(), "PRAGMA application_id = 12345", "PRAGMA user_version = " + supported);
        final var assertionValue2 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue2, "Open should throw IncompatibleStorageException.");
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains("application_id=12345");
        evaluatePmdArgument(assertionValue1Argument1);
        final var assertionValue1Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("다른 표식이 있으면 user version이 지원하는 버전과 같아도 거부한다 — check 2")
    void ifThereIsADifferentMarkerTheFileIsRejectedEvenIfTheUserVersionIsTheSameAsTheSupportedVersionCheck2()
            throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        jdbc(this.db(), "PRAGMA application_id = 12345", "PRAGMA user_version = " + supported);
        final var assertionValue2 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue2;
        final var assertionValue1Argument1 = e.getMessage().contains("application_id=12345");
        final var assertionValue1Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
    }

    @Test
    @DisplayName("크기가 0인 파일은 새 파일처럼 열린다")
    void filesWithASizeOf0OpenLikeNewFiles() throws Exception {
        Files.createFile(this.db());
        try (Smq smq = Smq.sqlite(this.db()).open()) {
            Assertions.assertNotNull(smq.declare("q", QueueOptions.defaults()), "Declare should be present.");
        }
    }

    @Test
    @DisplayName("지원하는 버전보다 낮은 smq 파일은 업그레이드 틀을 거치되 마이그레이션이 없으면 거부한다 — check 1")
    void smqFilesWithAVersionLowerThanTheSupportedVersionGoThroughTheUpgradeProcessButAreRejectedIfThereIsNoMigration()
            throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        jdbc(this.db(), "PRAGMA user_version = 0");
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        Assertions.assertInstanceOf(
                IncompatibleStorageException.class, assertionValue4, "Open should throw IncompatibleStorageException.");
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found 0");
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("지원하는 버전보다 낮은 smq 파일은 업그레이드 틀을 거치되 마이그레이션이 없으면 거부한다 — check 2")
    void
            smqFilesWithAVersionLowerThanTheSupportedVersionGoThroughTheUpgradeProcessButAreRejectedIfThereIsNoMigrationCheck2()
                    throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        jdbc(this.db(), "PRAGMA user_version = 0");
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found 0");
        final var assertionValue3Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("지원하는 버전보다 낮은 smq 파일은 업그레이드 틀을 거치되 마이그레이션이 없으면 거부한다 — check 3")
    void
            smqFilesWithAVersionLowerThanTheSupportedVersionGoThroughTheUpgradeProcessButAreRejectedIfThereIsNoMigrationCheck3()
                    throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        jdbc(this.db(), "PRAGMA user_version = 0");
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found 0");
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        final var assertionValue2Argument2 = e.getMessage();
        Assertions.assertTrue(assertionValue2Argument1, assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        evaluatePmdArgument(assertionValue1Argument2);
    }

    @Test
    @DisplayName("지원하는 버전보다 낮은 smq 파일은 업그레이드 틀을 거치되 마이그레이션이 없으면 거부한다 — check 4")
    void
            smqFilesWithAVersionLowerThanTheSupportedVersionGoThroughTheUpgradeProcessButAreRejectedIfThereIsNoMigrationCheck4()
                    throws Exception {
        final int supported;
        final IncompatibleStorageException e;
        supported = this.supportedVersion();
        Smq.sqlite(this.db()).open().close();
        jdbc(this.db(), "PRAGMA user_version = 0");
        final var assertionValue4 = captureThrowable(() -> Smq.sqlite(this.db()).open());
        e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
        final var assertionValue3Argument1 = e.getMessage().contains("found 0");
        evaluatePmdArgument(assertionValue3Argument1);
        final var assertionValue3Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue3Argument2);
        final var assertionValue2Argument1 = e.getMessage().contains("supported " + supported);
        evaluatePmdArgument(assertionValue2Argument1);
        final var assertionValue2Argument2 = e.getMessage();
        evaluatePmdArgument(assertionValue2Argument2);
        final var assertionValue1Argument2 = userVersion(this.db());
        Assertions.assertEquals(0, assertionValue1Argument2, "User version should match the expected value.");
    }

    @Test
    @DisplayName("견본은 열기 전 버전 1이다")
    void sampleStartsAtVersion1() throws Exception {
        final Path copy;
        copy = StorageFactory.copyV1Sample(this.db());
        Assertions.assertEquals(1, userVersion(copy), "User version should match the expected value.");
    }

    @Test
    @DisplayName("버전 1 견본을 열면 버전 2로 올라간다")
    void openingAVersion1SampleUpgradesItToVersion2() throws Exception {
        final Path copy;
        copy = StorageFactory.copyV1Sample(this.db());
        Smq.sqlite(copy).open().close();
        Assertions.assertEquals(2, userVersion(copy), "User version should match the expected value.");
    }

    @Test
    @DisplayName("v1 변환이 실패하면 롤백되어 다시 올릴 수 있다 — check 1")
    void failedV1UpgradeRollsBackAndCanBeRetriedCheck1() throws Exception {
        final Path copy;
        final Throwable assertionValue1;
        copy = StorageFactory.copyV1Sample(this.db());
        jdbc(
                copy,
                "UPDATE message SET available_at = lease_deadline WHERE lease_token IS NOT NULL",
                "CREATE TRIGGER fail_upgrade BEFORE UPDATE OF available_at ON message\nWHEN OLD.lease_token IS NOT NULL\nBEGIN SELECT RAISE(ABORT, 'injected upgrade failure'); END\n");
        assertionValue1 = captureThrowable(() -> Smq.sqlite(copy).open());
        Assertions.assertInstanceOf(
                SmqStorageException.class, assertionValue1, "Open should throw SmqStorageException.");
        evaluate(userVersion(copy));
        jdbc(copy, "DROP TRIGGER fail_upgrade");
        Smq.sqlite(copy).open().close();
        evaluate(userVersion(copy));
    }

    @Test
    @DisplayName("v1 변환이 실패하면 롤백되어 다시 올릴 수 있다 — check 2")
    void failedV1UpgradeRollsBackAndCanBeRetriedCheck2() throws Exception {
        final Path copy;
        final int assertionValue2Argument2;
        copy = StorageFactory.copyV1Sample(this.db());
        jdbc(
                copy,
                "UPDATE message SET available_at = lease_deadline WHERE lease_token IS NOT NULL",
                "CREATE TRIGGER fail_upgrade BEFORE UPDATE OF available_at ON message\nWHEN OLD.lease_token IS NOT NULL\nBEGIN SELECT RAISE(ABORT, 'injected upgrade failure'); END\n");
        evaluate(captureThrowable(() -> Smq.sqlite(copy).open()));
        assertionValue2Argument2 = userVersion(copy);
        Assertions.assertEquals(1, assertionValue2Argument2, "User version should match the expected value.");
        jdbc(copy, "DROP TRIGGER fail_upgrade");
        Smq.sqlite(copy).open().close();
        evaluate(userVersion(copy));
    }

    @Test
    @DisplayName("v1 변환이 실패하면 롤백되어 다시 올릴 수 있다 — check 3")
    void failedV1UpgradeRollsBackAndCanBeRetriedCheck3() throws Exception {
        final Path copy;
        final int assertionValue3Argument2;
        copy = StorageFactory.copyV1Sample(this.db());
        jdbc(
                copy,
                "UPDATE message SET available_at = lease_deadline WHERE lease_token IS NOT NULL",
                "CREATE TRIGGER fail_upgrade BEFORE UPDATE OF available_at ON message\nWHEN OLD.lease_token IS NOT NULL\nBEGIN SELECT RAISE(ABORT, 'injected upgrade failure'); END\n");
        evaluate(captureThrowable(() -> Smq.sqlite(copy).open()));
        evaluate(userVersion(copy));
        jdbc(copy, "DROP TRIGGER fail_upgrade");
        Smq.sqlite(copy).open().close();
        assertionValue3Argument2 = userVersion(copy);
        Assertions.assertEquals(2, assertionValue3Argument2, "User version should match the expected value.");
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
