package io.github.ppzxc.smq.storage.sqlite;

import io.github.ppzxc.smq.Durability;
import io.github.ppzxc.smq.IncompatibleStorageException;
import io.github.ppzxc.smq.PolicyException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class SqliteSchemaModuleTest {

    @TempDir
    Path dir;

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + this.dir.resolve("smq.db"));
    }

    private static void exec(final Connection c, final String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static String query(final Connection c, final String sql) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            return rs.getString(1);
        }
    }

    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "Rollback must run for errors as well as exceptions before the failure is rethrown.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private static int establishInTransaction(final SqliteSchema schema, final Connection c) throws SQLException {
        exec(c, "BEGIN IMMEDIATE");
        try {
            final int version;
            version = schema.establish(c);
            exec(c, "COMMIT");
            return version;
        } catch (Throwable t) {
            exec(c, "ROLLBACK");
            throw t;
        }
    }

    private static SqliteSchema production() {
        return new SqliteSchema(1, Map.of());
    }

    private void createV1File() throws SQLException {
        try (Connection c = this.connect()) {
            establishInTransaction(production(), c);
        }
    }

    private static SqliteSchema schemaV2WithQueueNote() {
        return new SqliteSchema(2, Map.of(1, conn -> {
            exec(conn, "ALTER TABLE queue ADD COLUMN note TEXT");
            exec(conn, "UPDATE queue SET note = upper(name)");
        }));
    }

    @Test
    @DisplayName("빈 파일이면 v1을 만들고 버전을 돌려준다")
    void ifTheFileIsEmptyCreateV1AndReturnTheVersion() throws Exception {
        try (Connection c = this.connect()) {
            Assertions.assertEquals(
                    1,
                    establishInTransaction(production(), c),
                    "Establish in transaction should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 v1 파일에 application id를 기록한다")
    void newV1FileHasApplicationId() throws Exception {
        try (Connection c = this.connect()) {
            establishInTransaction(production(), c);
            Assertions.assertEquals(
                    String.valueOf(1_397_575_985),
                    query(c, "PRAGMA application_id"),
                    "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 v1 파일에 버전을 기록한다")
    void newV1FileHasVersionOne() throws Exception {
        try (Connection c = this.connect()) {
            establishInTransaction(production(), c);
            Assertions.assertEquals("1", query(c, "PRAGMA user_version"), "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("새 v1 파일의 리스 순번은 0이다")
    void newV1FileStartsWithZeroLeaseSequence() throws Exception {
        try (Connection c = this.connect()) {
            establishInTransaction(production(), c);
            Assertions.assertEquals(
                    "0",
                    query(c, "SELECT value FROM meta WHERE key = 'lease_seq'"),
                    "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("application id가 다른 파일은 smq 파일이 아니라고 거부한다 — check 1")
    void filesWithDifferentApplicationIDsAreRejectedAsNonSMQFiles() throws Exception {
        try (Connection c = this.connect()) {
            final IncompatibleStorageException e;
            exec(c, "PRAGMA application_id = 12345");
            final var assertionValue2 = captureThrowable(() -> establishInTransaction(production(), c));
            Assertions.assertInstanceOf(
                    IncompatibleStorageException.class,
                    assertionValue2,
                    "Establish in transaction should throw IncompatibleStorageException.");
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue2;
            final var assertionValue1Argument1 = e.getMessage().startsWith("not an smq database");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("application id가 다른 파일은 smq 파일이 아니라고 거부한다 — check 2")
    void filesWithDifferentApplicationIDsAreRejectedAsNonSMQFilesCheck2() throws Exception {
        try (Connection c = this.connect()) {
            final IncompatibleStorageException e;
            exec(c, "PRAGMA application_id = 12345");
            final var assertionValue2 = captureThrowable(() -> establishInTransaction(production(), c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue2;
            final var assertionValue1Argument1 = e.getMessage().startsWith("not an smq database");
            final var assertionValue1Argument2 = e.getMessage();
            Assertions.assertTrue(assertionValue1Argument1, assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("버전 1만 지원하면 버전 2 파일을 거부한다 — check 1")
    void ifOnlyVersion1IsSupportedVersion2FilesAreRejected() throws Exception {
        SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH).close();
        try (Connection c = this.connect()) {
            final IncompatibleStorageException e;
            final var assertionValue3 = captureThrowable(() -> establishInTransaction(production(), c));
            Assertions.assertInstanceOf(
                    IncompatibleStorageException.class,
                    assertionValue3,
                    "Establish in transaction should throw IncompatibleStorageException.");
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
            final var assertionValue2Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 = query(c, "PRAGMA user_version");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("버전 1만 지원하면 버전 2 파일을 거부한다 — check 2")
    void ifOnlyVersion1IsSupportedVersion2FilesAreRejectedCheck2() throws Exception {
        SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH).close();
        try (Connection c = this.connect()) {
            final IncompatibleStorageException e;
            final var assertionValue3 = captureThrowable(() -> establishInTransaction(production(), c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
            final var assertionValue2Argument2 = e.getMessage();
            Assertions.assertEquals(
                    "unsupported schema version: found 2, supported 1",
                    assertionValue2Argument2,
                    "Message should match the expected value.");
            final var assertionValue1Argument2 = query(c, "PRAGMA user_version");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("버전 1만 지원하면 버전 2 파일을 거부한다 — check 3")
    void ifOnlyVersion1IsSupportedVersion2FilesAreRejectedCheck3() throws Exception {
        SqliteStorage.open(this.dir.resolve("smq.db"), Durability.PROCESS_CRASH).close();
        try (Connection c = this.connect()) {
            final IncompatibleStorageException e;
            final var assertionValue3 = captureThrowable(() -> establishInTransaction(production(), c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue3;
            final var assertionValue2Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 = query(c, "PRAGMA user_version");
            Assertions.assertEquals("2", assertionValue1Argument2, "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("v1 파일을 v2로 올리면 DDL 데이터 변환 버전이 한 트랜잭션에서 일어난다")
    void whenUpgradingAV1FileToV2TheDDLDataConversionOccursInASingleTransaction() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            exec(c, "INSERT INTO queue(name) VALUES ('q')");
            Assertions.assertEquals(
                    2, establishInTransaction(schemaV2WithQueueNote(), c), "The upgrade should return version two.");
        }
    }

    @Test
    @DisplayName("v1에서 v2로 올리면 버전을 기록한다")
    void upgradingV1ToV2RecordsTheVersion() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            exec(c, "INSERT INTO queue(name) VALUES ('q')");
            establishInTransaction(schemaV2WithQueueNote(), c);
            Assertions.assertEquals("2", query(c, "PRAGMA user_version"), "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("v1에서 v2로 올리면 데이터를 변환한다")
    void upgradingV1ToV2TransformsQueueData() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            exec(c, "INSERT INTO queue(name) VALUES ('q')");
            establishInTransaction(schemaV2WithQueueNote(), c);
            Assertions.assertEquals(
                    "Q", query(c, "SELECT note FROM queue WHERE name = 'q'"), "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("v1에서 v3까지 한 번에 차례로 올린다 — check 1")
    void upgradesFromV1ToV3InOrderCheck1() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            v3 = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"), 2, conn -> {
                final String assertionValue1Argument2;
                exec(conn, "CREATE TABLE step_b AS SELECT count(*) AS n FROM step_a");
                assertionValue1Argument2 = query(conn, "PRAGMA user_version");
                Assertions.assertEquals("2", assertionValue1Argument2, "Query should match the expected value.");
            }));
            evaluate(establishInTransaction(v3, c));
            evaluate(query(c, "PRAGMA user_version"));
            evaluate(query(c, "SELECT n FROM step_b"));
        }
    }

    @Test
    @DisplayName("v1에서 v3까지 한 번에 차례로 올린다 — check 2")
    void upgradesFromV1ToV3InOrderCheck2() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final int assertionValue2Argument2;
            v3 = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"), 2, conn -> {
                exec(conn, "CREATE TABLE step_b AS SELECT count(*) AS n FROM step_a");
                evaluate(query(conn, "PRAGMA user_version"));
            }));
            assertionValue2Argument2 = establishInTransaction(v3, c);
            Assertions.assertEquals(
                    3, assertionValue2Argument2, "Establish in transaction should match the expected value.");
            evaluate(query(c, "PRAGMA user_version"));
            evaluate(query(c, "SELECT n FROM step_b"));
        }
    }

    @Test
    @DisplayName("v1에서 v3까지 한 번에 차례로 올린다 — check 3")
    void upgradesFromV1ToV3InOrderCheck3() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final String assertionValue3Argument2;
            v3 = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"), 2, conn -> {
                exec(conn, "CREATE TABLE step_b AS SELECT count(*) AS n FROM step_a");
                evaluate(query(conn, "PRAGMA user_version"));
            }));
            evaluate(establishInTransaction(v3, c));
            assertionValue3Argument2 = query(c, "PRAGMA user_version");
            Assertions.assertEquals("3", assertionValue3Argument2, "Query should match the expected value.");
            evaluate(query(c, "SELECT n FROM step_b"));
        }
    }

    @Test
    @DisplayName("v1에서 v3까지 한 번에 차례로 올린다 — check 4")
    void upgradesFromV1ToV3InOrderCheck4() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final String assertionValue4Argument2;
            v3 = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"), 2, conn -> {
                exec(conn, "CREATE TABLE step_b AS SELECT count(*) AS n FROM step_a");
                evaluate(query(conn, "PRAGMA user_version"));
            }));
            evaluate(establishInTransaction(v3, c));
            evaluate(query(c, "PRAGMA user_version"));
            assertionValue4Argument2 = query(c, "SELECT n FROM step_b");
            Assertions.assertEquals("0", assertionValue4Argument2, "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("업그레이드 단계가 중간에 실패하면 파일은 원래 버전으로 남는다 — check 1")
    void ifTheUpgradeStepFailsInTheMiddleTheFileRemainsInItsOriginalVersionCheck1() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final Throwable assertionValue1;
            v3 = new SqliteSchema(
                    3,
                    Map.of(
                            1,
                            conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"),
                            2,
                            conn -> exec(conn, "THIS IS NOT SQL")));
            assertionValue1 = captureThrowable(() -> establishInTransaction(v3, c));
            Assertions.assertInstanceOf(
                    SQLException.class, assertionValue1, "Establish in transaction should throw SQLException.");
            evaluate(query(c, "PRAGMA user_version"));
            evaluate(query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'"));
        }
    }

    @Test
    @DisplayName("업그레이드 단계가 중간에 실패하면 파일은 원래 버전으로 남는다 — check 2")
    void ifTheUpgradeStepFailsInTheMiddleTheFileRemainsInItsOriginalVersionCheck2() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final String assertionValue2Argument2;
            v3 = new SqliteSchema(
                    3,
                    Map.of(
                            1,
                            conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"),
                            2,
                            conn -> exec(conn, "THIS IS NOT SQL")));
            evaluate(captureThrowable(() -> establishInTransaction(v3, c)));
            assertionValue2Argument2 = query(c, "PRAGMA user_version");
            Assertions.assertEquals("1", assertionValue2Argument2, "Query should match the expected value.");
            evaluate(query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'"));
        }
    }

    @Test
    @DisplayName("업그레이드 단계가 중간에 실패하면 파일은 원래 버전으로 남는다 — check 3")
    void ifTheUpgradeStepFailsInTheMiddleTheFileRemainsInItsOriginalVersionCheck3() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema v3;
            final String assertionValue3Argument2;
            v3 = new SqliteSchema(
                    3,
                    Map.of(
                            1,
                            conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)"),
                            2,
                            conn -> exec(conn, "THIS IS NOT SQL")));
            evaluate(captureThrowable(() -> establishInTransaction(v3, c)));
            evaluate(query(c, "PRAGMA user_version"));
            assertionValue3Argument2 = query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'");
            Assertions.assertEquals("0", assertionValue3Argument2, "Query should match the expected value.");
        }
    }

    @Test
    @DisplayName("중간 단계가 빠진 목록이면 지원하지 않는 버전으로 거부한다 — check 1")
    void ifTheListIsMissingAnIntermediateStepItIsRejectedAsAnUnsupportedVersion() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema gap;
            final IncompatibleStorageException e;
            gap = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)")));
            final var assertionValue4 = captureThrowable(() -> establishInTransaction(gap, c));
            Assertions.assertInstanceOf(
                    IncompatibleStorageException.class,
                    assertionValue4,
                    "Establish in transaction should throw IncompatibleStorageException.");
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument2 = query(c, "PRAGMA user_version");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 = query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("중간 단계가 빠진 목록이면 지원하지 않는 버전으로 거부한다 — check 2")
    void ifTheListIsMissingAnIntermediateStepItIsRejectedAsAnUnsupportedVersionCheck2() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema gap;
            final IncompatibleStorageException e;
            gap = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)")));
            final var assertionValue4 = captureThrowable(() -> establishInTransaction(gap, c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
            final var assertionValue3Argument2 = e.getMessage();
            Assertions.assertEquals(
                    "unsupported schema version: found 1, supported 3",
                    assertionValue3Argument2,
                    "Message should match the expected value.");
            final var assertionValue2Argument2 = query(c, "PRAGMA user_version");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 = query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("중간 단계가 빠진 목록이면 지원하지 않는 버전으로 거부한다 — check 3")
    void ifTheListIsMissingAnIntermediateStepItIsRejectedAsAnUnsupportedVersionCheck3() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema gap;
            final IncompatibleStorageException e;
            gap = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)")));
            final var assertionValue4 = captureThrowable(() -> establishInTransaction(gap, c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument2 = query(c, "PRAGMA user_version");
            Assertions.assertEquals("1", assertionValue2Argument2, "Query should match the expected value.");
            final var assertionValue1Argument2 = query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'");
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("중간 단계가 빠진 목록이면 지원하지 않는 버전으로 거부한다 — check 4")
    void ifTheListIsMissingAnIntermediateStepItIsRejectedAsAnUnsupportedVersionCheck4() throws Exception {
        this.createV1File();
        try (Connection c = this.connect()) {
            final SqliteSchema gap;
            final IncompatibleStorageException e;
            gap = new SqliteSchema(3, Map.of(1, conn -> exec(conn, "CREATE TABLE step_a (n INTEGER)")));
            final var assertionValue4 = captureThrowable(() -> establishInTransaction(gap, c));
            e = (IncompatibleStorageException) (IncompatibleStorageException) assertionValue4;
            final var assertionValue3Argument2 = e.getMessage();
            evaluatePmdArgument(assertionValue3Argument2);
            final var assertionValue2Argument2 = query(c, "PRAGMA user_version");
            evaluatePmdArgument(assertionValue2Argument2);
            final var assertionValue1Argument2 = query(c, "SELECT count(*) FROM sqlite_schema WHERE name = 'step_a'");
            Assertions.assertEquals("0", assertionValue1Argument2, "Query should match the expected value.");
        }
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
