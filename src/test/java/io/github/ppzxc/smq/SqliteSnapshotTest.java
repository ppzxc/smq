package io.github.ppzxc.smq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

@PolicyException(
        rule = "PMD.CyclomaticComplexity",
        reason =
                "The independent single-assertion tests keep each method below the limit; the class total sums every scenario.",
        approvedBy = "ppzxc",
        reviewBy = "2027-11-28")
@SuppressWarnings("PMD.CyclomaticComplexity")
@Timeout(60L)
class SqliteSnapshotTest {

    @TempDir
    Path dir;

    private Smq open() {
        return Smq.sqlite(this.dir.resolve("smq.db")).open();
    }

    private static OutgoingMessage msg(final String s) {
        return OutgoingMessage.of(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Connection external(final Path file) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + file);
    }

    private static String scalar(final Connection c, final String sql) throws SQLException {
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery(sql)) {
            if (!r.next()) {
                throw new AssertionError("Expected a query result row");
            } else {
                return r.getString(1);
            }
        }
    }

    private static List<String> payloads(final Connection c) throws SQLException {
        final List<String> out;
        out = new ArrayList<>();
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery("SELECT payload FROM message ORDER BY id")) {
            while (r.next()) {
                out.add(new String(r.getBytes(1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    @Test
    @DisplayName("작은따옴표가 있는 경로에 스냅샷을 만들고 기존 파일을 보존한다 — check 1")
    void createASnapshotInAPathWithSingleQuotesAndPreserveTheExistingFileCheck1() throws Exception {
        final Path quotedDir;
        final Path target;
        quotedDir = Files.createDirectory(this.dir.resolve("consumer's snapshots"));
        target = quotedDir.resolve("snapshot.db");
        try (Smq smq = this.open()) {
            final Throwable assertionValue1;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.snapshot(target);
            evaluate(Files.readAllBytes(target));
            assertionValue1 = captureThrowable(() -> smq.snapshot(target));
            Assertions.assertInstanceOf(
                    SmqStorageException.class, assertionValue1, "Snapshot should throw SmqStorageException.");
            evaluate(Files.readAllBytes(target));
        }
        try (Connection c = external(target)) {
            evaluate(payloads(c));
        }
    }

    @Test
    @DisplayName("작은따옴표가 있는 경로에 스냅샷을 만들고 기존 파일을 보존한다 — check 2")
    void createASnapshotInAPathWithSingleQuotesAndPreserveTheExistingFileCheck2() throws Exception {
        final Path quotedDir;
        final Path target;
        quotedDir = Files.createDirectory(this.dir.resolve("consumer's snapshots"));
        target = quotedDir.resolve("snapshot.db");
        try (Smq smq = this.open()) {
            final byte[] snapshot;
            final byte[] assertionValue2Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.snapshot(target);
            snapshot = Files.readAllBytes(target);
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            assertionValue2Argument2 = Files.readAllBytes(target);
            Assertions.assertArrayEquals(
                    snapshot, assertionValue2Argument2, "Read all bytes should match the expected value.");
        }
        try (Connection c = external(target)) {
            evaluate(payloads(c));
        }
    }

    @Test
    @DisplayName("작은따옴표가 있는 경로에 스냅샷을 만들고 기존 파일을 보존한다 — check 3")
    void createASnapshotInAPathWithSingleQuotesAndPreserveTheExistingFileCheck3() throws Exception {
        final Path quotedDir;
        final Path target;
        quotedDir = Files.createDirectory(this.dir.resolve("consumer's snapshots"));
        target = quotedDir.resolve("snapshot.db");
        try (Smq smq = this.open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.snapshot(target);
            evaluate(Files.readAllBytes(target));
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            evaluate(Files.readAllBytes(target));
        }
        try (Connection c = external(target)) {
            final List<String> assertionValue3Argument1;
            final List<String> assertionValue3Argument2;
            assertionValue3Argument1 = List.of("a");
            assertionValue3Argument2 = payloads(c);
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Payloads should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷은 외부 SQLite로 열어 원본과 같은 메시지를 보여 준다 — external Snapshot Passes Integrity Check")
    void externalSnapshotPassesIntegrityCheck() throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final Queue q;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));
            smq.snapshot(target);
        }
        try (Connection c = external(target)) {
            Assertions.assertEquals(
                    "ok", scalar(c, "PRAGMA integrity_check"), "Scalar should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷은 외부 SQLite로 열어 원본과 같은 메시지를 보여 준다 — external Snapshot Preserves Messages")
    void externalSnapshotPreservesMessages() throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final Queue q;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));
            smq.snapshot(target);
        }
        try (Connection c = external(target)) {
            Assertions.assertEquals(List.of("a", "b", "c"), payloads(c), "Payloads should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷은 외부 SQLite로 열어 원본과 같은 메시지를 보여 준다 — external Snapshot Preserves Queue Name")
    void externalSnapshotPreservesQueueName() throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final Queue q;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(List.of(msg("a"), msg("b"), msg("c")));
            smq.snapshot(target);
        }
        try (Connection c = external(target)) {
            Assertions.assertEquals(
                    "orders", scalar(c, "SELECT name FROM queue"), "Scalar should match the expected value.");
        }
    }

    private static Path symlink(final Path link, final Path target) {
        try {
            return Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            Assumptions.abort("symbolic links are not available: " + e);
            throw new AssertionError(e);
        }
    }

    private Set<String> names(final Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.<String>map(e -> e.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    @Test
    @DisplayName("대상이 이미 있으면 예외를 던지고 기존 파일과 디렉터리를 그대로 둔다 — check 1")
    void ifTheTargetAlreadyExistsAnExceptionIsThrownAndTheExistingFilesAndDirectoriesAreLeftAsTheyAreCheck1()
            throws Exception {
        final Path target;
        final byte[] existing;
        target = this.dir.resolve("snap.db");
        existing = "not touched".getBytes(StandardCharsets.UTF_8);
        Files.write(target, existing);
        try (Smq smq = this.open()) {
            final Throwable assertionValue1;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            assertionValue1 = captureThrowable(() -> smq.snapshot(target));
            Assertions.assertInstanceOf(
                    SmqStorageException.class, assertionValue1, "Snapshot should throw SmqStorageException.");
            evaluate(Files.readAllBytes(target));
            evaluate(this.names(this.dir));
        }
    }

    @Test
    @DisplayName("대상이 이미 있으면 예외를 던지고 기존 파일과 디렉터리를 그대로 둔다 — check 2")
    void ifTheTargetAlreadyExistsAnExceptionIsThrownAndTheExistingFilesAndDirectoriesAreLeftAsTheyAreCheck2()
            throws Exception {
        final Path target;
        final byte[] existing;
        target = this.dir.resolve("snap.db");
        existing = "not touched".getBytes(StandardCharsets.UTF_8);
        Files.write(target, existing);
        try (Smq smq = this.open()) {
            final byte[] assertionValue2Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            assertionValue2Argument2 = Files.readAllBytes(target);
            Assertions.assertArrayEquals(
                    existing, assertionValue2Argument2, "Read all bytes should match the expected value.");
            evaluate(this.names(this.dir));
        }
    }

    @Test
    @DisplayName("대상이 이미 있으면 예외를 던지고 기존 파일과 디렉터리를 그대로 둔다 — check 3")
    void ifTheTargetAlreadyExistsAnExceptionIsThrownAndTheExistingFilesAndDirectoriesAreLeftAsTheyAreCheck3()
            throws Exception {
        final Path target;
        final byte[] existing;
        target = this.dir.resolve("snap.db");
        existing = "not touched".getBytes(StandardCharsets.UTF_8);
        Files.write(target, existing);
        try (Smq smq = this.open()) {
            final Set<String> before;
            final Set<String> assertionValue3Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            before = this.names(this.dir);
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            evaluate(Files.readAllBytes(target));
            assertionValue3Argument2 = this.names(this.dir);
            Assertions.assertEquals(before, assertionValue3Argument2, "Names should match the expected value.");
        }
    }

    @Test
    @DisplayName("대상이 실행 중인 DB와 WAL과 락 파일과 겹치면 어떤 경로로 가리켜도 거부한다 — check 1")
    void ifTheTargetOverlapsWithTheRunningDBWALAndLockFileItIsRejectedRegardlessOfThePathItPointsToCheck1()
            throws Exception {
        final Path alias;
        alias = symlink(this.dir.resolveSibling(this.dir.getFileName() + "-alias"), this.dir);
        try (Smq smq = this.open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            for (final String name : List.of("smq.db", "smq.db-wal", "smq.db.lock")) {
                final Throwable assertionValue1;
                assertionValue1 = captureThrowable(() -> smq.snapshot(this.dir.resolve(name)));
                Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue1, name);
                evaluate(captureThrowable(() -> smq.snapshot(alias.resolve(name))));
            }
            evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve("./smq.db"))));
            evaluate(this.names(this.dir));
        } finally {
            Files.delete(alias);
        }
    }

    @Test
    @DisplayName("대상이 실행 중인 DB와 WAL과 락 파일과 겹치면 어떤 경로로 가리켜도 거부한다 — check 2")
    void ifTheTargetOverlapsWithTheRunningDBWALAndLockFileItIsRejectedRegardlessOfThePathItPointsToCheck2()
            throws Exception {
        final Path alias;
        alias = symlink(this.dir.resolveSibling(this.dir.getFileName() + "-alias"), this.dir);
        try (Smq smq = this.open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            for (final String name : List.of("smq.db", "smq.db-wal", "smq.db.lock")) {
                final Throwable assertionValue2;
                evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve(name))));
                assertionValue2 = captureThrowable(() -> smq.snapshot(alias.resolve(name)));
                Assertions.assertInstanceOf(IllegalArgumentException.class, assertionValue2, name);
            }
            evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve("./smq.db"))));
            evaluate(this.names(this.dir));
        } finally {
            Files.delete(alias);
        }
    }

    @Test
    @DisplayName("대상이 실행 중인 DB와 WAL과 락 파일과 겹치면 어떤 경로로 가리켜도 거부한다 — check 3")
    void ifTheTargetOverlapsWithTheRunningDBWALAndLockFileItIsRejectedRegardlessOfThePathItPointsToCheck3()
            throws Exception {
        final Path alias;
        alias = symlink(this.dir.resolveSibling(this.dir.getFileName() + "-alias"), this.dir);
        try (Smq smq = this.open()) {
            final Throwable assertionValue3;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            for (final String name : List.of("smq.db", "smq.db-wal", "smq.db.lock")) {
                evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve(name))));
                evaluate(captureThrowable(() -> smq.snapshot(alias.resolve(name))));
            }
            assertionValue3 = captureThrowable(() -> smq.snapshot(this.dir.resolve("./smq.db")));
            Assertions.assertInstanceOf(
                    IllegalArgumentException.class, assertionValue3, "Snapshot should throw IllegalArgumentException.");
            evaluate(this.names(this.dir));
        } finally {
            Files.delete(alias);
        }
    }

    @Test
    @DisplayName("대상이 실행 중인 DB와 WAL과 락 파일과 겹치면 어떤 경로로 가리켜도 거부한다 — check 4")
    void ifTheTargetOverlapsWithTheRunningDBWALAndLockFileItIsRejectedRegardlessOfThePathItPointsToCheck4()
            throws Exception {
        final Path alias;
        alias = symlink(this.dir.resolveSibling(this.dir.getFileName() + "-alias"), this.dir);
        try (Smq smq = this.open()) {
            final Set<String> before;
            final Set<String> assertionValue4Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            before = this.names(this.dir);
            for (final String name : List.of("smq.db", "smq.db-wal", "smq.db.lock")) {
                evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve(name))));
                evaluate(captureThrowable(() -> smq.snapshot(alias.resolve(name))));
            }
            evaluate(captureThrowable(() -> smq.snapshot(this.dir.resolve("./smq.db"))));
            assertionValue4Argument2 = this.names(this.dir);
            Assertions.assertEquals(before, assertionValue4Argument2, "Names should match the expected value.");
        } finally {
            Files.delete(alias);
        }
    }

    @Test
    @DisplayName("대상의 부모 디렉터리가 없으면 예외를 던지고 디렉터리를 만들지 않는다 — check 1")
    void ifTheTargetSParentDirectoryDoesNotExistAnExceptionIsThrownAndTheDirectoryIsNotCreatedCheck1()
            throws Exception {
        final Path missing;
        missing = this.dir.resolve("no-such-dir");
        try (Smq smq = this.open()) {
            final Throwable assertionValue1;
            smq.declare("orders", QueueOptions.defaults());
            assertionValue1 = captureThrowable(() -> smq.snapshot(missing.resolve("snap.db")));
            Assertions.assertInstanceOf(
                    SmqStorageException.class, assertionValue1, "Snapshot should throw SmqStorageException.");
            evaluate(Files.exists(missing));
        }
    }

    @Test
    @DisplayName("대상의 부모 디렉터리가 없으면 예외를 던지고 디렉터리를 만들지 않는다 — check 2")
    void ifTheTargetSParentDirectoryDoesNotExistAnExceptionIsThrownAndTheDirectoryIsNotCreatedCheck2()
            throws Exception {
        final Path missing;
        missing = this.dir.resolve("no-such-dir");
        try (Smq smq = this.open()) {
            final boolean assertionValue2Argument1;
            smq.declare("orders", QueueOptions.defaults());
            evaluate(captureThrowable(() -> smq.snapshot(missing.resolve("snap.db"))));
            assertionValue2Argument1 = Files.exists(missing);
            Assertions.assertFalse(assertionValue2Argument1, "Missing should not exist.");
        }
    }

    @Test
    @DisplayName("임시 파일을 만든 뒤 연결에 실패하면 임시 파일을 남기지 않는다 — check 1")
    void ifTheConnectionFailsAfterCreatingATemporaryFileTheTemporaryFileIsNotLeftBehindCheck1() throws Exception {
        final Path dangling;
        dangling = symlink(this.dir.resolve("snap.db"), this.dir.resolve("nowhere"));
        try (Smq smq = this.open()) {
            final Throwable assertionValue1;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            assertionValue1 = captureThrowable(() -> smq.snapshot(dangling));
            Assertions.assertInstanceOf(
                    SmqStorageException.class, assertionValue1, "Snapshot should throw SmqStorageException.");
            evaluate(this.names(this.dir));
            evaluate(this.dir.resolve("nowhere"));
            evaluate(Files.readSymbolicLink(dangling));
        }
    }

    @Test
    @DisplayName("임시 파일을 만든 뒤 연결에 실패하면 임시 파일을 남기지 않는다 — check 2")
    void ifTheConnectionFailsAfterCreatingATemporaryFileTheTemporaryFileIsNotLeftBehindCheck2() throws Exception {
        final Path dangling;
        dangling = symlink(this.dir.resolve("snap.db"), this.dir.resolve("nowhere"));
        try (Smq smq = this.open()) {
            final Set<String> before;
            final Set<String> assertionValue2Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            before = this.names(this.dir);
            evaluate(captureThrowable(() -> smq.snapshot(dangling)));
            assertionValue2Argument2 = this.names(this.dir);
            Assertions.assertEquals(before, assertionValue2Argument2, "Names should match the expected value.");
            evaluate(this.dir.resolve("nowhere"));
            evaluate(Files.readSymbolicLink(dangling));
        }
    }

    @Test
    @DisplayName("임시 파일을 만든 뒤 연결에 실패하면 임시 파일을 남기지 않는다 — check 3")
    void ifTheConnectionFailsAfterCreatingATemporaryFileTheTemporaryFileIsNotLeftBehindCheck3() throws Exception {
        final Path dangling;
        dangling = symlink(this.dir.resolve("snap.db"), this.dir.resolve("nowhere"));
        try (Smq smq = this.open()) {
            final Path assertionValue3Argument1;
            final Path assertionValue3Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            evaluate(this.names(this.dir));
            evaluate(captureThrowable(() -> smq.snapshot(dangling)));
            evaluate(this.names(this.dir));
            assertionValue3Argument1 = this.dir.resolve("nowhere");
            assertionValue3Argument2 = Files.readSymbolicLink(dangling);
            Assertions.assertEquals(
                    assertionValue3Argument1,
                    assertionValue3Argument2,
                    "Read symbolic link should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 1")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceeds()
            throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    Assertions.assertNull(assertionValue6Argument1, "Failure should be null.");
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        evaluatePmdArgument(assertionValue5Argument2);
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        evaluatePmdArgument(assertionValue4Argument1);
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        evaluatePmdArgument(assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        evaluatePmdArgument(assertionValue3Argument1);
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        evaluatePmdArgument(assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = initial + published.get();
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts().ready();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 2")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void
            evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceedsCheck2()
                    throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    evaluatePmdArgument(assertionValue6Argument1);
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        Assertions.assertEquals(
                                "ok", assertionValue5Argument2, "Scalar should match the expected value.");
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        evaluatePmdArgument(assertionValue4Argument1);
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        evaluatePmdArgument(assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        evaluatePmdArgument(assertionValue3Argument1);
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        evaluatePmdArgument(assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = initial + published.get();
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts().ready();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 3")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void
            evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceedsCheck3()
                    throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    evaluatePmdArgument(assertionValue6Argument1);
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        evaluatePmdArgument(assertionValue5Argument2);
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        Assertions.assertTrue(assertionValue4Argument1, assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        evaluatePmdArgument(assertionValue3Argument1);
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        evaluatePmdArgument(assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = initial + published.get();
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts().ready();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 4")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void
            evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceedsCheck4()
                    throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    evaluatePmdArgument(assertionValue6Argument1);
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        evaluatePmdArgument(assertionValue5Argument2);
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        evaluatePmdArgument(assertionValue4Argument1);
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        evaluatePmdArgument(assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        Assertions.assertTrue(assertionValue3Argument1, assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = initial + published.get();
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts().ready();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 5")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void
            evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceedsCheck5()
                    throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    evaluatePmdArgument(assertionValue6Argument1);
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        evaluatePmdArgument(assertionValue5Argument2);
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        evaluatePmdArgument(assertionValue4Argument1);
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        evaluatePmdArgument(assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        evaluatePmdArgument(assertionValue3Argument1);
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        evaluatePmdArgument(assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            Assertions.assertNull(assertionValue2Argument1, "Failure should be null.");
            final var assertionValue1Argument1 = initial + published.get();
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = q.counts().ready();
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷을 뜨는 동안 발행이 이어져도 스냅샷은 일관되고 모든 발행이 끝내 성공한다 — check 6")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "The worker forwards every failure, including assertion errors, to the test thread.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void
            evenIfPublishingContinuesWhileTakingASnapshotTheSnapshotRemainsConsistentAndAllPublishingUltimatelySucceedsCheck6()
                    throws Exception {
        final int initial;
        final int rounds;
        final byte[] big;
        final AtomicInteger published;
        final AtomicBoolean stop;
        final AtomicReference<Throwable> failure;
        initial = 1000;
        rounds = 5;
        big = new byte[16_384];
        published = new AtomicInteger();
        stop = new AtomicBoolean();
        failure = new AtomicReference<>();
        try (Smq smq = this.open()) {
            final Queue q;
            final List<OutgoingMessage> bulk;
            final Thread publisher;
            q = smq.declare("orders", QueueOptions.defaults());
            bulk = new ArrayList<>();
            for (int i = 0; i < initial; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            q.publish(bulk);
            publisher = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        q.publish(msg("p"));
                        published.incrementAndGet();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            publisher.start();
            try {
                for (int round = 0; round < rounds; round++) {
                    final int progressed;
                    final Path target;
                    final int before;
                    final int after;
                    progressed = published.get();
                    while (published.get() == progressed && failure.get() == null) {
                        Thread.onSpinWait();
                    }
                    final var assertionValue6Argument1 = failure.get();
                    evaluatePmdArgument(assertionValue6Argument1);
                    target = this.dir.resolve("snap" + round + ".db");
                    before = published.get();
                    smq.snapshot(target);
                    after = published.get();
                    try (Connection c = external(target)) {
                        final int inSnapshot;
                        final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                        evaluatePmdArgument(assertionValue5Argument2);
                        inSnapshot = Integer.parseInt(scalar(c, "SELECT count(*) FROM message"));
                        final var assertionValue4Argument1 = inSnapshot >= initial + before;
                        evaluatePmdArgument(assertionValue4Argument1);
                        final var assertionValue4Argument2 = "snapshot=" + inSnapshot + " before=" + before;
                        evaluatePmdArgument(assertionValue4Argument2);
                        final var assertionValue3Argument1 = inSnapshot <= initial + after + 1;
                        evaluatePmdArgument(assertionValue3Argument1);
                        final var assertionValue3Argument2 = "snapshot=" + inSnapshot + " after=" + after;
                        evaluatePmdArgument(assertionValue3Argument2);
                    }
                }
            } finally {
                stop.set(true);
                publisher.join();
            }
            final var assertionValue2Argument1 = failure.get();
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = initial + published.get();
            final var assertionValue1Argument2 = q.counts().ready();
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷을 뜬 뒤에도 같은 Smq로 발행하고 꺼내고 정산할 수 있다 — check 1")
    void evenAfterTakingASnapshotYouCanPublishRetrieveAndSettleWithTheSameSmqCheck1() throws Exception {
        try (Smq smq = this.open()) {
            final Queue q;
            final Chunk chunk;
            final int assertionValue1Argument2;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(msg("a"));
            smq.snapshot(this.dir.resolve("snap.db"));
            q.publish(msg("b"));
            chunk = q.pull(2, Duration.ZERO);
            assertionValue1Argument2 = chunk.messages().size();
            Assertions.assertEquals(2, assertionValue1Argument2, "Size should match the expected value.");
            evaluate(q.settle(chunk.ackAll()).allOk());
        }
    }

    @Test
    @DisplayName("스냅샷을 뜬 뒤에도 같은 Smq로 발행하고 꺼내고 정산할 수 있다 — check 2")
    void evenAfterTakingASnapshotYouCanPublishRetrieveAndSettleWithTheSameSmqCheck2() throws Exception {
        try (Smq smq = this.open()) {
            final Queue q;
            final Chunk chunk;
            final boolean assertionValue2Argument1;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(msg("a"));
            smq.snapshot(this.dir.resolve("snap.db"));
            q.publish(msg("b"));
            chunk = q.pull(2, Duration.ZERO);
            assertionValue2Argument1 = q.settle(chunk.ackAll()).allOk();
            Assertions.assertTrue(assertionValue2Argument1, "Outcomes should succeed.");
        }
    }

    @Test
    @DisplayName("스냅샷을 뜬 뒤에도 같은 Smq로 발행하고 꺼내고 정산할 수 있다 — check 3")
    void evenAfterTakingASnapshotYouCanPublishRetrieveAndSettleWithTheSameSmqCheck3() throws Exception {
        try (Smq smq = this.open()) {
            final Queue q;
            final Chunk chunk;
            final long assertionValue3Argument2;
            q = smq.declare("orders", QueueOptions.defaults());
            q.publish(msg("a"));
            smq.snapshot(this.dir.resolve("snap.db"));
            q.publish(msg("b"));
            chunk = q.pull(2, Duration.ZERO);
            evaluate(q.settle(chunk.ackAll()).allOk());
            assertionValue3Argument2 = q.counts().ready();
            Assertions.assertEquals(0L, assertionValue3Argument2, "Ready should match the expected value.");
        }
    }

    @Test
    @DisplayName("대상 디렉터리 이름에 작은따옴표가 있어도 스냅샷을 만든다")
    void aSnapshotIsCreatedEvenIfTheTargetDirectoryNameContainsSingleQuotes() throws Exception {
        final Path quoted;
        final Path target;
        quoted = Files.createDirectory(this.dir.resolve("it's here"));
        target = quoted.resolve("snap's.db");
        try (Smq smq = this.open()) {
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.snapshot(target);
        }
        try (Connection c = external(target)) {
            Assertions.assertEquals(List.of("a"), payloads(c), "Payloads should match the expected value.");
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 1")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFile() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            Assertions.assertFalse(assertionValue7Argument1, "스냅샷이 끝나지 않았다");
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 2")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck2() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            Assertions.assertNull(assertionValue6Argument1, "스냅샷이 도는 중 close를 불러도 스냅샷은 성공해야 한다");
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 3")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck3() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                Assertions.assertEquals("ok", assertionValue5Argument2, "Scalar should match the expected value.");
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 4")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck4() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                Assertions.assertEquals(
                        assertionValue4Argument1, assertionValue4Argument2, "Scalar should match the expected value.");
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 5")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck5() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            Assertions.assertEquals(
                    assertionValue3Argument1, assertionValue3Argument2, "Without wal should match the expected value.");
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 6")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck6() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            Assertions.assertFalse(assertionValue2Argument1, "Target should not exist.");
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            evaluatePmdArgument(assertionValue1Argument1);
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue1Argument2);
        }
    }

    @Test
    @DisplayName("스냅샷 도중 다른 스레드가 close를 불러도 스냅샷은 완전한 파일로 끝난다 — check 7")
    @PolicyExceptions({
        @PolicyException(
                rule = "PMD.UseTryWithResources",
                reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26"),
        @PolicyException(
                rule = "PMD.AvoidCatchingGenericException",
                reason = "The worker forwards every failure, including assertion errors, to the test thread.",
                approvedBy = "ppzxc",
                reviewBy = "2027-11-26")
    })
    @SuppressWarnings({"PMD.UseTryWithResources", "PMD.AvoidCatchingGenericException"})
    void evenIfAnotherThreadCallsCloseDuringTheSnapshotTheSnapshotEndsAsACompleteFileCheck7() throws Exception {
        boolean observed;
        final int rows;
        final byte[] big;
        final Path target;
        final AtomicReference<Throwable> failure;
        final Smq smq;
        rows = 4000;
        big = new byte[16_384];
        target = this.dir.resolve("snap.db");
        failure = new AtomicReference<>();
        smq = this.open();
        try {
            final List<OutgoingMessage> bulk;
            final Thread snapshotter;
            final long deadline;
            bulk = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                bulk.add(OutgoingMessage.of(big));
            }
            smq.declare("orders", QueueOptions.defaults()).publish(bulk);
            snapshotter = new Thread(() -> {
                try {
                    smq.snapshot(target);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            snapshotter.start();
            deadline = System.nanoTime() + Duration.ofSeconds(5L).toNanos();
            observed = false;
            while (failure.get() == null && snapshotter.isAlive() && System.nanoTime() < deadline) {
                if (this.snapshotInProgress()) {
                    observed = true;
                    break;
                }
                Thread.onSpinWait();
            }
            smq.close();
            snapshotter.join(Duration.ofSeconds(30L));
            final var assertionValue7Argument1 = snapshotter.isAlive();
            evaluatePmdArgument(assertionValue7Argument1);
        } finally {
            smq.close();
        }
        if (observed) {
            final var assertionValue6Argument1 = failure.get();
            evaluatePmdArgument(assertionValue6Argument1);
        }
        if (failure.get() == null) {
            try (Connection c = external(target)) {
                final var assertionValue5Argument2 = scalar(c, "PRAGMA integrity_check");
                evaluatePmdArgument(assertionValue5Argument2);
                final var assertionValue4Argument1 = String.valueOf(rows);
                evaluatePmdArgument(assertionValue4Argument1);
                final var assertionValue4Argument2 = scalar(c, "SELECT count(*) FROM message");
                evaluatePmdArgument(assertionValue4Argument2);
            }
            final var assertionValue3Argument1 = Set.of("smq.db", "smq.db.lock", "snap.db");
            evaluatePmdArgument(assertionValue3Argument1);
            final var assertionValue3Argument2 = withoutWal(this.names(this.dir));
            evaluatePmdArgument(assertionValue3Argument2);
        } else {
            final var assertionValue2Argument1 = Files.exists(target);
            evaluatePmdArgument(assertionValue2Argument1);
            final var assertionValue1Argument1 = Set.of("smq.db", "smq.db.lock");
            final var assertionValue1Argument2 = withoutWal(this.names(this.dir));
            Assertions.assertEquals(
                    assertionValue1Argument1, assertionValue1Argument2, "Without wal should match the expected value.");
        }
    }

    private boolean snapshotInProgress() throws IOException {
        try (Stream<Path> entries = Files.list(this.dir)) {
            return entries.anyMatch(e -> {
                final String name;
                name = e.getFileName().toString();
                try {
                    return name.startsWith("smq-snapshot-") && Files.size(e) > 0L;
                } catch (IOException gone) {
                    return false;
                }
            });
        }
    }

    private static Set<String> withoutWal(final Set<String> names) {
        return names.stream().filter(n -> !n.endsWith("-wal")).collect(Collectors.toSet());
    }

    @Test
    @DisplayName("닫힌 뒤 스냅샷은 예외가 나고 대상 파일도 임시 파일도 남지 않는다 — check 1")
    void afterClosingTheSnapshotThrowsAnExceptionAndNeitherTheTargetFileNorTheTemporaryFileRemainsCheck1()
            throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final Throwable assertionValue1;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.close();
            evaluate(this.names(this.dir));
            assertionValue1 = captureThrowable(() -> smq.snapshot(target));
            Assertions.assertInstanceOf(
                    IllegalStateException.class, assertionValue1, "Snapshot should throw IllegalStateException.");
            evaluate(Files.exists(target));
            evaluate(this.names(this.dir));
        }
    }

    @Test
    @DisplayName("닫힌 뒤 스냅샷은 예외가 나고 대상 파일도 임시 파일도 남지 않는다 — check 2")
    void afterClosingTheSnapshotThrowsAnExceptionAndNeitherTheTargetFileNorTheTemporaryFileRemainsCheck2()
            throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final boolean assertionValue2Argument1;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.close();
            evaluate(this.names(this.dir));
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            assertionValue2Argument1 = Files.exists(target);
            Assertions.assertFalse(assertionValue2Argument1, "Target should not exist.");
            evaluate(this.names(this.dir));
        }
    }

    @Test
    @DisplayName("닫힌 뒤 스냅샷은 예외가 나고 대상 파일도 임시 파일도 남지 않는다 — check 3")
    void afterClosingTheSnapshotThrowsAnExceptionAndNeitherTheTargetFileNorTheTemporaryFileRemainsCheck3()
            throws Exception {
        final Path target;
        target = this.dir.resolve("snap.db");
        try (Smq smq = this.open()) {
            final Set<String> before;
            final Set<String> assertionValue3Argument2;
            smq.declare("orders", QueueOptions.defaults()).publish(msg("a"));
            smq.close();
            before = this.names(this.dir);
            evaluate(captureThrowable(() -> smq.snapshot(target)));
            evaluate(Files.exists(target));
            assertionValue3Argument2 = this.names(this.dir);
            Assertions.assertEquals(before, assertionValue3Argument2, "Names should match the expected value.");
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
