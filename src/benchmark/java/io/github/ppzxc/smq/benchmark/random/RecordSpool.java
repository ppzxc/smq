package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.PolicyException;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * 발행·전달 기록을 힙이 아니라 실행 디렉터리의 파일에 쌓는다. 기록을 힙에 두면 실행 길이에 비례해 힙이 차서
 * 수 시간짜리 실행이 {@code OutOfMemoryError}로 끝났다. {@code published.bin}과 {@code deliveries.bin}을
 * 쓰고, 검사는 끝난 뒤 {@link SpoolCheck}가 그 파일을 읽어 한다.
 */
final class RecordSpool implements AutoCloseable {

    private static final int BUFFER_BYTES = 1 << 20;

    private final Object publishedLock = new Object();
    private final Object deliveriesLock = new Object();
    private final DataOutputStream published;
    private final DataOutputStream deliveries;

    @PolicyException(
            rule = "PMD.CloseResource",
            reason =
                    "The streams transfer to this spool and close together in close; the first closes if opening the second fails.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @PolicyException(
            rule = "PMD.AvoidCatchingGenericException",
            reason = "A failure opening the second stream must close the first, including runtime failures.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings({"PMD.CloseResource", "PMD.AvoidCatchingGenericException"})
    RecordSpool(final Path dir) throws IOException {
        final DataOutputStream second;
        this.published = open(publishedFile(dir));

        try {
            second = open(deliveriesFile(dir));
        } catch (IOException | RuntimeException e) {
            published.close();
            throw e;
        }
        this.deliveries = second;
    }

    static Path publishedFile(final Path dir) {
        return dir.resolve("published.bin");
    }

    static Path deliveriesFile(final Path dir) {
        return dir.resolve("deliveries.bin");
    }

    /** {@code dir}의 발행 기록 파일. */
    static List<Path> publishedFiles(final Path dir) throws IOException {
        return files(dir, "published");
    }

    /** {@code dir}의 전달 기록 파일. */
    static List<Path> deliveriesFiles(final Path dir) throws IOException {
        return files(dir, "deliveries");
    }

    private static List<Path> files(final Path dir, final String kind) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(f -> f.getFileName().toString().equals(kind + ".bin"))
                    .sorted()
                    .toList();
        }
    }

    private static DataOutputStream open(final Path file) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file), BUFFER_BYTES));
    }

    void add(final Published p) {
        try {
            synchronized (publishedLock) {
                RecordCodec.write(published, p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void add(final Delivery d) {
        try {
            synchronized (deliveriesLock) {
                RecordCodec.write(deliveries, d);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 버퍼를 내보내고 닫는다. 검사가 읽기 전에 불러야 한다. */
    @Override
    @PolicyException(
            rule = "PMD.UseTryWithResources",
            reason = "Required output, fixture behavior, or resource ownership contract is preserved.",
            approvedBy = "ppzxc",
            reviewBy = "2027-11-26")
    @SuppressWarnings("PMD.UseTryWithResources")
    public void close() throws IOException {
        try {
            synchronized (publishedLock) {
                published.close();
            }
        } finally {
            synchronized (deliveriesLock) {
                deliveries.close();
            }
        }
    }
}
