package io.github.ppzxc.smq.benchmark.random;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * 이 프로세스의 발행·전달 기록을 힙이 아니라 실행 디렉터리의 파일에 쌓는다. 기록을 힙에 두면 실행 길이에 비례해 힙이 차서
 * 수 시간짜리 실행이 {@code OutOfMemoryError}로 끝났다. 프로세스마다 {@code p<n>-published.bin}과 {@code p<n>-deliveries.bin}을
 * 쓰고, 검사는 끝난 뒤 {@link SpoolCheck}가 모든 프로세스의 파일을 읽어 한다.
 */
final class RecordSpool implements AutoCloseable {

    private static final int BUFFER_BYTES = 1 << 20;

    private final Object publishedLock = new Object();
    private final Object deliveriesLock = new Object();
    private final DataOutputStream published;
    private final DataOutputStream deliveries;

    RecordSpool(Path dir, int process) throws IOException {
        this.published = open(publishedFile(dir, process));
        DataOutputStream second;
        try {
            second = open(deliveriesFile(dir, process));
        } catch (IOException | RuntimeException e) {
            published.close();
            throw e;
        }
        this.deliveries = second;
    }

    static Path publishedFile(Path dir, int process) {
        return dir.resolve("p" + process + "-published.bin");
    }

    static Path deliveriesFile(Path dir, int process) {
        return dir.resolve("p" + process + "-deliveries.bin");
    }

    /** {@code dir}의 모든 프로세스의 발행 기록 파일. */
    static List<Path> publishedFiles(Path dir) throws IOException {
        return files(dir, "published");
    }

    /** {@code dir}의 모든 프로세스의 전달 기록 파일. */
    static List<Path> deliveriesFiles(Path dir) throws IOException {
        return files(dir, "deliveries");
    }

    private static List<Path> files(Path dir, String kind) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(f -> f.getFileName().toString().matches("p\\d+-" + kind + "\\.bin"))
                    .sorted()
                    .toList();
        }
    }

    private static DataOutputStream open(Path file) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file), BUFFER_BYTES));
    }

    void add(Published p) {
        try {
            synchronized (publishedLock) {
                RecordCodec.write(published, p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void add(Delivery d) {
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
