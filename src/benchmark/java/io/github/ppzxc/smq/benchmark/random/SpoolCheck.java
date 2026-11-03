package io.github.ppzxc.smq.benchmark.random;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * {@link RecordSpool}이 쌓은 기록을 힙에 한꺼번에 올리지 않고 검사한다. 한 번에 올리면 실행이 길수록 힙이 차서 수 시간짜리 실행이
 * 끝난다. 그래서 기록을 해시로 나눠 조각 파일에 다시 쓰고, 조각 하나씩 읽어 검사한 뒤 버린다. 힙은 가장 큰 조각의 크기에 머문다.
 *
 * <ul>
 *   <li>태그 단위 검사(유령, 중복, 지연, 유실, dead letter)는 한 태그의 발행과 전달이 한 조각에 모이게 태그로 나눈다.
 *   <li>청크 순서 검사는 한 청크의 전달이 한 조각에 모이게 청크 번호로 따로 나눈다.
 *   <li>단일 활성 컨슈머 검사는 시간 순서로 모든 청크를 겹쳐 봐야 해서 이 방식으로는 하지 못한다. 고정 속도 모드가 단일 활성을 끄므로
 *       이 검사기는 그 모드에서만 쓴다.
 * </ul>
 */
final class SpoolCheck {

    /** 조각 하나의 목표 크기(스풀 파일 바이트). 객체로 올리면 몇 배가 된다. */
    static final long PARTITION_BYTES = 48L << 20;

    /**
     * 조각 수의 상한. 파일은 종류별로 차례로 열어 한 번에 최대 두 배(발행·전달)를 쓰므로 기본 FD 한도 1,024 안에 든다. 스풀이
     * {@code 400 * PARTITION_BYTES}(약 19GB)를 넘으면 조각이 목표보다 커지고 힙이 그만큼 는다.
     */
    private static final int MAX_PARTITIONS = 400;

    private static final int WRITER_BUFFER_BYTES = 32 << 10;
    /** 위반이 이만큼 쌓이면 더 모으지 않는다. 같은 원인으로 수백만 건이 나는 경우에 힙을 지킨다. */
    static final int MAX_VIOLATIONS = 1_000;

    private SpoolCheck() {}

    /**
     * @param publishedInLoad 부하 구간에 발행한 메시지 수
     * @param p50Millis 부하 구간에 발행해 지연 없이 처음 꺼낸 대상 큐 전달의 발행에서 꺼내기까지 걸린 시간의 중앙값(ms). 표본이 없으면 null
     * @param p99Millis 같은 시간의 99백분위수(ms). 표본이 없으면 null
     */
    record Result(List<Violation> violations, long publishedInLoad, Double p50Millis, Double p99Millis) {}

    /**
     * 이 전달이 지연 표본이면 발행에서 꺼내기까지 걸린 시간(ms), 아니면 -1. 표본은 부하 구간에 발행해 지연 없이 처음 꺼낸 대상 큐
     * 전달이다. 힙에서 검사하는 쪽과 조각으로 검사하는 쪽이 같은 기준을 쓰게 여기 한 곳에 둔다.
     */
    static long latencyMillis(Delivery d, Published p, long loadStartMillis, long loadEndMillis) {
        if (d.queue() != QueueKind.TARGET
                || d.deliveryCount() != 1
                || d.pull().atMillis() < loadStartMillis
                || d.pull().atMillis() >= loadEndMillis
                || p.delayMillis() != 0
                || p.atMillis() < loadStartMillis) {
            return -1;
        }
        return Math.max(0, d.pull().atMillis() - p.atMillis());
    }

    static Result run(
            Path dir,
            InvariantChecker.Config config,
            List<CountsSample> samples,
            long loadStartMillis,
            long loadEndMillis)
            throws IOException {
        if (config.singleActiveConsumer()) {
            throw new IllegalArgumentException("단일 활성 컨슈머 검사는 조각으로 나눠 할 수 없다");
        }
        return run(dir, config, samples, loadStartMillis, loadEndMillis, partitionCount(dir));
    }

    static Result run(
            Path dir,
            InvariantChecker.Config config,
            List<CountsSample> samples,
            long loadStartMillis,
            long loadEndMillis,
            int partitions)
            throws IOException {
        Path work = dir.resolve("check");
        Files.createDirectories(work);
        try {
            split(dir, work, partitions);
            return verify(work, config, samples, loadStartMillis, loadEndMillis, partitions);
        } finally {
            try {
                deleteTree(work);
            } catch (IOException | java.io.UncheckedIOException ignored) {
                // 임시 조각 파일을 못 지워도 검사 결과나 원래 예외를 가리지 않는다.
            }
        }
    }

    private static int partitionCount(Path dir) throws IOException {
        long bytes = 0;
        for (Path f : RecordSpool.publishedFiles(dir)) {
            bytes += Files.size(f);
        }
        for (Path f : RecordSpool.deliveriesFiles(dir)) {
            bytes += Files.size(f);
        }
        return (int) Math.max(1, Math.min(MAX_PARTITIONS, (bytes + PARTITION_BYTES - 1) / PARTITION_BYTES));
    }

    // ---- 나누기

    /**
     * 조각 파일은 종류별로 따로, 차례로 만든다(발행·전달 태그 조각 → 청크 조각 → id 조각). 한꺼번에 열면 조각 수의 네 배가 FD를
     * 쓰는데, 이렇게 하면 한 번에 최대 조각 수의 두 배다. 스풀을 세 번 읽는 값을 치르지만 읽기는 쓰기보다 싸다.
     */
    private static void split(Path dir, Path work, int partitions) throws IOException {
        try (Writers pub = new Writers(work, "tag-%d-pub.bin", partitions);
                Writers del = new Writers(work, "tag-%d-del.bin", partitions)) {
            for (Path f : RecordSpool.publishedFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Published p = RecordCodec.readPublished(in); p != null; p = RecordCodec.readPublished(in)) {
                        RecordCodec.write(pub.outs[bucket(tagHash(p.tag()), partitions)], p);
                    }
                }
            }
            for (Path f : RecordSpool.deliveriesFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Delivery d = RecordCodec.readDelivery(in); d != null; d = RecordCodec.readDelivery(in)) {
                        RecordCodec.write(del.outs[bucket(tagHash(d.tag()), partitions)], d);
                    }
                }
            }
        }
        try (Writers chunk = new Writers(work, "chunk-%d.bin", partitions)) {
            for (Path f : RecordSpool.deliveriesFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Delivery d = RecordCodec.readDelivery(in); d != null; d = RecordCodec.readDelivery(in)) {
                        RecordCodec.write(chunk.outs[bucket(mix(d.pull().chunk()), partitions)], d);
                    }
                }
            }
        }
        try (Writers ids = new Writers(work, "id-%d.bin", partitions)) {
            for (Path f : RecordSpool.deliveriesFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Delivery d = RecordCodec.readDelivery(in); d != null; d = RecordCodec.readDelivery(in)) {
                        writeId(ids.outs[bucket(idHash(d.queue(), d.messageId()), partitions)], d);
                    }
                }
            }
        }
    }

    /** 조각 수만큼의 출력 파일. 닫기 실패는 본문의 예외를 가리지 않고(try-with-resources가 suppressed로 붙인다) 첫 실패만 올린다. */
    private static final class Writers implements AutoCloseable {
        final DataOutputStream[] outs;

        Writers(Path work, String pattern, int partitions) throws IOException {
            outs = new DataOutputStream[partitions];
            try {
                for (int k = 0; k < partitions; k++) {
                    outs[k] = writer(work.resolve(String.format(pattern, k)));
                }
            } catch (IOException | RuntimeException e) {
                try {
                    close();
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }

        @Override
        public void close() throws IOException {
            closeAll(outs);
        }
    }

    /**
     * 메시지 id가 어느 태그의 것인지만 적은 짧은 기록. 한 id가 서로 다른 태그로 전달되는 유령은 id가 같은 전달이 한 조각에 모여야 보이는데,
     * 태그로 나눈 조각에서는 그 전달들이 흩어진다.
     */
    private static void writeId(DataOutputStream out, Delivery d) throws IOException {
        out.writeByte(d.queue().ordinal());
        out.writeLong(d.messageId());
        out.writeLong(d.tag().seed());
        out.writeLong(d.tag().seq());
    }

    private static long idHash(QueueKind queue, long messageId) {
        return mix(messageId * 3 + queue.ordinal());
    }

    private static void closeAll(DataOutputStream[] outs) throws IOException {
        IOException first = null;
        for (DataOutputStream o : outs) {
            if (o == null) {
                continue;
            }
            try {
                o.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private static DataOutputStream writer(Path f) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(f), WRITER_BUFFER_BYTES));
    }

    private static DataInputStream reader(Path f) throws IOException {
        return new DataInputStream(new BufferedInputStream(Files.newInputStream(f), 1 << 16));
    }

    private static long tagHash(Tag t) {
        return mix(t.seed() * 31) ^ mix(t.seq());
    }

    private static long mix(long x) {
        long h = x * 0x9E3779B97F4A7C15L;
        return h ^ (h >>> 32);
    }

    private static int bucket(long hash, int partitions) {
        return (int) Long.remainderUnsigned(hash, partitions);
    }

    // ---- 조각마다 검사

    private static Result verify(
            Path work,
            InvariantChecker.Config config,
            List<CountsSample> samples,
            long loadStartMillis,
            long loadEndMillis,
            int partitions)
            throws IOException {
        Collected violations = new Collected();
        long publishedInLoad = 0;
        LatencyHistogram latencies = new LatencyHistogram();
        for (int k = 0; k < partitions; k++) {
            publishedInLoad += verifyTags(work, k, config, loadStartMillis, loadEndMillis, violations, latencies);
            // 태그 조각은 위 메서드가 끝나며 힙에서 내려간다. 청크 조각과 id 조각은 그 뒤에 올린다.
            Path chunkFile = work.resolve("chunk-" + k + ".bin");
            violations.add(InvariantChecker.checkChunks(readAllDeliveries(chunkFile)));
            Files.delete(chunkFile);
            Path idFile = work.resolve("id-" + k + ".bin");
            violations.add(sameIdDifferentTag(idFile));
            Files.delete(idFile);
        }
        violations.add(InvariantChecker.checkSamples(samples));
        Double p50 = latencies.total() == 0 ? null : (double) latencies.percentile(50);
        Double p99 = latencies.total() == 0 ? null : (double) latencies.percentile(99);
        return new Result(List.copyOf(violations.list), publishedInLoad, p50, p99);
    }

    /** 태그 조각 하나를 검사하고 지우며, 부하 구간에 발행한 수를 돌려준다. 지연 표본은 {@code latencies}에 더한다. */
    private static long verifyTags(
            Path work,
            int k,
            InvariantChecker.Config config,
            long loadStartMillis,
            long loadEndMillis,
            Collected violations,
            LatencyHistogram latencies)
            throws IOException {
        Path pubFile = work.resolve("tag-" + k + "-pub.bin");
        Path delFile = work.resolve("tag-" + k + "-del.bin");
        List<Published> pub = readAllPublished(pubFile);
        List<Delivery> del = readAllDeliveries(delFile);
        violations.add(InvariantChecker.checkTags(config, pub, del));
        Map<Tag, Published> byTag = new HashMap<>();
        long inLoad = 0;
        for (Published p : pub) {
            byTag.put(p.tag(), p);
            if (p.atMillis() >= loadStartMillis && p.atMillis() < loadEndMillis) {
                inLoad++;
            }
        }
        for (Delivery d : del) {
            Published p = byTag.get(d.tag());
            long latency = p == null ? -1 : latencyMillis(d, p, loadStartMillis, loadEndMillis);
            if (latency >= 0) {
                latencies.add(latency);
            }
        }
        Files.delete(pubFile);
        Files.delete(delFile);
        return inLoad;
    }

    /** 같은 큐의 같은 메시지 id가 서로 다른 태그로 전달된 것. {@link InvariantChecker}의 같은 이름의 유령 검사와 같은 뜻이다. */
    private static List<Violation> sameIdDifferentTag(Path file) throws IOException {
        List<Violation> out = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Map<Long, Tag>[] seen = new Map[QueueKind.values().length];
        for (int q = 0; q < seen.length; q++) {
            seen[q] = new HashMap<>();
        }
        try (DataInputStream in = reader(file)) {
            while (true) {
                QueueKind queue;
                long id;
                Tag tag;
                try {
                    queue = QueueKind.values()[in.readUnsignedByte()];
                    id = in.readLong();
                    tag = new Tag(in.readLong(), in.readLong());
                } catch (java.io.EOFException e) {
                    return out;
                }
                Tag first = seen[queue.ordinal()].putIfAbsent(id, tag);
                if (first != null && !first.equals(tag)) {
                    out.add(new Violation(
                            Invariant.GHOST, List.of(first, tag), List.of(id), queue + " 메시지 id 하나가 서로 다른 태그로 전달됐다"));
                }
            }
        }
    }

    /** 위반 모음. 상한까지만 모으고, 같은 위반(불변식, 설명, 메시지 id, 태그 집합)은 한 번만 넣는다(태그 검사와 id 검사가 같은 유령을 따로 볼 수 있다). */
    private static final class Collected {
        private final List<Violation> list = new ArrayList<>();
        private final java.util.Set<String> keys = new java.util.HashSet<>();

        void add(List<Violation> found) {
            for (Violation v : found) {
                if (list.size() >= MAX_VIOLATIONS) {
                    return;
                }
                List<String> tags =
                        v.tags().stream().map(Tag::toString).sorted().toList();
                if (keys.add(v.invariant() + "|" + v.detail() + "|" + v.messageIds() + "|" + tags)) {
                    list.add(v);
                }
            }
        }
    }

    private static List<Published> readAllPublished(Path f) throws IOException {
        List<Published> out = new ArrayList<>();
        try (DataInputStream in = reader(f)) {
            for (Published p = RecordCodec.readPublished(in); p != null; p = RecordCodec.readPublished(in)) {
                out.add(p);
            }
        }
        return out;
    }

    private static List<Delivery> readAllDeliveries(Path f) throws IOException {
        List<Delivery> out = new ArrayList<>();
        try (DataInputStream in = reader(f)) {
            for (Delivery d = RecordCodec.readDelivery(in); d != null; d = RecordCodec.readDelivery(in)) {
                out.add(d);
            }
        }
        return out;
    }

    private static void deleteTree(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) s.sorted(java.util.Comparator.reverseOrder())::iterator) {
                Files.delete(p);
            }
        }
    }
}
