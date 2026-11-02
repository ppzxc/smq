package io.github.ppzxc.smq.benchmark.random;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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

    private static final int MAX_PARTITIONS = 512;
    private static final int WRITER_BUFFER_BYTES = 32 << 10;
    /** 위반이 이만큼 쌓이면 더 모으지 않는다. 같은 원인으로 수백만 건이 나는 경우에 힙을 지킨다. */
    static final int MAX_VIOLATIONS = 1_000;

    private SpoolCheck() {}

    /**
     * @param publishedInLoad 부하 구간에 발행한 메시지 수
     * @param latenciesMillis 부하 구간에 발행해 지연 없이 처음 꺼낸 대상 큐 전달의 발행에서 꺼내기까지 걸린 시간(ms). 정렬돼 있다
     */
    record Result(List<Violation> violations, long publishedInLoad, long[] latenciesMillis) {}

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
            deleteTree(work);
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

    private static void split(Path dir, Path work, int partitions) throws IOException {
        DataOutputStream[] pub = new DataOutputStream[partitions];
        DataOutputStream[] del = new DataOutputStream[partitions];
        DataOutputStream[] chunk = new DataOutputStream[partitions];
        try {
            for (int k = 0; k < partitions; k++) {
                pub[k] = writer(work.resolve("tag-" + k + "-pub.bin"));
                del[k] = writer(work.resolve("tag-" + k + "-del.bin"));
                chunk[k] = writer(work.resolve("chunk-" + k + ".bin"));
            }
            for (Path f : RecordSpool.publishedFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Published p = RecordCodec.readPublished(in); p != null; p = RecordCodec.readPublished(in)) {
                        RecordCodec.write(pub[bucket(tagHash(p.tag()), partitions)], p);
                    }
                }
            }
            for (Path f : RecordSpool.deliveriesFiles(dir)) {
                try (DataInputStream in = reader(f)) {
                    for (Delivery d = RecordCodec.readDelivery(in); d != null; d = RecordCodec.readDelivery(in)) {
                        RecordCodec.write(del[bucket(tagHash(d.tag()), partitions)], d);
                        RecordCodec.write(chunk[bucket(mix(d.pull().chunk()), partitions)], d);
                    }
                }
            }
        } finally {
            closeAll(pub);
            closeAll(del);
            closeAll(chunk);
        }
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
        return mix(t.seed() * 31 + t.process()) ^ mix(t.seq());
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
        List<Violation> violations = new ArrayList<>();
        long publishedInLoad = 0;
        long[] latencies = new long[1024];
        int latencyCount = 0;
        for (int k = 0; k < partitions; k++) {
            List<Published> pub = readAllPublished(work.resolve("tag-" + k + "-pub.bin"));
            List<Delivery> del = readAllDeliveries(work.resolve("tag-" + k + "-del.bin"));
            addCapped(violations, InvariantChecker.checkTags(config, pub, del));
            Map<Tag, Published> byTag = new HashMap<>();
            for (Published p : pub) {
                byTag.put(p.tag(), p);
                if (p.atMillis() >= loadStartMillis && p.atMillis() < loadEndMillis) {
                    publishedInLoad++;
                }
            }
            for (Delivery d : del) {
                if (d.queue() != QueueKind.TARGET
                        || d.deliveryCount() != 1
                        || d.pull().atMillis() < loadStartMillis
                        || d.pull().atMillis() >= loadEndMillis) {
                    continue;
                }
                Published p = byTag.get(d.tag());
                if (p != null && p.delayMillis() == 0 && p.atMillis() >= loadStartMillis) {
                    if (latencyCount == latencies.length) {
                        latencies = Arrays.copyOf(latencies, latencies.length * 2);
                    }
                    latencies[latencyCount++] = Math.max(0, d.pull().atMillis() - p.atMillis());
                }
            }
            Files.delete(work.resolve("tag-" + k + "-pub.bin"));
            Files.delete(work.resolve("tag-" + k + "-del.bin"));
            Path chunkFile = work.resolve("chunk-" + k + ".bin");
            addCapped(violations, InvariantChecker.checkChunks(readAllDeliveries(chunkFile)));
            Files.delete(chunkFile);
        }
        addCapped(violations, InvariantChecker.checkSamples(samples));
        long[] sorted = Arrays.copyOf(latencies, latencyCount);
        Arrays.sort(sorted);
        return new Result(List.copyOf(violations), publishedInLoad, sorted);
    }

    private static void addCapped(List<Violation> into, List<Violation> found) {
        for (Violation v : found) {
            if (into.size() >= MAX_VIOLATIONS) {
                return;
            }
            into.add(v);
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
