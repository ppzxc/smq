package io.github.ppzxc.smq.benchmark.random;

import io.github.ppzxc.smq.OutgoingMessage;
import io.github.ppzxc.smq.benchmark.random.Delivery.Action;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.zip.CRC32C;

/**
 * 시드에서 끌어낸 결정 모음. 메시지의 크기와 내용과 지연은 태그만으로, 정산과 처리 시간은 (태그, 전달 횟수)만으로 정해진다.
 * 그래서 어느 스레드가 언제 다뤄도 같은 시드는 같은 워크로드다. 끼어들기 순서만 실행마다 다르다.
 */
final class Script {

    static final String TAG_HEADER = "bench-tag";

    /** 발행할 메시지와 그 기록 내용. */
    record Planned(OutgoingMessage message, Content content, long delayMillis) {
    }

    /** 전달 하나의 정산 결정. */
    record Decision(Action action, long nackDelayMillis) {
    }

    /** 청크 하나의 처리 방식. */
    record Processing(long millis, boolean extend) {
    }

    private final RunParams params;

    Script(RunParams params) {
        this.params = params;
    }

    Planned message(Tag tag) {
        SplittableRandom r = rng(tag, 0, 1);
        int size = RunParams.logUniform(r, 0, params.maxMessageBytes());
        byte[] payload = new byte[size];
        for (int i = 0; i < size; i += 8) {
            long v = r.nextLong();
            for (int j = i; j < Math.min(size, i + 8); j++) {
                payload[j] = (byte) v;
                v >>>= 8;
            }
        }
        long delay = 0;
        if (params.publishDelayMaxMillis() > 0 && r.nextDouble() < params.publishDelayRatio()) {
            delay = 1 + r.nextLong(params.publishDelayMaxMillis());
        }
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(TAG_HEADER, tag.toString());
        int extra = r.nextInt(4);
        for (int i = 0; i < extra; i++) {
            headers.put("h" + i, Long.toHexString(r.nextLong()));
        }
        OutgoingMessage message = new OutgoingMessage(payload, headers,
                delay > 0 ? Duration.ofMillis(delay) : Duration.ZERO);
        return new Planned(message, new Content(checksum(payload), headers), delay);
    }

    Decision settle(Tag tag, int deliveryCount) {
        SplittableRandom r = rng(tag, deliveryCount, 2);
        double u = r.nextDouble();
        if (u < params.ackRatio()) {
            return new Decision(Action.ACK, 0);
        }
        u -= params.ackRatio();
        if (u < params.nackRatio()) {
            long delay = 0;
            if (params.nackDelayMaxMillis() > 0 && r.nextDouble() < params.nackDelayRatio()) {
                delay = 1 + r.nextLong(params.nackDelayMaxMillis());
            }
            return new Decision(Action.NACK, delay);
        }
        u -= params.nackRatio();
        return new Decision(u < params.rejectRatio() ? Action.REJECT : Action.NONE, 0);
    }

    /** 청크 처리 시간. 청크의 첫 메시지(태그와 전달 횟수)로 정한다. */
    Processing processing(Tag firstTag, int firstDeliveryCount) {
        SplittableRandom r = rng(firstTag, firstDeliveryCount, 3);
        if (r.nextDouble() >= params.slowChunkRatio()) {
            return new Processing(0, false);
        }
        long millis = r.nextLong((long) (params.leaseMillis() * 1.5) + 1);
        return new Processing(millis, r.nextDouble() < params.extendRatio());
    }

    static long checksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload);
        return crc.getValue();
    }

    private SplittableRandom rng(Tag tag, long a, long purpose) {
        long h = mix(params.seed() ^ 0x9E3779B97F4A7C15L);
        h = mix(h ^ tag.process());
        h = mix(h ^ tag.seq());
        h = mix(h ^ a);
        h = mix(h ^ purpose);
        return new SplittableRandom(h);
    }

    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
