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
    record Planned(OutgoingMessage message, Content content, long delayMillis) {}

    /** 전달 하나의 정산 결정. */
    record Decision(Action action, long nackDelayMillis) {}

    /** 청크 하나의 처리 방식. */
    record Processing(long millis, boolean extend) {}

    private final RunParams params;

    Script(final RunParams params) {
        this.params = params;
    }

    Planned message(final Tag tag) {
        final SplittableRandom r;
        final int size;
        final byte[] payload;
        long delay;
        final Map<String, String> headers;
        final int extra;
        final OutgoingMessage message;
        r = rng(tag, 0, 1);
        size = params.minMessageBytes() > 0
                ? params.minMessageBytes() + r.nextInt(params.maxMessageBytes() - params.minMessageBytes() + 1)
                : RunParams.logUniform(r, 0, params.maxMessageBytes());
        payload = new byte[size];
        for (int i = 0; i < size; i += 8) {
            long v;
            v = r.nextLong();
            for (int j = i; j < Math.min(size, i + 8); j++) {
                payload[j] = (byte) v;
                v >>>= 8;
            }
        }
        delay = 0;
        if (params.publishDelayMaxMillis() > 0 && r.nextDouble() < params.publishDelayRatio()) {
            delay = 1 + r.nextLong(params.publishDelayMaxMillis());
        }
        headers = new LinkedHashMap<>();
        headers.put(TAG_HEADER, tag.toString());
        extra = r.nextInt(4);
        for (int i = 0; i < extra; i++) {
            headers.put("h" + i, Long.toHexString(r.nextLong()));
        }
        message = new OutgoingMessage(payload, headers, delay > 0 ? Duration.ofMillis(delay) : Duration.ZERO);
        return new Planned(message, new Content(payloadChecksum(payload), headers), delay);
    }

    Decision settle(final Tag tag, final int deliveryCount) {
        final SplittableRandom r;
        double u;
        r = rng(tag, deliveryCount, 2);
        u = r.nextDouble();
        if (u < params.ackRatio()) {
            return new Decision(Action.ACK, 0);
        }
        u -= params.ackRatio();
        if (u < params.nackRatio()) {
            long delay;
            delay = 0;
            if (params.nackDelayMaxMillis() > 0 && r.nextDouble() < params.nackDelayRatio()) {
                delay = 1 + r.nextLong(params.nackDelayMaxMillis());
            }
            return new Decision(Action.NACK, delay);
        }
        u -= params.nackRatio();
        return new Decision(u < params.rejectRatio() ? Action.REJECT : Action.NONE, 0);
    }

    /** 청크 처리 시간. 청크의 첫 메시지(태그와 전달 횟수)로 정한다. */
    Processing processing(final Tag firstTag, final int firstDeliveryCount) {
        final SplittableRandom r;
        final long millis;
        r = rng(firstTag, firstDeliveryCount, 3);
        if (r.nextDouble() >= params.slowChunkRatio()) {
            return new Processing(0, false);
        }
        millis = r.nextLong((long) (params.leaseMillis() * 1.5) + 1);
        return new Processing(millis, r.nextDouble() < params.extendRatio());
    }

    static long payloadChecksum(final byte[] payload) {
        final CRC32C crc;
        crc = new CRC32C();
        crc.update(payload);
        return crc.getValue();
    }

    private SplittableRandom rng(final Tag tag, final long a, final long purpose) {
        long h;
        h = mix(params.seed() ^ 0x9E3779B97F4A7C15L);
        // 예전에는 프로세스 번호(항상 0)를 섞던 자리다. 같은 시드가 같은 매개변수와 같은 워크로드를 내야 하므로
        // 값이 0일 때와 같은 결과를 내는 mix(h)로 남긴다. 지우면 모든 시드의 생성 워크로드가 달라진다.
        h = mix(h);
        h = mix(h ^ tag.seq());
        h = mix(h ^ a);
        h = mix(h ^ purpose);
        return new SplittableRandom(h);
    }

    private static long mix(final long z) {
        final long first;
        final long second;
        first = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        second = (first ^ (first >>> 27)) * 0x94D049BB133111EBL;
        return second ^ (second >>> 31);
    }
}
