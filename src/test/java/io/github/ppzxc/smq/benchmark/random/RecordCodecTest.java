package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 기록의 이진 형식. 디스크에 쌓았다 읽어도 같은 기록이어야 하고, 쓰다 죽은 파일의 꼬리는 없는 기록으로 본다. */
class RecordCodecTest {

    /** 2^53을 넘어 double로는 표현이 갈리는 값. JSON(double) 왕복이면 아래 비트를 잃는다. */
    private static final long BIG = Long.MAX_VALUE - 1;

    private static Published published() {
        return new Published(new Tag(-7, 41), new Content(BIG, Map.of("k", "한글")), 1_700_000_000_123L, 250);
    }

    private static Delivery delivery() {
        return new Delivery(
                QueueKind.DEAD_LETTER,
                new Tag(-7, 41),
                BIG - 1,
                3,
                new Content(-BIG, Map.of("smq-reason", "rejected")),
                new Pull(1L << 41, 5, 1_700_000_000_456L, 1_700_000_030_456L),
                Settle.nack(1_700_000_000_500L, 75));
    }

    @Test
    void 발행_기록은_64비트_값과_헤더까지_그대로_돌아온다() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        RecordCodec.write(new DataOutputStream(bytes), published());
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        assertEquals(published(), RecordCodec.readPublished(in));
        assertNull(RecordCodec.readPublished(in), "다 읽으면 null");
    }

    @Test
    void 전달_기록은_모든_필드가_그대로_돌아온다() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        RecordCodec.write(out, delivery());
        RecordCodec.write(out, delivery());
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        assertEquals(delivery(), RecordCodec.readDelivery(in));
        assertEquals(delivery(), RecordCodec.readDelivery(in));
        assertNull(RecordCodec.readDelivery(in));
    }

    @Test
    void 정산하지_않은_전달도_그대로_돌아온다() throws Exception {
        Delivery d = new Delivery(
                QueueKind.TARGET, new Tag(1, 1), 9, 1, new Content(0, Map.of()), new Pull(1, 0, 5, 6), Settle.NONE);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        RecordCodec.write(new DataOutputStream(bytes), d);
        assertEquals(d, RecordCodec.readDelivery(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))));
    }

    @Test
    void 끝이_잘린_기록은_없는_것으로_보고_앞의_기록은_읽는다() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        RecordCodec.write(out, delivery());
        int whole = bytes.size();
        RecordCodec.write(out, delivery());
        byte[] cut = java.util.Arrays.copyOf(bytes.toByteArray(), whole + 20);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(cut));
        assertEquals(delivery(), RecordCodec.readDelivery(in));
        assertNull(RecordCodec.readDelivery(in), "잘린 두 번째 기록은 없는 것으로 본다");
    }
}
