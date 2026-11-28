package io.github.ppzxc.smq.benchmark.random;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.smq.benchmark.random.Delivery.Pull;
import io.github.ppzxc.smq.benchmark.random.Delivery.Settle;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecordCodecTest {

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

    private record PublishedReads(Published first, Published next) {}

    private static PublishedReads readPublishedRecords() throws Exception {
        final ByteArrayOutputStream bytes;
        final DataInputStream in;
        bytes = new ByteArrayOutputStream();
        RecordCodec.write(new DataOutputStream(bytes), published());
        in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        return new PublishedReads(RecordCodec.readPublished(in), RecordCodec.readPublished(in));
    }

    @Test
    @DisplayName("발행 기록은 64비트 값과 헤더까지 그대로 돌아온다")
    void publishedRecordRoundTrips() throws Exception {
        assertEquals(published(), readPublishedRecords().first(), "The published record should round-trip.");
    }

    @Test
    @DisplayName("발행 기록을 모두 읽으면 null이다")
    void publishedRecordsEndAfterLastRecord() throws Exception {
        assertNull(readPublishedRecords().next(), "Reading past the last published record should return null.");
    }

    private record DeliveryReads(Delivery first, Delivery second, Delivery next) {}

    private static DeliveryReads readDeliveryRecords() throws Exception {
        final ByteArrayOutputStream bytes;
        final DataOutputStream out;
        final DataInputStream in;
        bytes = new ByteArrayOutputStream();
        out = new DataOutputStream(bytes);
        RecordCodec.write(out, delivery());
        RecordCodec.write(out, delivery());
        in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        return new DeliveryReads(
                RecordCodec.readDelivery(in), RecordCodec.readDelivery(in), RecordCodec.readDelivery(in));
    }

    @Test
    @DisplayName("첫 전달 기록은 모든 필드가 그대로 돌아온다")
    void firstDeliveryRecordRoundTrips() throws Exception {
        assertEquals(delivery(), readDeliveryRecords().first(), "The first delivery record should round-trip.");
    }

    @Test
    @DisplayName("두 번째 전달 기록도 모든 필드가 그대로 돌아온다")
    void secondDeliveryRecordRoundTrips() throws Exception {
        assertEquals(delivery(), readDeliveryRecords().second(), "The second delivery record should round-trip.");
    }

    @Test
    @DisplayName("전달 기록을 모두 읽으면 null이다")
    void deliveryRecordsEndAfterLastRecord() throws Exception {
        assertNull(readDeliveryRecords().next(), "Reading past the last delivery record should return null.");
    }

    @Test
    @DisplayName("정산하지 않은 전달도 그대로 돌아온다")
    void unsettledTransmissionsAlsoReturnAsTheyAre() throws Exception {
        final Delivery d;
        final ByteArrayOutputStream bytes;
        d = new Delivery(
                QueueKind.TARGET, new Tag(1, 1), 9, 1, new Content(0, Map.of()), new Pull(1, 0, 5, 6), Settle.NONE);
        bytes = new ByteArrayOutputStream();
        RecordCodec.write(new DataOutputStream(bytes), d);
        assertEquals(
                d,
                RecordCodec.readDelivery(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))),
                "Read delivery should match the expected value.");
    }

    private static DeliveryReads readTruncatedDeliveryRecords() throws Exception {
        final ByteArrayOutputStream bytes;
        final DataOutputStream out;
        final int whole;
        final byte[] cut;
        final DataInputStream in;
        bytes = new ByteArrayOutputStream();
        out = new DataOutputStream(bytes);
        RecordCodec.write(out, delivery());
        whole = bytes.size();
        RecordCodec.write(out, delivery());
        cut = java.util.Arrays.copyOf(bytes.toByteArray(), whole + 20);
        in = new DataInputStream(new ByteArrayInputStream(cut));
        return new DeliveryReads(RecordCodec.readDelivery(in), RecordCodec.readDelivery(in), null);
    }

    @Test
    @DisplayName("끝이 잘린 기록 앞의 전달 기록은 읽는다")
    void completeDeliveryBeforeTruncatedRecordIsRead() throws Exception {
        assertEquals(delivery(), readTruncatedDeliveryRecords().first(), "The complete delivery should be read.");
    }

    @Test
    @DisplayName("끝이 잘린 전달 기록은 없는 것으로 본다")
    void truncatedDeliveryRecordIsAbsent() throws Exception {
        assertNull(readTruncatedDeliveryRecords().second(), "The truncated delivery should be absent.");
    }
}
